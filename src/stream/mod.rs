//! # Neolink Stream
//!
//! Streams a single camera to stdout as a byte pipe. There is no RTSP server,
//! no session management and no shared media: one process, one camera, one
//! consumer, one file descriptor.
//!
//! ## Why a pipe
//!
//! `neolink rtsp` serves a `gst-rtsp-server` mount that any number of clients
//! may attach to and detach from at any time. That generality is what most of
//! the moving parts exist for — shared-media lifecycle, suspend modes, session
//! reaping, appsrc back-pressure, egress liveness probes — and it is
//! unnecessary when the deployment is "one local consumer per camera", which
//! is the common case for Frigate, Home Assistant and friends.
//!
//! In pipe mode the consumer owns the process. It spawns us, reads our stdout,
//! and kills us when it no longer wants the stream. That collapses the failure
//! model to two rules:
//!
//! * **Stdout closed** means the consumer is gone. Exit 0.
//! * **No frame for `--stale-timeout`**, or the BC subscription giving up,
//!   means the camera is gone. Exit non-zero and let the consumer respawn.
//!
//! The second rule replaces every in-process watchdog. A wedged pipeline
//! cannot exist because there is no pipeline; the worst a stall can do is stop
//! the byte flow, which the timeout already covers, and a fresh process starts
//! from a clean BC login rather than trying to repair a half-dead one.
//!
//! ## What is reused
//!
//! Everything below the transport: [`NeoReactor`] resolves the camera,
//! `run_task` runs the subscription and transparently re-runs it against a new
//! [`BcCamera`] whenever the connection is re-established, and
//! `start_video` yields [`BcMedia`] frames. This module only adds framing.
//!
//! ## Output
//!
//! `--format ts` (the default) wraps frames in MPEG-TS, which carries a 90 kHz
//! PTS and can carry the camera's AAC audio alongside video. `--format h26x`
//! writes the Annex-B video elementary stream verbatim: smaller and simpler,
//! but video only and with no timestamps, so the consumer has to invent them.
//!
//! Stdout is media. All logging goes to stderr.
//!
//! ## Usage
//!
//! ```bash
//! neolink stream --config=config.toml CameraName --stream=main --format=ts
//! ```
use anyhow::{anyhow, Context, Result};
use log::*;
use neolink_core::{
    bc_protocol::StreamKind,
    bcmedia::model::{BcMedia, VideoType},
};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::{
    io::{stdout, AsyncWrite, AsyncWriteExt},
    sync::mpsc::{channel, Receiver},
    time::{timeout, Duration, Instant},
};

// `neolink stream` needs no GStreamer for video — `mpegts.rs` is hand-rolled —
// so a `--no-default-features` build keeps the subcommand and loses only the
// ADPCM transcode. See `aac_nogst.rs`.
#[cfg_attr(not(feature = "gstreamer"), path = "aac_nogst.rs")]
mod aac;
#[cfg(feature = "gstreamer")]
mod adpcm;
mod cmdline;
mod mpegts;

use crate::common::{now_epoch_ms, FrameConsumer, NeoInstance, NeoReactor};
use crate::AnyResult;
use aac::{adts_duration_micros, AdpcmTranscoder};
use cmdline::{AudioMode, Format};
pub(crate) use cmdline::Opt;
use mpegts::{AudioKind, TsMuxer};

/// How long the codec-learning phase may buffer frames before giving up on
/// ever seeing an audio track and emitting a video-only PMT.
///
/// Measured from the FIRST MEDIA UNIT, not from process start. Anchoring it at
/// start was a latent bug: `NeoCam` init takes 4.4 s (login is 90 ms, the rest
/// is camera-time and model/firmware queries at 2 s spacing), so the window had
/// always expired before frame one and the PMT was decided on the first
/// I-frame with the audio track still unknown. Nothing noticed while ADPCM was
/// being dropped anyway; it is exactly what stopped the transcoder from ever
/// being asked for (measured 2026-09-10: "Track layout learned: video H264,
/// audio none" on a camera C that does send ADPCM).
const LEARN_TIMEOUT: Duration = Duration::from_secs(3);
/// Hard cap on media units buffered during the learning phase, so a camera
/// that sends video but never audio cannot grow the buffer without bound.
const LEARN_MAX_FRAMES: usize = 30;
/// Only treat a backwards jump in the camera's microsecond stamp as a counter
/// wrap when the previous value was close to the top of the `u32`. Anything
/// else is a camera-side reset (a reconnect), which must not add 2^32.
const WRAP_GUARD: u32 = 0xF000_0000;
/// Re-anchor the synthetic audio clock if it drifts this far from video.
///
/// This is the hard backstop: a reconnect, a camera clock rebase or a
/// transcoder burst can leave the two clocks seconds apart, and only a snap
/// recovers from that. Steady-state surplus is handled by the much tighter
/// guard below, because a snap this large is itself a two second A/V jump.
const AUDIO_RESYNC_90K: i64 = 2 * 90_000;
/// Nominal audio frame length, used when the ADTS header could not be parsed.
/// 1024 samples at 16 kHz, which is what the cameras on this path send.
const AUDIO_FRAME_MICROS: u32 = 64_000;
/// How far ahead of a newly arrived video stamp the audio clock may run, as a
/// fraction of one audio frame, before the next audio frame is dropped: one
/// and a half frames.
///
/// The comparison is made in `Clock::video`, against the stamp that has just
/// arrived, never in `Clock::audio` against a stale one. Judging on every
/// audio frame mistook a video delivery gap for surplus audio: while video
/// was absent every audio frame past the band was dropped, and once video
/// resumed the audio was left that far behind for the rest of the session
/// (measured in the unit test: a 200 ms gap cost two frames and 61 ms of
/// permanent lag). With no fresh video there is nothing to judge against, so
/// a gap or a stall leaves the audio clock alone and the two second backstop
/// covers it as before.
///
/// A one frame band flaps, because video arrives every 66 ms at 15 fps while
/// audio frames arrive every 64 ms, so the measured lead oscillates by up to
/// a frame with nothing wrong.
const AUDIO_GUARD_NUM: i64 = 3;
const AUDIO_GUARD_DEN: i64 = 2;

/// Entry point for the stream subcommand.
pub(crate) async fn main(opt: Opt, reactor: NeoReactor) -> Result<()> {
    let camera = reactor
        .get(&opt.camera)
        .await
        .with_context(|| format!("Could not find camera `{}` in the config", opt.camera))?;
    let stream: StreamKind = opt.stream.into();
    let stale_timeout = Duration::from_secs(opt.stale_timeout);
    let strict = camera.config().await?.borrow().strict;

    info!(
        "{}::{stream}: pipe mode ({:?}), stale timeout {}s",
        opt.camera, opt.format, opt.stale_timeout
    );

    // Decided once, before any frame is written, because it decides the PMT:
    // announcing an AAC track we then cannot produce leaves the consumer
    // waiting on a stream that never arrives, which is worse than no audio.
    let transcode = match (opt.format, opt.audio) {
        (Format::H26x, _) => false,
        (Format::Ts, AudioMode::None) => {
            info!("{}::{stream}: audio disabled (--audio none)", opt.camera);
            false
        }
        (Format::Ts, AudioMode::Aac) => match aac::probe() {
            Ok(()) => true,
            Err(e) => {
                error!(
                    "{}::{stream}: no AAC encoder, ADPCM audio will be dropped: {e:#}",
                    opt.camera
                );
                false
            }
        },
    };

    // The camera thread's own connection watchdog decides whether a BC session
    // is alive by reading this cell, on the understanding that whoever is
    // consuming frames writes to it. `neolink rtsp` writes it from the
    // GStreamer frame-pump; if a consumer does not write it at all, the
    // watchdog concludes the session is delivering nothing and tears down a
    // perfectly healthy connection on a timer. Measured before this was added:
    // "pings OK but no frames" every ~34 s, each costing a ~4.4 s reconnect
    // gap, i.e. 1607 of an expected 1801 frames in 120 s.
    let last_frame_at = camera.last_frame_at().await?;

    let mut sink = Sink::new();
    let mut media_rx = subscribe(&camera, stream, strict);
    let mut framer = Framer::new(opt.format, opt.audio, transcode, opt.audio_rate);

    pump(
        &opt.camera,
        stream,
        stale_timeout,
        &mut media_rx,
        &mut sink,
        &mut framer,
        &last_frame_at,
    )
    .await
}

/// The whole of pipe mode's failure model, in one loop.
///
/// * A frame arrives: frame it, write it, note the time.
/// * The write fails with a closed-pipe error: the consumer left. `Ok(())`.
/// * No frame for `stale_timeout`: the camera left. `Err`.
/// * The media channel closes: `run_task` gave up. `Err`.
///
/// Split out of [`main`] so it can be driven from a test with a channel and a
/// pipe instead of a camera and stdout.
#[allow(clippy::too_many_arguments)]
async fn pump<W: AsyncWrite + Unpin>(
    camera_name: &str,
    stream: StreamKind,
    stale_timeout: Duration,
    media_rx: &mut Receiver<BcMedia>,
    sink: &mut Sink<W>,
    framer: &mut Framer,
    last_frame_at: &AtomicU64,
) -> Result<()> {
    let mut last_frame = Instant::now();

    loop {
        let remaining = stale_timeout.saturating_sub(last_frame.elapsed());
        if remaining.is_zero() {
            return Err(stale_error(camera_name, stream, stale_timeout));
        }

        match timeout(remaining, media_rx.recv()).await {
            Err(_elapsed) => return Err(stale_error(camera_name, stream, stale_timeout)),
            Ok(None) => {
                // `run_task` absorbs every error it thinks a retry can fix, so
                // the channel closing means it gave up. Re-subscribing on the
                // same NeoInstance was measured to be useless: after a 40 s
                // link cut the resubscribe returned instantly and repeatedly
                // (one "subscription ended" per second for 30 s) because the
                // camera state that broke it lives in the reactor, not in the
                // subscription. Exiting hands the problem to the supervisor,
                // whose respawn rebuilds the reactor from scratch.
                return Err(anyhow!(
                    "{camera_name}::{stream}: camera subscription ended — exiting so the supervisor can respawn"
                ));
            }
            Ok(Some(media)) => {
                if framer.push(&media, sink.buffer()) {
                    last_frame = Instant::now();
                    last_frame_at.store(now_epoch_ms(), Ordering::Relaxed);
                }
                if sink.flush().await? {
                    info!("{camera_name}::{stream}: consumer closed the pipe");
                    return Ok(());
                }
            }
        }
    }
}

/// The error used for both stale-timeout paths, so the exit reason is
/// identical whichever one fires.
fn stale_error(camera: &str, stream: StreamKind, stale_timeout: Duration) -> anyhow::Error {
    anyhow!(
        "{camera}::{stream}: no frame for {}s — exiting so the supervisor can respawn",
        stale_timeout.as_secs()
    )
}

/// Subscribe to the camera's video stream.
///
/// The returned receiver stays valid across camera reconnects: `run_task`
/// re-runs the closure against the new [`BcCamera`] and the same sender is
/// used, so the consumer sees a gap in frames rather than a closed channel.
/// It closing therefore means `run_task` gave up, which the caller treats as
/// grounds to exit.
fn subscribe(camera: &NeoInstance, stream: StreamKind, strict: bool) -> Receiver<BcMedia> {
    let (media_tx, media_rx) = channel(100);
    let camera = camera.clone();
    tokio::task::spawn(async move {
        let frame_consumers = match camera.frame_consumers().await {
            Ok(counter) => counter,
            Err(e) => {
                warn!("BC video subscription could not register as a consumer: {e:?}");
                return;
            }
        };
        let result = camera
            .run_task(move |cam| {
                let media_tx = media_tx.clone();
                let frame_consumers = frame_consumers.clone();
                Box::pin(async move {
                    // fix 17: the pipe is a permanent consumer, so the
                    // camthread frame-staleness watchdog stays armed for it.
                    let _consumer = FrameConsumer::hold(frame_consumers);
                    let mut media_stream = cam.start_video(stream, 0, strict).await?;
                    debug!("BC video subscription started");
                    while let Ok(media) = media_stream.get_data().await? {
                        media_tx.send(media).await?;
                    }
                    AnyResult::Ok(())
                })
            })
            .await;
        // Logged loudly because this is the reason the process is about to
        // exit, and stderr is the only place a supervisor can see it.
        warn!("BC video subscription finished: {result:?}");
    });
    media_rx
}

/// Owns the output handle and the scratch buffer frames are framed into.
///
/// Generic over the writer only so that tests can hand it a pipe; in
/// production it is always [`Sink<Stdout>`].
struct Sink<W> {
    out: W,
    buffer: Vec<u8>,
}

impl Sink<tokio::io::Stdout> {
    fn new() -> Self {
        Self::with_writer(stdout())
    }
}

impl<W: AsyncWrite + Unpin> Sink<W> {
    fn with_writer(out: W) -> Self {
        Self {
            out,
            // Comfortably larger than a 4K key frame, so steady state never
            // reallocates.
            buffer: Vec::with_capacity(512 * 1024),
        }
    }

    fn buffer(&mut self) -> &mut Vec<u8> {
        &mut self.buffer
    }

    /// Write and clear the scratch buffer.
    ///
    /// Returns `Ok(true)` when the consumer has closed the pipe, which is a
    /// normal shutdown rather than an error.
    async fn flush(&mut self) -> Result<bool> {
        if self.buffer.is_empty() {
            return Ok(false);
        }
        let result = self
            .out
            .write_all(&self.buffer)
            .await
            .and(self.out.flush().await);
        self.buffer.clear();
        match result {
            Ok(()) => Ok(false),
            Err(e) if is_pipe_closed(&e) => Ok(true),
            Err(e) => Err(anyhow::Error::new(e).context("Failed to write to stdout")),
        }
    }
}

/// Whether an stdout error means "the reader went away" rather than a real
/// failure. Rust ignores `SIGPIPE`, so a closed pipe surfaces here as `EPIPE`.
fn is_pipe_closed(e: &std::io::Error) -> bool {
    use std::io::ErrorKind::*;
    matches!(e.kind(), BrokenPipe | ConnectionReset | ConnectionAborted)
}

/// Turns [`BcMedia`] frames into output bytes.
enum Framer {
    /// Annex-B passthrough, waiting for the first key frame.
    Raw { started: bool },
    /// MPEG-TS, buffering frames until the track layout is known.
    TsLearning {
        /// Set on the first media unit, not at construction. See
        /// [`LEARN_TIMEOUT`].
        deadline: Option<Instant>,
        video: Option<VideoType>,
        audio: Option<AudioKind>,
        /// Whether the audio track, if any, comes from transcoding ADPCM.
        transcoded: bool,
        /// Set once the audio question is answered: an AAC or ADPCM frame has
        /// arrived, or `--audio none` means the answer is fixed in advance.
        /// This is what lets a camera stop buffering early.
        audio_known: bool,
        /// Whether an ADPCM track may be transcoded at all: `--audio aac` and
        /// an encoder that actually exists in this process.
        transcode_ok: bool,
        /// `--audio-rate`, forwarded to the transcoder.
        forced_rate: Option<u32>,
        pending: Vec<BcMedia>,
    },
    /// MPEG-TS, muxing.
    TsRunning {
        mux: TsMuxer,
        clock: Clock,
        /// `Some` only when the camera sends ADPCM and an encoder was built.
        transcoder: Option<AdpcmTranscoder>,
    },
}

impl Framer {
    fn new(
        format: Format,
        audio_mode: AudioMode,
        transcode_ok: bool,
        forced_rate: Option<u32>,
    ) -> Self {
        match format {
            Format::H26x => Framer::Raw { started: false },
            Format::Ts => Framer::TsLearning {
                deadline: None,
                video: None,
                audio: None,
                transcoded: false,
                // `--audio none` answers the audio question before any frame
                // arrives, so those cameras never pay the learning wait.
                audio_known: audio_mode == AudioMode::None,
                transcode_ok,
                forced_rate,
                pending: Vec::new(),
            },
        }
    }

    /// Frame one media unit into `out`.
    ///
    /// Returns whether this counted as a frame arriving from the camera, which
    /// is what the stale timeout is measured against. Note that this is true
    /// even while frames are being buffered during the learning phase: the
    /// camera is demonstrably alive.
    fn push(&mut self, media: &BcMedia, out: &mut Vec<u8>) -> bool {
        match self {
            Framer::Raw { started } => match media {
                BcMedia::Iframe(frame) => {
                    *started = true;
                    out.extend_from_slice(&frame.data);
                    true
                }
                BcMedia::Pframe(frame) => {
                    // A decoder attaching mid-GOP has no parameter sets and no
                    // reference frame, so hold output until the first I-frame.
                    if *started {
                        out.extend_from_slice(&frame.data);
                    }
                    true
                }
                _ => false,
            },
            Framer::TsLearning {
                deadline,
                video,
                audio,
                transcoded,
                audio_known,
                transcode_ok,
                forced_rate,
                pending,
            } => {
                let deadline = *deadline.get_or_insert_with(|| Instant::now() + LEARN_TIMEOUT);
                let counted = match media {
                    BcMedia::Iframe(frame) => {
                        *video = Some(frame.video_type);
                        true
                    }
                    BcMedia::Pframe(frame) => {
                        *video = Some(frame.video_type);
                        true
                    }
                    BcMedia::Aac(_) => {
                        *audio = Some(AudioKind::Aac);
                        *transcoded = false;
                        *audio_known = true;
                        false
                    }
                    // ADPCM has no MPEG-TS stream type of its own, so it goes
                    // into the PMT as AAC and through the encoder on the way
                    // out. A camera that sent both would already have been
                    // caught by the `Aac` arm above.
                    BcMedia::Adpcm(_) => {
                        if audio.is_none() && *transcode_ok {
                            *audio = Some(AudioKind::Aac);
                            *transcoded = true;
                        }
                        // Answered either way: with no encoder this camera's
                        // audio is simply uncarryable, and waiting out the rest
                        // of the window would not change that.
                        *audio_known = true;
                        false
                    }
                    _ => false,
                };
                // Only start buffering at a key frame; anything before it is
                // undecodable and would only delay the PMT.
                let buffering = !pending.is_empty() || matches!(media, BcMedia::Iframe(_));
                if buffering {
                    pending.push(media.clone());
                }

                let both_known = video.is_some() && *audio_known;
                let expired = Instant::now() >= deadline || pending.len() >= LEARN_MAX_FRAMES;
                if video.is_some() && (both_known || expired) {
                    let video = video.expect("checked just above");
                    let audio = *audio;
                    // The one line that says what this process is doing with
                    // audio: carried, transcoded, or absent.
                    info!(
                        "Track layout learned after {} buffered units: video {video:?}, audio {}",
                        pending.len(),
                        match (audio, *transcoded) {
                            (Some(AudioKind::Aac), true) => "adpcm -> aac (transcoded)",
                            (Some(AudioKind::Aac), false) => "aac (camera, passthrough)",
                            (None, _) => "none",
                        }
                    );
                    let mut transcoder = transcoded.then(|| AdpcmTranscoder::new(*forced_rate));
                    let mut mux = TsMuxer::new(video, audio);
                    let mut clock = Clock::default();
                    let replay = std::mem::take(pending);
                    for frame in &replay {
                        write_ts(&mut mux, &mut clock, &mut transcoder, frame, out);
                    }
                    *self = Framer::TsRunning {
                        mux,
                        clock,
                        transcoder,
                    };
                }
                counted
            }
            Framer::TsRunning {
                mux,
                clock,
                transcoder,
            } => write_ts(mux, clock, transcoder, media, out),
        }
    }
}

/// Mux one media unit into the transport stream. Returns whether it counted as
/// a camera frame for staleness purposes.
fn write_ts(
    mux: &mut TsMuxer,
    clock: &mut Clock,
    transcoder: &mut Option<AdpcmTranscoder>,
    media: &BcMedia,
    out: &mut Vec<u8>,
) -> bool {
    match media {
        BcMedia::Iframe(frame) => {
            let pts = clock.video(frame.microseconds);
            mux.write_video(out, &frame.data, pts, true);
            true
        }
        BcMedia::Pframe(frame) => {
            let pts = clock.video(frame.microseconds);
            mux.write_video(out, &frame.data, pts, false);
            true
        }
        BcMedia::Aac(frame) => {
            if mux.has_audio() {
                if let Some(pts) = clock.audio(frame.duration()) {
                    mux.write_audio(out, &frame.data, pts);
                }
            }
            false
        }
        // ADPCM has no MPEG-TS stream type, so it is decoded to PCM and
        // re-encoded as AAC, which does. The frames come out of the encoder in
        // bursts (nothing at all until the sample rate has been measured, then
        // ~1 s at once), which is exactly what the accumulate-and-re-anchor
        // audio clock below already handles.
        BcMedia::Adpcm(frame) => {
            if let Some(transcoder) = transcoder.as_mut() {
                let transcoded = transcoder.feed(&frame.data, clock.last_video_90k);
                if let Some(anchor) = transcoded.anchor_90k {
                    clock.anchor_audio(anchor);
                }
                if mux.has_audio() {
                    for aac in &transcoded.frames {
                        if let Some(pts) = clock.audio(adts_duration_micros(aac)) {
                            mux.write_audio(out, aac, pts);
                        }
                    }
                }
            }
            false
        }
        // InfoV1/V2 carry no media.
        _ => false,
    }
}

/// Converts the camera's clocks into a monotonic 90 kHz presentation clock.
///
/// Two things make this more than a multiplication:
///
/// * The BC video stamp is a `u32` count of microseconds, so it wraps every
///   71.58 minutes. A wrap must extend into 64 bits; a camera-side *reset*
///   (which looks identical in isolation) must not, or the output jumps
///   forward by 4295 seconds.
/// * BC audio frames carry no timestamp at all. Their PTS is accumulated from
///   the ADTS frame durations and re-anchored to video if it drifts, which is
///   the best that can be done without a real audio clock.
#[derive(Default)]
struct Clock {
    last_micros: Option<u32>,
    /// Added to the raw `u32` to undo wraps and resets.
    offset: u64,
    /// Absolute microseconds of the first frame, so output starts near zero.
    origin: Option<u64>,
    last_video_90k: u64,
    audio_90k: Option<u64>,
    /// Audio frames dropped since the last re-anchor to hold the clock on video.
    audio_drops: u64,
    /// Length of the last audio frame in 90 kHz ticks (0 until one is seen),
    /// so the guard band follows the source's sample rate.
    audio_frame_90k: u64,
    /// Set by `video` when the audio clock is more than the guard band ahead
    /// of the stamp that just arrived; `audio` then drops one frame.
    drop_next_audio: bool,
}

impl Clock {
    /// Extend a raw video stamp into a monotonic 90 kHz PTS.
    fn video(&mut self, micros: u32) -> u64 {
        if let Some(last) = self.last_micros {
            if micros < last {
                if last >= WRAP_GUARD {
                    // Genuine u32 wrap.
                    self.offset += u64::from(u32::MAX) + 1;
                } else {
                    // The camera restarted its counter, almost always because
                    // the BC session was re-established. Rebase so the output
                    // clock keeps moving forward rather than jumping back.
                    debug!("Camera timestamp reset ({last} -> {micros}), rebasing");
                    self.offset += u64::from(last - micros);
                }
            }
        }
        self.last_micros = Some(micros);
        let absolute = self.offset + u64::from(micros);
        let origin = *self.origin.get_or_insert(absolute);
        let pts = micros_to_90k(absolute.saturating_sub(origin));
        self.last_video_90k = pts;
        // Judge the audio clock only here, against a stamp that has just
        // arrived. See `AUDIO_GUARD_NUM` for why not on every audio frame.
        // Re-evaluated on every video frame, so a pending drop is cancelled
        // if the next stamp shows it is no longer needed.
        if let Some(audio) = self.audio_90k {
            let ahead = audio as i64 - pts as i64;
            self.drop_next_audio = ahead > self.audio_guard_90k();
        }
        pts
    }

    /// The guard band in 90 kHz ticks: one and a half audio frames, from the
    /// last frame length seen (nominal until one has been).
    fn audio_guard_90k(&self) -> i64 {
        let frame = if self.audio_frame_90k == 0 {
            micros_to_90k(u64::from(AUDIO_FRAME_MICROS))
        } else {
            self.audio_frame_90k
        };
        (frame as i64 * AUDIO_GUARD_NUM) / AUDIO_GUARD_DEN
    }

    /// Pin the audio clock to a known video PTS.
    ///
    /// Used once, when the transcoder releases the audio it held while
    /// measuring the camera's sample rate: those samples belong at the video
    /// time they were captured, not at the video time the encoder started.
    fn anchor_audio(&mut self, pts_90k: u64) {
        self.audio_90k = Some(pts_90k);
    }

    /// Next audio PTS, advancing by `duration` microseconds if the ADTS header
    /// could be parsed.
    ///
    /// Returns `None` when the frame must be dropped because the audio clock
    /// has run ahead of the camera's video clock. A camera that delivers more
    /// audio frames than its own video clock accounts for (measured: 0.067%,
    /// one surplus 1024 sample frame every 96 s) otherwise pushes that surplus
    /// downstream forever. Dropping is the only correction that survives a
    /// consumer which rebuilds the audio timeline by counting samples and
    /// ignores the PTS entirely, which is what go2rtc's mp4 muxer did: there
    /// the surplus became unbounded A/V drift, 2.4 s per hour, cleared only by
    /// a reconnect.
    ///
    /// The decision to drop is taken in [`Clock::video`], when a fresh video
    /// stamp arrives; this only carries it out. The clock is held while the
    /// frame is dropped, so one dropped frame removes exactly one frame of
    /// surplus and the PTS grid downstream stays continuous.
    ///
    /// Only the ahead direction is corrected. Audio behind video is left to
    /// the `AUDIO_RESYNC_90K` snap above, because the frames that would close
    /// the gap do not exist and inventing them (padding with silence) would
    /// put made up audio into a recording. The same snap is the backstop for
    /// the ahead direction while video is absent.
    fn audio(&mut self, duration: Option<u32>) -> Option<u64> {
        let pts = *self.audio_90k.get_or_insert(self.last_video_90k);
        let drift = pts as i64 - self.last_video_90k as i64;
        if drift.abs() > AUDIO_RESYNC_90K {
            debug!("Audio clock drifted {drift} ticks from video, re-anchoring");
            self.audio_90k = Some(self.last_video_90k);
            self.audio_drops = 0;
            self.drop_next_audio = false;
            return Some(self.last_video_90k);
        }

        if let Some(duration) = duration {
            self.audio_frame_90k = micros_to_90k(u64::from(duration));
        }

        if std::mem::take(&mut self.drop_next_audio) {
            // Hold the clock where it is: the video clock keeps moving, so
            // each dropped frame removes exactly one frame of surplus.
            self.audio_drops += 1;
            if self.audio_drops == 1 {
                info!(
                    "Audio clock {drift} ticks ({} ms) ahead of video, dropping frames to hold it",
                    drift / 90
                );
            } else {
                debug!(
                    "Audio clock {drift} ticks ahead of video, dropped {} frames this session",
                    self.audio_drops
                );
            }
            return None;
        }

        self.audio_90k = Some(pts + micros_to_90k(u64::from(duration.unwrap_or(0))));
        Some(pts)
    }
}

/// Microseconds to 90 kHz ticks.
fn micros_to_90k(micros: u64) -> u64 {
    micros.saturating_mul(9) / 100
}

#[cfg(test)]
mod tests {
    use super::*;

    use neolink_core::bcmedia::model::BcMediaIframe;
    use tokio::io::AsyncReadExt;

    fn iframe(micros: u32, bytes: usize) -> BcMedia {
        BcMedia::Iframe(BcMediaIframe {
            video_type: VideoType::H264,
            microseconds: micros,
            time: None,
            data: vec![0x5Au8; bytes],
        })
    }

    /// Rust ignores `SIGPIPE`, so a consumer walking away surfaces as an
    /// `io::Error`. Only the three kinds that mean "the reader went away" are a
    /// clean shutdown; anything else has to stay an error.
    #[test]
    fn only_reader_gone_errors_count_as_a_closed_pipe() {
        use std::io::{Error, ErrorKind};
        for kind in [
            ErrorKind::BrokenPipe,
            ErrorKind::ConnectionReset,
            ErrorKind::ConnectionAborted,
        ] {
            assert!(is_pipe_closed(&Error::new(kind, "x")), "{:?}", kind);
        }
        for kind in [
            ErrorKind::PermissionDenied,
            ErrorKind::OutOfMemory,
            ErrorKind::WouldBlock,
            ErrorKind::Other,
        ] {
            assert!(!is_pipe_closed(&Error::new(kind, "x")), "{:?}", kind);
        }
    }

    /// Rule 1 of pipe mode: stdout closed means the consumer left, so exit 0.
    /// Driven through a real socket pair whose reader has been dropped, which
    /// is what go2rtc killing an `exec:` producer looks like.
    #[tokio::test]
    async fn pump_exits_zero_when_the_consumer_closes_the_pipe() {
        let (writer, reader) = tokio::net::UnixStream::pair().unwrap();
        drop(reader);

        let (tx, mut rx) = channel(8);
        // Big enough that the write cannot be absorbed by a socket buffer.
        tx.send(iframe(0, 512 * 1024)).await.unwrap();

        let result = pump(
            "testcam",
            StreamKind::Main,
            Duration::from_secs(30),
            &mut rx,
            &mut Sink::with_writer(writer),
            &mut Framer::new(Format::H26x, AudioMode::None, false, None),
            &AtomicU64::new(0),
        )
        .await;
        assert!(result.is_ok(), "expected a clean exit, got {:?}", result);
    }

    /// Rule 2: no frame for `--stale-timeout` means the camera left, so exit
    /// non-zero. The supervisor's respawn is the only watchdog pipe mode has,
    /// so this timer has to fire even though the pipe and the channel are both
    /// perfectly healthy.
    #[tokio::test]
    async fn pump_exits_non_zero_when_frames_stop() {
        let (writer, mut reader) = tokio::net::UnixStream::pair().unwrap();
        tokio::spawn(async move {
            let mut sink = Vec::new();
            let _ = reader.read_to_end(&mut sink).await;
        });

        let (tx, mut rx) = channel(8);
        tx.send(iframe(0, 64)).await.unwrap();
        // Keep the channel open for the whole test: the exit under test is the
        // timeout, not the channel closing.
        let _keepalive = tx;

        let started = Instant::now();
        let result = pump(
            "testcam",
            StreamKind::Main,
            Duration::from_millis(300),
            &mut rx,
            &mut Sink::with_writer(writer),
            &mut Framer::new(Format::H26x, AudioMode::None, false, None),
            &AtomicU64::new(0),
        )
        .await;
        let elapsed = started.elapsed();

        let err = result.expect_err("a stalled camera must not exit 0");
        eprintln!("stale exit after {elapsed:?}: {err}");
        assert!(err.to_string().contains("no frame for"), "{}", err);
        assert!(
            elapsed >= Duration::from_millis(300) && elapsed < Duration::from_secs(2),
            "fired at {:?}, expected ~300 ms after the last frame",
            elapsed
        );
    }

    /// The other non-zero exit: `run_task` gave up and closed the media
    /// channel. Re-subscribing on the same reactor was measured to be useless,
    /// so the process exits and lets the supervisor rebuild everything.
    #[tokio::test]
    async fn pump_exits_non_zero_when_the_subscription_ends() {
        let (writer, mut reader) = tokio::net::UnixStream::pair().unwrap();
        tokio::spawn(async move {
            let mut sink = Vec::new();
            let _ = reader.read_to_end(&mut sink).await;
        });

        let (tx, mut rx) = channel(8);
        tx.send(iframe(0, 64)).await.unwrap();
        drop(tx);

        let err = pump(
            "testcam",
            StreamKind::Main,
            Duration::from_secs(30),
            &mut rx,
            &mut Sink::with_writer(writer),
            &mut Framer::new(Format::H26x, AudioMode::None, false, None),
            &AtomicU64::new(0),
        )
        .await
        .expect_err("a dead subscription must not exit 0");
        assert!(err.to_string().contains("subscription ended"), "{}", err);
    }

    /// The negative control for all three exit tests: while frames arrive and
    /// the consumer reads, `pump` writes them out, keeps running, and keeps the
    /// camera thread's liveness cell fresh (without which the BC watchdog tears
    /// down a healthy connection on a timer).
    #[tokio::test]
    async fn pump_forwards_frames_and_marks_the_camera_alive() {
        let (writer, mut reader) = tokio::net::UnixStream::pair().unwrap();
        let collected = tokio::spawn(async move {
            let mut sink = Vec::new();
            let _ = reader.read_to_end(&mut sink).await;
            sink
        });

        let (tx, mut rx) = channel(8);
        for n in 0..5u32 {
            tx.send(iframe(n * 50_000, 128)).await.unwrap();
        }
        drop(tx);

        let last_frame_at = AtomicU64::new(0);
        let _ = pump(
            "testcam",
            StreamKind::Main,
            Duration::from_secs(30),
            &mut rx,
            &mut Sink::with_writer(writer),
            &mut Framer::new(Format::H26x, AudioMode::None, false, None),
            &last_frame_at,
        )
        .await;

        let bytes = collected.await.unwrap();
        assert_eq!(bytes.len(), 5 * 128, "every frame should reach the consumer");
        assert!(
            last_frame_at.load(Ordering::Relaxed) > 0,
            "the liveness cell was never written"
        );
    }

    #[test]
    fn video_clock_handles_a_real_wrap() {
        let mut clock = Clock::default();
        let near_top = u32::MAX - 1_000_000;
        assert_eq!(clock.video(near_top), 0);
        // 2 s later, having wrapped.
        let after = 1_000_000u32;
        assert_eq!(clock.video(after), micros_to_90k(2_000_001));
    }

    #[test]
    fn video_clock_treats_a_low_reset_as_a_rebase_not_a_wrap() {
        let mut clock = Clock::default();
        assert_eq!(clock.video(5_000_000), 0);
        // Camera reconnected and restarted its counter near zero. The output
        // must stay put rather than leap 4295 s forward.
        let pts = clock.video(1_000);
        assert!(pts < micros_to_90k(1_000_000), "unexpected jump to {}", pts);
    }

    #[test]
    fn video_clock_is_monotonic_across_a_reset() {
        let mut clock = Clock::default();
        let mut previous = 0;
        for micros in [1_000u32, 500_000, 1_000_000, 100, 200_000] {
            let pts = clock.video(micros);
            assert!(pts >= previous, "{} went backwards from {}", pts, previous);
            previous = pts;
        }
    }

    fn adpcm() -> BcMedia {
        // A DVI4 block as `de.rs` hands it over: 4 bytes of predictor state
        // (zeroed) then nibble pairs. Built inline so this test compiles
        // without the `gstreamer` feature, where `super::adpcm` is not.
        let mut data = vec![0u8; 4];
        data.resize(244, 0x35);
        BcMedia::Adpcm(neolink_core::bcmedia::model::BcMediaAdpcm { data })
    }

    /// The learning window must start at the first frame, not at construction.
    ///
    /// `NeoCam` init takes 4.4 s, so a deadline anchored at `Framer::new` has
    /// always expired by frame one and the PMT is written before any audio can
    /// be seen. That is what shipped video-only PMTs to camera A and camera C.
    #[test]
    fn the_learning_window_starts_at_the_first_frame() {
        let mut framer = Framer::new(Format::Ts, AudioMode::Aac, true, Some(16000));
        // Stand in for the cold start: the camera delivers nothing for longer
        // than the whole learning window.
        std::thread::sleep(LEARN_TIMEOUT + Duration::from_millis(50));
        let mut out = Vec::new();
        framer.push(&iframe(0, 64), &mut out);
        framer.push(&adpcm(), &mut out);
        framer.push(&iframe(66_667, 64), &mut out);
        match &framer {
            Framer::TsRunning { mux, .. } => {
                assert!(mux.has_audio(), "PMT came out video-only");
            }
            Framer::TsLearning { audio, .. } => {
                assert_eq!(*audio, Some(AudioKind::Aac), "audio not learned");
            }
            Framer::Raw { .. } => panic!("wrong framer"),
        }
    }

    /// `--audio none` must not pay the learning wait at all.
    #[test]
    fn audio_none_emits_the_pmt_on_the_first_key_frame() {
        let mut framer = Framer::new(Format::Ts, AudioMode::None, false, None);
        let mut out = Vec::new();
        framer.push(&iframe(0, 64), &mut out);
        match &framer {
            Framer::TsRunning { mux, .. } => assert!(!mux.has_audio()),
            _ => panic!("still learning after the first key frame"),
        }
    }

    #[test]
    fn audio_clock_is_monotonic_across_a_transcoded_run() {
        let mut clock = Clock::default();
        // 15 fps video and 64 ms AAC frames, which is 1024 samples at 16 kHz:
        // the steady state of a transcoded camera C.
        let mut previous = 0;
        let mut last_video = 0;
        for i in 0..300u64 {
            last_video = clock.video((i * 66_667) as u32);
            let pts = clock
                .audio(Some(64_000))
                .expect("a source on the nominal rate must not have frames dropped");
            assert!(pts >= previous, "{} went backwards from {}", pts, previous);
            previous = pts;
        }
        // 300 frames is ~20 s. Audio must still be sitting on video, i.e. the
        // drift guard neither fired nor was needed.
        let drift = previous as i64 - last_video as i64;
        assert!(
            drift.abs() < AUDIO_RESYNC_90K,
            "audio drifted {} ticks from video over 300 frames",
            drift
        );
    }

    #[test]
    fn anchoring_puts_held_audio_back_where_it_was_captured() {
        let mut clock = Clock::default();
        clock.video(0);
        // One second of video passes while the transcoder measures the sample
        // rate and holds the audio.
        for i in 1..=15u64 {
            clock.video((i * 66_667) as u32);
        }
        // The held audio belongs at the start of that second, not at "now".
        clock.anchor_audio(0);
        let first = clock
            .audio(Some(64_000))
            .expect("held audio must not be dropped");
        assert_eq!(first, 0);
        // Flushing ~1 s of it lands back alongside the current video PTS.
        let mut pts = first;
        for _ in 0..14 {
            pts = clock
                .audio(Some(64_000))
                .expect("catching up audio must not be dropped");
        }
        let video = clock.last_video_90k;
        assert!(
            (pts as i64 - video as i64).abs() < micros_to_90k(200_000) as i64,
            "flushed audio ended at {}, video is at {}",
            pts,
            video
        );
    }

    #[test]
    fn audio_clock_reanchors_when_it_drifts() {
        let mut clock = Clock::default();
        clock.video(0);
        // 21 audio frames of ~1 s each with no video advancing: the accumulated
        // clock must be pulled back to video rather than running away.
        let mut pts = 0;
        for _ in 0..21 {
            if let Some(next) = clock.audio(Some(1_000_000)) {
                pts = next;
            }
        }
        assert!(
            pts <= micros_to_90k(3_000_000),
            "audio clock ran away to {}",
            pts
        );
    }

    /// Emit one hour of a camera whose audio runs `surplus` faster than its own
    /// video clock, and report (kept, dropped, final drift, drift the old
    /// accumulate-only clock would have reached).
    fn run_surplus(surplus: f64) -> (u64, u64, i64, i64) {
        const VIDEO_FRAME_MICROS: u64 = 66_667; // 15 fps
        const SECONDS: u64 = 3600;

        let mut clock = Clock::default();
        let mut offered = 0u64;
        let mut kept = 0u64;

        let video_frames = SECONDS * 1_000_000 / VIDEO_FRAME_MICROS;
        for i in 0..video_frames {
            let video_micros = i * VIDEO_FRAME_MICROS;
            clock.video(video_micros as u32);

            // How many audio frames the camera has handed over by now.
            let want = ((video_micros as f64 / f64::from(AUDIO_FRAME_MICROS)) * surplus) as u64;
            while offered < want {
                offered += 1;
                if clock.audio(Some(AUDIO_FRAME_MICROS)).is_some() {
                    kept += 1;
                }
            }
        }

        let video = clock.last_video_90k as i64;
        let drift = clock.audio_90k.unwrap_or(0) as i64 - video;
        // What an accumulate-only clock reaches: one frame per offered frame.
        let undisciplined =
            (offered as i64) * micros_to_90k(u64::from(AUDIO_FRAME_MICROS)) as i64 - video;

        (kept, clock.audio_drops, drift, undisciplined)
    }

    /// The measured defect: the camera hands over 0.067% more AAC frames than
    /// its own video clock accounts for (156.35 per 10.000 s of video where
    /// 16 kHz gives 156.25). The clock must shed exactly that surplus instead
    /// of carrying it until the two second snap fires.
    #[test]
    fn audio_clock_drops_only_the_surplus_frames() {
        let (kept, dropped, drift, undisciplined) = run_surplus(1.000_67);
        println!(
            "surplus 0.067%: kept {kept}, dropped {dropped}, end drift {drift} ticks, \
             accumulate-only drift {undisciplined} ticks ({} ms)",
            undisciplined / 90
        );

        let frame = micros_to_90k(u64::from(AUDIO_FRAME_MICROS)) as i64;
        let guard = (frame * AUDIO_GUARD_NUM) / AUDIO_GUARD_DEN;
        // The band is judged once per video frame and at most one frame is
        // dropped per judgement, while up to two audio frames arrive between
        // judgements, so the clock may sit up to two frames past the band at
        // any instant. What matters is that it never reaches the two second
        // snap.
        assert!(
            drift <= guard + 2 * frame,
            "audio ended {} ticks ahead of video, guard is {} plus two frames",
            drift,
            guard
        );
        assert!(dropped > 0, "no frame was dropped, surplus was absorbed");
        // One hour of 0.067% surplus is ~37.7 frames, ~2.4 s.
        assert!(
            (30..=50).contains(&dropped),
            "dropped {} frames, expected the ~38 frame surplus",
            dropped
        );
        assert!(kept > 56_000, "dropped far too much: kept only {}", kept);
        // For the record: what the old accumulate-only clock handed downstream
        // between snaps, and what go2rtc then turned into unbounded drift.
        assert!(
            undisciplined > 2 * 90_000,
            "surplus model is wrong, only {} ticks",
            undisciplined
        );
    }

    /// A camera whose audio matches its video clock must keep every frame.
    #[test]
    fn audio_clock_keeps_every_frame_at_the_nominal_rate() {
        let (kept, dropped, drift, _) = run_surplus(1.0);
        println!("nominal rate: kept {kept}, dropped {dropped}, end drift {drift} ticks");

        assert_eq!(dropped, 0, "dropped {} frames at the nominal rate", dropped);
        assert!(kept > 56_000, "kept only {} frames in an hour", kept);
        assert!(
            drift.abs() <= 6_000,
            "drift {} ticks at the nominal rate",
            drift
        );
    }
    /// A camera-side video hiccup: video stops for `gap_micros` at t=600 s
    /// while audio keeps arriving at the nominal rate, then resumes with
    /// correct stamps. Returns (kept, dropped, final drift).
    fn run_video_gap(gap_micros: u64) -> (u64, u64, i64) {
        const VIDEO_FRAME_MICROS: u64 = 66_667; // 15 fps
        const SECONDS: u64 = 1200;
        const GAP_AT_MICROS: u64 = 600 * 1_000_000;

        let mut clock = Clock::default();
        let mut offered = 0u64;
        let mut kept = 0u64;

        let video_frames = SECONDS * 1_000_000 / VIDEO_FRAME_MICROS;
        for i in 0..video_frames {
            let video_micros = i * VIDEO_FRAME_MICROS;
            let in_gap = video_micros >= GAP_AT_MICROS && video_micros < GAP_AT_MICROS + gap_micros;
            if !in_gap {
                clock.video(video_micros as u32);
            }
            let want = video_micros / u64::from(AUDIO_FRAME_MICROS);
            while offered < want {
                offered += 1;
                if clock.audio(Some(AUDIO_FRAME_MICROS)).is_some() {
                    kept += 1;
                }
            }
        }
        let drift = clock.audio_90k.unwrap_or(0) as i64 - clock.last_video_90k as i64;
        (kept, clock.audio_drops, drift)
    }

    /// Nothing about the audio is wrong when video pauses, so no frame may be
    /// dropped and the clocks must agree again once video is back. Gaps under
    /// the two second backstop are the ones the guard alone has to get right;
    /// judging the audio clock against a stale video stamp used to drop two
    /// frames on a 200 ms gap and leave the audio 61 ms behind for good.
    #[test]
    fn audio_clock_ignores_a_video_gap() {
        for gap_ms in [200u64, 500, 1500] {
            let (kept, dropped, drift) = run_video_gap(gap_ms * 1000);
            println!(
                "{} ms video gap: kept {}, dropped {}, end drift {} ticks ({} ms)",
                gap_ms,
                kept,
                dropped,
                drift,
                drift / 90
            );
            assert_eq!(
                dropped, 0,
                "dropped {} frames across a {} ms video gap",
                dropped, gap_ms
            );
            assert!(
                drift.abs() <= 6_000,
                "drift {} ticks ({} ms) after a {} ms video gap",
                drift,
                drift / 90,
                gap_ms
            );
        }
    }
}
