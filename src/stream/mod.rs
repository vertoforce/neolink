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

mod cmdline;
mod mpegts;

use crate::common::{now_epoch_ms, NeoInstance, NeoReactor};
use crate::AnyResult;
use cmdline::Format;
pub(crate) use cmdline::Opt;
use mpegts::{AudioKind, TsMuxer};

/// How long the codec-learning phase may buffer frames before giving up on
/// ever seeing an audio track and emitting a video-only PMT.
const LEARN_TIMEOUT: Duration = Duration::from_secs(3);
/// Hard cap on frames buffered during the learning phase, so a camera that
/// sends video but never audio cannot grow the buffer without bound.
const LEARN_MAX_FRAMES: usize = 20;
/// Only treat a backwards jump in the camera's microsecond stamp as a counter
/// wrap when the previous value was close to the top of the `u32`. Anything
/// else is a camera-side reset (a reconnect), which must not add 2^32.
const WRAP_GUARD: u32 = 0xF000_0000;
/// Re-anchor the synthetic audio clock if it drifts this far from video.
const AUDIO_RESYNC_90K: i64 = 2 * 90_000;

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
    let mut framer = Framer::new(opt.format);

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
        let result = camera
            .run_task(move |cam| {
                let media_tx = media_tx.clone();
                Box::pin(async move {
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
        deadline: Instant,
        video: Option<VideoType>,
        audio: Option<AudioKind>,
        pending: Vec<BcMedia>,
    },
    /// MPEG-TS, muxing.
    TsRunning { mux: TsMuxer, clock: Clock },
}

impl Framer {
    fn new(format: Format) -> Self {
        match format {
            Format::H26x => Framer::Raw { started: false },
            Format::Ts => Framer::TsLearning {
                deadline: Instant::now() + LEARN_TIMEOUT,
                video: None,
                audio: None,
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
                pending,
            } => {
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
                        false
                    }
                    BcMedia::Adpcm(_) => false,
                    _ => false,
                };
                // Only start buffering at a key frame; anything before it is
                // undecodable and would only delay the PMT.
                let buffering = !pending.is_empty() || matches!(media, BcMedia::Iframe(_));
                if buffering {
                    pending.push(media.clone());
                }

                let both_known = video.is_some() && audio.is_some();
                let expired = Instant::now() >= *deadline || pending.len() >= LEARN_MAX_FRAMES;
                if video.is_some() && (both_known || expired) {
                    let video = video.expect("checked just above");
                    let audio = *audio;
                    info!(
                        "Track layout learned: video {video:?}, audio {}",
                        match audio {
                            Some(kind) => format!("{kind:?}"),
                            None => "none".to_string(),
                        }
                    );
                    let mut mux = TsMuxer::new(video, audio);
                    let mut clock = Clock::default();
                    let replay = std::mem::take(pending);
                    for frame in &replay {
                        write_ts(&mut mux, &mut clock, frame, out);
                    }
                    *self = Framer::TsRunning { mux, clock };
                }
                counted
            }
            Framer::TsRunning { mux, clock } => write_ts(mux, clock, media, out),
        }
    }
}

/// Mux one media unit into the transport stream. Returns whether it counted as
/// a camera frame for staleness purposes.
fn write_ts(mux: &mut TsMuxer, clock: &mut Clock, media: &BcMedia, out: &mut Vec<u8>) -> bool {
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
                let pts = clock.audio(frame.duration());
                mux.write_audio(out, &frame.data, pts);
            }
            false
        }
        // ADPCM has no MPEG-TS stream type, and InfoV1/V2 carry no media.
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
        pts
    }

    /// Next audio PTS, advancing by `duration` microseconds if the ADTS header
    /// could be parsed.
    fn audio(&mut self, duration: Option<u32>) -> u64 {
        let pts = *self.audio_90k.get_or_insert(self.last_video_90k);
        let drift = pts as i64 - self.last_video_90k as i64;
        if drift.abs() > AUDIO_RESYNC_90K {
            debug!("Audio clock drifted {drift} ticks from video, re-anchoring");
            self.audio_90k = Some(self.last_video_90k);
            return self.last_video_90k;
        }
        self.audio_90k = Some(pts + micros_to_90k(u64::from(duration.unwrap_or(0))));
        pts
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
            &mut Framer::new(Format::H26x),
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
            &mut Framer::new(Format::H26x),
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
            &mut Framer::new(Format::H26x),
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
            &mut Framer::new(Format::H26x),
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

    #[test]
    fn audio_clock_reanchors_when_it_drifts() {
        let mut clock = Clock::default();
        clock.video(0);
        // 21 audio frames of ~1 s each with no video advancing: the accumulated
        // clock must be pulled back to video rather than running away.
        let mut pts = 0;
        for _ in 0..21 {
            pts = clock.audio(Some(1_000_000));
        }
        assert!(
            pts <= micros_to_90k(3_000_000),
            "audio clock ran away to {}",
            pts
        );
    }
}
