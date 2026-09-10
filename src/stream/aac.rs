//! ADPCM -> PCM -> AAC transcoding for the pipe path.
//!
//! MPEG-TS has a `stream_type` for AAC (`0x0F`, ADTS framing) and none for the
//! DVI4/IMA ADPCM the Reolink BC protocol actually delivers. Before this
//! module the muxer's only options were "carry the camera's AAC verbatim" or
//! "emit a video-only PMT", and every camera here is in the second group, so
//! pipe mode silently dropped audio that the RTSP path had been carrying.
//!
//! The decode is [`super::adpcm`] (pure Rust). The encode is a four-element
//! GStreamer pipeline driven by `push`/`pull` rather than a clock:
//!
//! ```text
//! appsrc(audio/x-raw,S16LE,mono) ! audioconvert ! audioresample
//!   ! capsfilter(rate) ! voaacenc ! aacparse ! capsfilter(adts) ! appsink
//! ```
//!
//! `voaacenc` is the encoder because it is the only AAC encoder in Debian
//! bookworm's `gstreamer1.0-plugins-bad` (measured: `fdkaacenc` is not built,
//! and `avenc_aac` needs `gstreamer1.0-libav`, whose `libav*` closure is an
//! order of magnitude more to ship into Frigate's rootfs). It takes S16LE
//! mono at 8-96 kHz and emits ADTS directly, which is exactly what
//! [`super::mpegts::TsMuxer::write_audio`] already carries for AAC cameras.
//!
//! Nothing here is live: `is-live=false` on the source and `sync=false` on the
//! sink mean the pipeline runs as fast as it is fed and never blocks on a
//! clock. Output is drained non-blocking after each push, so a frame may lag
//! its input by one push; presentation time is not taken from GStreamer at all
//! (see [`AacEncoder::push`]).

use anyhow::{anyhow, Context, Result};
use gstreamer::{prelude::*, Buffer, Caps, ClockTime, ElementFactory, Format, Pipeline, State};
use gstreamer_app::{AppSink, AppSrc};
use log::*;

use super::adpcm::adpcm_to_pcm;

/// Elements the transcoder needs. Checked before the PMT is written, because a
/// PMT that advertises an audio track we then cannot produce is worse than no
/// audio at all: the consumer waits for a stream that never arrives.
pub(super) const REQUIRED_ELEMENTS: &[&str] = &[
    "appsrc",
    "audioconvert",
    "audioresample",
    "capsfilter",
    "voaacenc",
    "aacparse",
    "appsink",
];

/// Sample rates `voaacenc` accepts, which is also the set the measured rate is
/// snapped to.
const SUPPORTED_RATES: &[u32] = &[
    8000, 11025, 12000, 16000, 22050, 24000, 32000, 44100, 48000,
];

/// Samples per AAC-LC access unit. Fixed by the codec.
pub(super) const AAC_SAMPLES_PER_FRAME: u32 = 1024;

/// How many access units of encoder delay to tolerate before treating output
/// as overdue. `voaacenc` holds about one; two is slack.
const ENCODER_LATENCY_FRAMES: u64 = 2;

/// How long to wait for an access unit the encoder already owes us.
///
/// The drain is otherwise non-blocking, which is wrong on its own: a
/// `try_pull_sample(0)` only sees what the encoder's streaming thread has
/// already produced, so a caller that pushes back-to-back can outrun it
/// indefinitely. Measured — the release build of the end-to-end test pushed
/// 2.5 s of audio and drained **0** frames, while the same test in debug (slow
/// enough for the thread to keep up) drained 39. Live audio arrives every
/// ~60 ms, which is ample slack, but "ample slack in the normal case" is how
/// the muxer would end up emitting audio PES arbitrarily far behind video.
const DRAIN_TIMEOUT_MS: u64 = 20;

/// Bitrate for the encoder, as a multiple of the sample rate, capped.
///
/// `voaacenc` rejects a bitrate outside roughly 0.5x-6x the sample rate per
/// channel, so a fixed 64 kbps would fail at 8 kHz. 4x lands at 32 kbps for
/// 8 kHz mono and 64 kbps for 16 kHz mono, and the downstream `_norm`
/// re-encoders normalise to 64 kbps anyway.
fn bitrate_for(rate: u32) -> i32 {
    (rate * 4).min(64_000) as i32
}

/// Pick the supported rate closest (in ratio) to a measured one.
pub(super) fn snap_rate(measured: f64) -> u32 {
    let mut best = SUPPORTED_RATES[0];
    let mut best_err = f64::INFINITY;
    for &candidate in SUPPORTED_RATES {
        let err = (measured / f64::from(candidate)).ln().abs();
        if err < best_err {
            best_err = err;
            best = candidate;
        }
    }
    best
}

/// A running `appsrc -> voaacenc -> appsink` pipeline.
pub(super) struct AacEncoder {
    pipeline: Pipeline,
    appsrc: AppSrc,
    appsink: AppSink,
    rate: u32,
    /// Samples pushed so far, which is the input presentation clock.
    pushed_samples: u64,
    /// Access units pulled back out, for the overdue calculation.
    emitted_frames: u64,
}

impl AacEncoder {
    /// Names of the elements that are missing, if any.
    ///
    /// GStreamer plugins are `dlopen`ed from `GST_PLUGIN_PATH`, and the
    /// deployment that matters here bind-mounts a hand-built plugin directory
    /// into a container that ships no GStreamer at all, so "the binary links
    /// libgstreamer" says nothing about whether `voaacenc` can be created.
    pub(super) fn missing_elements() -> Vec<&'static str> {
        REQUIRED_ELEMENTS
            .iter()
            .copied()
            .filter(|name| ElementFactory::find(name).is_none())
            .collect()
    }

    /// Build and start a pipeline encoding mono S16LE at `rate`.
    pub(super) fn new(rate: u32) -> Result<Self> {
        let bitrate = bitrate_for(rate);
        let raw_caps = Caps::builder("audio/x-raw")
            .field("format", "S16LE")
            .field("layout", "interleaved")
            .field("rate", rate as i32)
            .field("channels", 1i32)
            .build();

        let appsrc = AppSrc::builder()
            .name("aacsrc")
            .caps(&raw_caps)
            .format(Format::Time)
            .is_live(false)
            .build();
        let convert = make("audioconvert")?;
        let resample = make("audioresample")?;
        // Belt and braces: audioresample is a no-op while the measured rate is
        // one voaacenc accepts, and the safety net if it ever is not.
        let rate_filter = ElementFactory::make("capsfilter")
            .name("aacrate")
            .property("caps", &raw_caps)
            .build()
            .context("Could not create capsfilter")?;
        let encoder = ElementFactory::make("voaacenc")
            .name("aacenc")
            .property("bitrate", bitrate)
            .build()
            .context("Could not create voaacenc")?;
        let parse = make("aacparse")?;
        let adts_caps = Caps::builder("audio/mpeg")
            .field("mpegversion", 4i32)
            .field("stream-format", "adts")
            .build();
        let adts_filter = ElementFactory::make("capsfilter")
            .name("aacadts")
            .property("caps", &adts_caps)
            .build()
            .context("Could not create the ADTS capsfilter")?;
        let appsink = AppSink::builder()
            .name("aacsink")
            .sync(false)
            .max_buffers(256)
            .build();

        let src_element = appsrc.clone().upcast::<gstreamer::Element>();
        let sink_element = appsink.clone().upcast::<gstreamer::Element>();
        let pipeline = Pipeline::with_name("neolink-adpcm-to-aac");
        pipeline
            .add_many([
                &src_element,
                &convert,
                &resample,
                &rate_filter,
                &encoder,
                &parse,
                &adts_filter,
                &sink_element,
            ])
            .context("Could not assemble the AAC pipeline")?;
        gstreamer::Element::link_many([
            &src_element,
            &convert,
            &resample,
            &rate_filter,
            &encoder,
            &parse,
            &adts_filter,
            &sink_element,
        ])
        .context("Could not link the AAC pipeline")?;

        pipeline
            .set_state(State::Playing)
            .context("Could not start the AAC pipeline")?;

        info!("ADPCM -> AAC: voaacenc, {rate} Hz mono, {bitrate} bps");
        Ok(Self {
            pipeline,
            appsrc,
            appsink,
            rate,
            pushed_samples: 0,
            emitted_frames: 0,
        })
    }

    /// Push one block of little-endian S16 mono PCM and return whatever ADTS
    /// frames are ready.
    ///
    /// The returned frames carry no presentation time. GStreamer's own output
    /// timestamps are deliberately ignored: the caller re-derives PTS by
    /// accumulating ADTS frame durations and re-anchoring on drift against the
    /// camera's video clock, which is the contract every other audio path in
    /// this module already follows and the only one that survives a camera
    /// reconnect rebasing the video stamps.
    pub(super) fn push(&mut self, pcm: &[u8]) -> Result<Vec<Vec<u8>>> {
        if pcm.is_empty() {
            return Ok(Vec::new());
        }
        let samples = (pcm.len() / 2) as u64;
        let mut buffer = Buffer::from_slice(pcm.to_vec());
        {
            let buffer = buffer.get_mut().expect("fresh buffer is uniquely owned");
            buffer.set_pts(self.samples_to_time(self.pushed_samples));
            buffer.set_duration(self.samples_to_time(samples));
        }
        self.pushed_samples += samples;
        self.appsrc
            .push_buffer(buffer)
            .map_err(|e| anyhow!("AAC appsrc rejected a buffer: {e:?}"))?;
        self.check_bus()?;
        Ok(self.drain())
    }

    /// Convert a sample count into pipeline time.
    fn samples_to_time(&self, samples: u64) -> ClockTime {
        ClockTime::from_nseconds(samples.saturating_mul(1_000_000_000) / u64::from(self.rate))
    }

    /// Pull the sink dry, waiting only for access units the encoder is
    /// already late with.
    ///
    /// "Late" is `pushed / 1024 - encoder delay - already emitted`. While that
    /// is positive the pull blocks for up to [`DRAIN_TIMEOUT_MS`]; once it hits
    /// zero the pull is non-blocking and the loop ends on the first miss. In
    /// steady state the encoder is never behind and nothing blocks.
    fn drain(&mut self) -> Vec<Vec<u8>> {
        let mut frames = Vec::new();
        loop {
            let overdue = (self.pushed_samples / u64::from(AAC_SAMPLES_PER_FRAME))
                .saturating_sub(ENCODER_LATENCY_FRAMES)
                .saturating_sub(self.emitted_frames);
            let timeout = if overdue > 0 {
                ClockTime::from_mseconds(DRAIN_TIMEOUT_MS)
            } else {
                ClockTime::ZERO
            };
            let Some(sample) = self.appsink.try_pull_sample(timeout) else {
                break;
            };
            let Some(buffer) = sample.buffer() else {
                continue;
            };
            let Ok(map) = buffer.map_readable() else {
                warn!("Could not map an encoded AAC buffer");
                continue;
            };
            let before = frames.len();
            split_adts(map.as_slice(), &mut frames);
            self.emitted_frames += (frames.len() - before) as u64;
        }
        frames
    }

    /// Surface a pipeline error rather than silently producing nothing.
    fn check_bus(&self) -> Result<()> {
        let Some(bus) = self.pipeline.bus() else {
            return Ok(());
        };
        while let Some(msg) = bus.pop() {
            use gstreamer::MessageView::*;
            match msg.view() {
                Error(err) => {
                    return Err(anyhow!(
                        "AAC pipeline error from {:?}: {} ({:?})",
                        msg.src().map(|s| s.path_string()),
                        err.error(),
                        err.debug()
                    ));
                }
                Warning(w) => warn!("AAC pipeline warning: {}", w.error()),
                _ => {}
            }
        }
        Ok(())
    }
}

/// Video span, in 90 kHz ticks, the sample-rate measurement needs.
const RATE_MEASURE_90K: u64 = 90_000;
/// ...and the number of ADPCM frames it needs, so a single fat block cannot
/// decide the answer.
const RATE_MEASURE_FRAMES: usize = 4;
/// Stop waiting for a clean measurement after this much video and take the
/// fallback. Only reachable if audio is arriving but video is not.
const RATE_MEASURE_MAX_90K: u64 = 5 * 90_000;
/// What to encode at when the measurement never gathers enough data.
const RATE_FALLBACK: u32 = 16_000;

/// What one ADPCM frame produced.
#[derive(Default)]
pub(super) struct Transcoded {
    /// Set exactly once, on the frame that starts the encoder: the video PTS
    /// the *first buffered* audio sample belongs at. The caller anchors its
    /// audio clock there so the ~1 s held during rate measurement is emitted
    /// at the time it was captured rather than bunched up at "now".
    pub(super) anchor_90k: Option<u64>,
    /// Complete ADTS access units, in order.
    pub(super) frames: Vec<Vec<u8>>,
}

/// ADPCM in, ADTS AAC out, with the input sample rate measured rather than
/// assumed.
///
/// BC audio frames carry no timestamp and the protocol does not state a
/// sample rate anywhere this code can read (`BcMediaAdpcm::duration()` in the
/// core crate hardcodes 8 kHz; `rtsp/factory.rs` hardcodes 8 kHz in its caps;
/// the `TalkAbility` XML documents 16 kHz for the *speaker* direction). Rather
/// than pick one and be silently half- or double-speed, the first second of
/// audio is held, counted against the camera's own video clock, and the ratio
/// snapped to the nearest rate `voaacenc` accepts. `--audio-rate` skips it.
pub(super) struct AdpcmTranscoder {
    encoder: Option<AacEncoder>,
    forced_rate: Option<u32>,
    /// Decoded PCM held while the rate is being measured.
    pending: Vec<u8>,
    pending_frames: usize,
    first_video_90k: Option<u64>,
    /// Set after a fatal encoder error; audio stops, video does not.
    failed: bool,
    decode_errors: u64,
}

impl AdpcmTranscoder {
    pub(super) fn new(forced_rate: Option<u32>) -> Self {
        Self {
            encoder: None,
            forced_rate,
            pending: Vec::new(),
            pending_frames: 0,
            first_video_90k: None,
            failed: false,
            decode_errors: 0,
        }
    }

    /// Decode one BC ADPCM frame and return whatever AAC it produced.
    ///
    /// Never fails the stream: a decode error is counted and skipped, and an
    /// encoder error disables audio for the life of the process. Losing audio
    /// is strictly better than exiting and making the supervisor respawn a
    /// camera whose video was fine.
    pub(super) fn feed(&mut self, data: &[u8], video_90k: u64) -> Transcoded {
        let mut out = Transcoded::default();
        if self.failed {
            return out;
        }
        let pcm = match adpcm_to_pcm(data) {
            Ok(pcm) => pcm,
            Err(e) => {
                self.decode_errors += 1;
                if self.decode_errors == 1 || self.decode_errors % 100 == 0 {
                    warn!(
                        "ADPCM decode failed ({} so far), skipping the frame: {e:#}",
                        self.decode_errors
                    );
                }
                return out;
            }
        };

        if self.encoder.is_some() {
            self.push_into(&pcm, &mut out);
            return out;
        }

        let first = *self.first_video_90k.get_or_insert(video_90k);
        self.pending.extend_from_slice(&pcm);
        self.pending_frames += 1;
        let span = video_90k.saturating_sub(first);
        let samples = (self.pending.len() / 2) as u64;

        let rate = match self.forced_rate {
            Some(rate) => Some(rate),
            None if span >= RATE_MEASURE_90K && self.pending_frames >= RATE_MEASURE_FRAMES => {
                let measured = samples as f64 * 90_000.0 / span as f64;
                let snapped = snap_rate(measured);
                info!(
                    "ADPCM sample rate measured {measured:.0} Hz \
                     ({samples} samples over {:.2} s of video, {} frames) -> encoding at {snapped} Hz",
                    span as f64 / 90_000.0,
                    self.pending_frames
                );
                Some(snapped)
            }
            None if span >= RATE_MEASURE_MAX_90K => {
                warn!(
                    "Could not measure the ADPCM sample rate in {:.1} s of video \
                     ({} frames); assuming {RATE_FALLBACK} Hz",
                    span as f64 / 90_000.0,
                    self.pending_frames
                );
                Some(RATE_FALLBACK)
            }
            None => None,
        };

        let Some(rate) = rate else {
            return out;
        };
        match AacEncoder::new(rate) {
            Ok(encoder) => {
                self.encoder = Some(encoder);
                out.anchor_90k = Some(first);
            }
            Err(e) => {
                error!("Could not start the AAC encoder, audio is off: {e:#}");
                self.failed = true;
                self.pending = Vec::new();
                return out;
            }
        }
        let buffered = std::mem::take(&mut self.pending);
        self.push_into(&buffered, &mut out);
        out
    }

    fn push_into(&mut self, pcm: &[u8], out: &mut Transcoded) {
        let Some(encoder) = self.encoder.as_mut() else {
            return;
        };
        match encoder.push(pcm) {
            Ok(frames) => out.frames.extend(frames),
            Err(e) => {
                error!("AAC encoder failed, audio stops here: {e:#}");
                self.failed = true;
                self.encoder = None;
            }
        }
    }
}

/// Initialise GStreamer and check the encoder can actually be built here.
///
/// Called once at startup so the "audio is transcoded" decision — and
/// therefore the PMT — is made before any frame is written.
pub(super) fn probe() -> Result<()> {
    gstreamer::init().context("Could not initialise GStreamer")?;
    let missing = AacEncoder::missing_elements();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(
            "GStreamer is missing {missing:?}; set GST_PLUGIN_PATH to a directory \
             holding libgstapp.so, libgstaudioconvert.so, libgstaudioresample.so, \
             libgstaudioparsers.so, libgstvoaacenc.so and libgstcoreelements.so"
        ))
    }
}

impl Drop for AacEncoder {
    fn drop(&mut self) {
        let _ = self.appsrc.end_of_stream();
        let _ = self.pipeline.set_state(State::Null);
    }
}

/// Create a plain element, naming the plugin that provides it on failure.
fn make(kind: &str) -> Result<gstreamer::Element> {
    ElementFactory::make(kind).build().with_context(|| {
        let plugin = match kind {
            "appsrc" | "appsink" => "app (gst-plugins-base)",
            "audioconvert" | "audioresample" => "gst-plugins-base",
            "capsfilter" => "coreelements (gstreamer)",
            "voaacenc" => "voaacenc (gst-plugins-bad)",
            "aacparse" => "audioparsers (gst-plugins-good)",
            _ => "unknown plugin",
        };
        format!("Could not create `{kind}`; it comes from {plugin}")
    })
}

/// Length of the ADTS frame starting at `data[0]`, header included.
///
/// `None` when the syncword or the length field is not there, which is the
/// check that stops a malformed encoder output from being muxed as if it were
/// an access unit.
pub(super) fn adts_frame_len(data: &[u8]) -> Option<usize> {
    if data.len() < 7 {
        return None;
    }
    if data[0] != 0xFF || (data[1] & 0xF0) != 0xF0 {
        return None;
    }
    let len = (usize::from(data[3] & 0x03) << 11)
        | (usize::from(data[4]) << 3)
        | (usize::from(data[5]) >> 5);
    // A frame must at least contain its own header.
    let header = if data[1] & 0x01 == 0 { 9 } else { 7 };
    if len < header || len > data.len() {
        return None;
    }
    Some(len)
}

/// Duration of one ADTS frame in microseconds.
///
/// Same arithmetic as `BcMediaAac::duration()` in the core crate, which is
/// what the AAC-camera path feeds the clock, so transcoded and passthrough
/// audio advance the same PTS the same way.
pub(super) fn adts_duration_micros(data: &[u8]) -> Option<u32> {
    if adts_frame_len(data).is_none() {
        return None;
    }
    let frequency_index = (data[2] & 0b0011_1100) >> 2;
    let sample_frequency = match frequency_index {
        0 => 96000u32,
        1 => 88200,
        2 => 64000,
        3 => 48000,
        4 => 44100,
        5 => 32000,
        6 => 24000,
        7 => 22050,
        8 => 16000,
        9 => 12000,
        10 => 11025,
        11 => 8000,
        12 => 7350,
        _ => return None,
    };
    let frames = u32::from(data[6] & 0b0000_0011) + 1;
    let samples = frames * AAC_SAMPLES_PER_FRAME;
    Some(samples * 1_000_000 / sample_frequency)
}

/// Split a buffer of concatenated ADTS frames into one `Vec` per access unit.
///
/// `aacparse` normally hands out exactly one frame per buffer, but nothing in
/// the API promises it, and the muxer wants one PES per access unit.
fn split_adts(data: &[u8], out: &mut Vec<Vec<u8>>) {
    let mut offset = 0usize;
    while offset < data.len() {
        match adts_frame_len(&data[offset..]) {
            Some(len) => {
                out.push(data[offset..offset + len].to_vec());
                offset += len;
            }
            None => {
                warn!(
                    "Discarding {} bytes of encoder output with no ADTS syncword",
                    data.len() - offset
                );
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal well-formed ADTS header for `payload` bytes of AAC-LC.
    fn adts(rate_index: u8, payload: usize) -> Vec<u8> {
        let len = 7 + payload;
        let mut h = vec![
            0xFF,
            0xF1, // MPEG-4, layer 0, no CRC (7 byte header)
            0x40 | (rate_index << 2), // profile LC, sampling index
            0x40 | ((len >> 11) & 0x03) as u8, // channel cfg 1 + length hi
            ((len >> 3) & 0xFF) as u8,
            (((len & 0x07) << 5) | 0x1F) as u8,
            0xFC, // buffer fullness low + 1 frame per packet
        ];
        h.resize(len, 0x00);
        h
    }

    #[test]
    fn adts_length_and_duration_are_read_back() {
        let frame = adts(8, 200); // index 8 = 16 kHz
        assert_eq!(adts_frame_len(&frame), Some(207));
        // 1024 samples at 16 kHz = 64000 us.
        assert_eq!(adts_duration_micros(&frame), Some(64_000));

        let frame = adts(11, 100); // index 11 = 8 kHz
        assert_eq!(adts_duration_micros(&frame), Some(128_000));
    }

    #[test]
    fn a_frame_without_a_syncword_is_rejected() {
        let mut frame = adts(8, 32);
        frame[0] = 0x00;
        assert_eq!(adts_frame_len(&frame), None);
        assert_eq!(adts_duration_micros(&frame), None);
        // Truncated below its declared length.
        let mut frame = adts(8, 32);
        frame.truncate(10);
        assert_eq!(adts_frame_len(&frame), None);
    }

    #[test]
    fn concatenated_frames_split_one_per_access_unit() {
        let mut blob = Vec::new();
        for payload in [10usize, 40, 7] {
            blob.extend_from_slice(&adts(8, payload));
        }
        let mut frames = Vec::new();
        split_adts(&blob, &mut frames);
        assert_eq!(frames.len(), 3);
        assert_eq!(
            frames.iter().map(|f| f.len()).collect::<Vec<_>>(),
            vec![17, 47, 14]
        );
        for frame in &frames {
            assert_eq!(adts_frame_len(frame), Some(frame.len()));
        }
    }

    #[test]
    fn rates_snap_to_what_the_encoder_accepts() {
        assert_eq!(snap_rate(15_900.0), 16000);
        assert_eq!(snap_rate(8_050.0), 8000);
        assert_eq!(snap_rate(7_900.0), 8000);
        assert_eq!(snap_rate(44_000.0), 44100);
    }

    #[test]
    fn bitrate_stays_inside_what_voaacenc_accepts() {
        for &rate in SUPPORTED_RATES {
            let bitrate = f64::from(bitrate_for(rate));
            let rate = f64::from(rate);
            assert!(
                bitrate >= rate * 0.5 && bitrate <= rate * 6.0,
                "{} Hz -> {} bps is outside voaacenc's range",
                rate,
                bitrate
            );
        }
    }

    /// End-to-end: real ADPCM blocks in, real ADTS frames out.
    ///
    /// Skipped when the GStreamer plugins are not installed, so `cargo test`
    /// on a bare host stays green; the `test` stage of
    /// `Dockerfile.stream-bookworm` builds on an image that has them.
    #[test]
    fn adpcm_blocks_encode_to_valid_adts_frames() {
        gstreamer::init().expect("gstreamer init");
        let missing = AacEncoder::missing_elements();
        if !missing.is_empty() {
            eprintln!("skipping: missing GStreamer elements {:?}", missing);
            return;
        }
        let mut encoder = AacEncoder::new(16000).expect("encoder");
        let mut frames = Vec::new();
        // 40 x 508-byte blocks = 40 x 1016 samples = ~2.5 s at 16 kHz.
        for i in 0..40u8 {
            let block = super::super::adpcm::test_block(508, 0x35u8.wrapping_add(i));
            let pcm = adpcm_to_pcm(&block).expect("decode");
            assert_eq!(pcm.len(), 508 * 2 * 2);
            frames.extend(encoder.push(&pcm).expect("push"));
        }
        // 40 x 1016 samples = 40640 = 39.7 AAC frames, minus the two frames
        // of encoder delay the drain deliberately does not wait for. A drain
        // that only pulls what is already queued returns 0 here in a release
        // build, which is the regression this number exists to catch.
        assert!(
            frames.len() >= 37,
            "expected ~39 AAC frames, got {}",
            frames.len()
        );
        for frame in &frames {
            assert_eq!(
                adts_frame_len(frame),
                Some(frame.len()),
                "frame is not a single well-formed ADTS access unit"
            );
            assert_eq!(adts_duration_micros(frame), Some(64_000));
        }
    }
}
