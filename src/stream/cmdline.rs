use clap::{Parser, ValueEnum};
use neolink_core::bc_protocol::StreamKind;

/// Stream one camera to stdout as a byte pipe, with no RTSP server.
///
/// Intended to be spawned as a child process by a consumer that owns the
/// pipe's lifetime, for example go2rtc's `exec:` producer:
///
/// ```yaml
/// streams:
///   alley: exec:neolink stream --config /etc/neolink.toml camera_d
/// ```
///
/// The process writes media to stdout and everything else to stderr, and
/// exits non-zero if the camera stops delivering frames, so the supervisor's
/// respawn is the only watchdog needed.
#[derive(Parser, Debug)]
pub struct Opt {
    /// The name of the camera to stream. Must be a name in the config
    pub camera: String,
    /// Which of the camera's streams to pull
    #[arg(long, value_enum, default_value_t = StreamSelect::Main)]
    pub stream: StreamSelect,
    /// Wire format written to stdout
    #[arg(long, value_enum, default_value_t = Format::Ts)]
    pub format: Format,
    /// What to do with the camera's audio track
    ///
    /// Reolink BC delivers either AAC, which MPEG-TS carries verbatim, or
    /// DVI4/IMA ADPCM, which has no MPEG-TS stream type and is therefore
    /// decoded and re-encoded to AAC in-process. `none` turns audio off
    /// entirely and emits a video-only PMT
    #[arg(long, value_enum, default_value_t = AudioMode::Aac)]
    pub audio: AudioMode,
    /// Sample rate of the camera's ADPCM, in Hz, instead of measuring it
    ///
    /// BC audio frames carry no timestamp and no rate, so by default the rate
    /// is inferred from how many samples arrive per second of video. Set this
    /// if that measurement is ever wrong; audio at half or double speed is
    /// what a wrong value sounds like
    #[arg(long)]
    pub audio_rate: Option<u32>,
    /// Exit non-zero after this many seconds without a frame from the camera
    ///
    /// This is the whole watchdog: the supervising process is expected to
    /// notice the exit and start a fresh instance
    #[arg(long, default_value_t = 30)]
    pub stale_timeout: u64,
}

/// What to do with the camera's audio.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum AudioMode {
    /// No audio track at all
    None,
    /// AAC: carried verbatim from an AAC camera, transcoded from ADPCM
    /// otherwise
    Aac,
}

/// Which camera stream to subscribe to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum StreamSelect {
    /// The HD stream
    Main,
    /// The SD stream
    Sub,
    /// The balanced stream, where the camera offers one
    Extern,
}

impl From<StreamSelect> for StreamKind {
    fn from(value: StreamSelect) -> Self {
        match value {
            StreamSelect::Main => StreamKind::Main,
            StreamSelect::Sub => StreamKind::Sub,
            StreamSelect::Extern => StreamKind::Extern,
        }
    }
}

/// Wire format written to stdout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Format {
    /// MPEG-TS carrying video, a 90 kHz PTS, and AAC audio when the camera
    /// sends it
    Ts,
    /// The raw Annex-B video elementary stream, with no audio and no
    /// timestamps
    H26x,
}
