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
    /// Exit non-zero after this many seconds without a frame from the camera
    ///
    /// This is the whole watchdog: the supervising process is expected to
    /// notice the exit and start a fresh instance
    #[arg(long, default_value_t = 30)]
    pub stale_timeout: u64,
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
