//! Stand-in for [`super::aac`] in builds without the `gstreamer` feature.
//!
//! `neolink stream` itself needs no GStreamer — `mpegts.rs` is hand-rolled —
//! so the subcommand stays available in a `--no-default-features` build (which
//! upstream CI clippy-checks). Only the ADPCM -> AAC transcode needs an
//! encoder, and here there is none: [`probe`] fails, the track layout comes
//! out video-only, and the ADPCM frames are dropped exactly as they were
//! before transcoding existed.
//!
//! Selected by `#[cfg_attr(not(feature = "gstreamer"), path = "aac_nogst.rs")]`
//! in `mod.rs`, so this file must keep the same item names as the real one.

use anyhow::{anyhow, Result};

/// What one ADPCM frame produced. Always empty here.
#[derive(Default)]
pub(super) struct Transcoded {
    pub(super) anchor_90k: Option<u64>,
    pub(super) frames: Vec<Vec<u8>>,
}

/// Never constructed: [`probe`] fails first, so the caller never asks for one.
pub(super) struct AdpcmTranscoder {
    _forced_rate: Option<u32>,
}

impl AdpcmTranscoder {
    pub(super) fn new(forced_rate: Option<u32>) -> Self {
        Self {
            _forced_rate: forced_rate,
        }
    }

    pub(super) fn feed(&mut self, _data: &[u8], _video_90k: u64) -> Transcoded {
        Transcoded::default()
    }
}

/// Duration of one ADTS frame in microseconds; unreachable without an encoder,
/// but kept so `mod.rs` needs no `cfg` of its own.
pub(super) fn adts_duration_micros(_data: &[u8]) -> Option<u32> {
    None
}

pub(super) fn probe() -> Result<()> {
    Err(anyhow!(
        "this binary was built without the `gstreamer` feature, so it has no AAC encoder"
    ))
}
