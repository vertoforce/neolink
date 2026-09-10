//! Turning a canned Annex-B H.264 elementary stream into BcMedia frames.
//!
//! The fixture in `data/testpattern.h264` is 2 s of `testsrc` at 640x360/15fps,
//! encoded by x264 as baseline profile, one slice per frame, no B-frames, IDR
//! every 15 frames, with SPS/PPS repeated ahead of every IDR. Repeating the
//! parameter sets matters: a client that joins mid-stream (which is every
//! client here, because the fake camera loops the fixture forever) has to be
//! able to start decoding at the next IDR without out-of-band extradata.

/// One H.264 access unit: all the NAL units belonging to a single picture,
/// still in Annex-B form with start codes, exactly as a Reolink camera puts
/// them in the BcMedia payload.
#[derive(Clone)]
pub struct AccessUnit {
    /// True when this AU contains an IDR slice, i.e. it should be sent as a
    /// BcMedia IFrame rather than a PFrame.
    pub keyframe: bool,
    /// Annex-B bytes, start codes included.
    pub data: Vec<u8>,
}

/// The compiled-in test pattern.
pub const DEFAULT_FIXTURE: &[u8] = include_bytes!("../data/testpattern.h264");

/// Split an Annex-B stream into access units.
///
/// The rule used is the practical subset of ITU-T H.264 7.4.1.2.4 that covers
/// what x264 emits: a new AU starts at an access-unit delimiter (9), a
/// sequence parameter set (7), or at a VCL NAL (1 or 5) when the AU being
/// built already holds a VCL NAL. SEI (6) and PPS (8) attach to the AU that
/// follows them.
pub fn parse_annexb(data: &[u8]) -> Vec<AccessUnit> {
    let mut nals: Vec<(usize, usize)> = Vec::new(); // (start of start-code, end)
    let mut starts: Vec<usize> = Vec::new();
    let mut i = 0usize;
    while i + 3 <= data.len() {
        if data[i] == 0 && data[i + 1] == 0 && data[i + 2] == 1 {
            starts.push(i);
            i += 3;
        } else {
            i += 1;
        }
    }
    for (n, &s) in starts.iter().enumerate() {
        // A 4-byte start code is a 3-byte one with a leading zero; keep the
        // leading zero with the previous NAL, it is harmless padding.
        let end = starts.get(n + 1).copied().unwrap_or(data.len());
        nals.push((s, end));
    }

    let mut aus: Vec<AccessUnit> = Vec::new();
    let mut cur: Vec<u8> = Vec::new();
    let mut cur_has_vcl = false;
    let mut cur_is_idr = false;

    for (s, e) in nals {
        let nal_type = data[s + 3] & 0x1f;
        let is_vcl = nal_type == 1 || nal_type == 5;
        let starts_au = nal_type == 9 || nal_type == 7 || (is_vcl && cur_has_vcl);
        if starts_au && !cur.is_empty() {
            aus.push(AccessUnit {
                keyframe: cur_is_idr,
                data: std::mem::take(&mut cur),
            });
            cur_has_vcl = false;
            cur_is_idr = false;
        }
        cur.extend_from_slice(&data[s..e]);
        if is_vcl {
            cur_has_vcl = true;
        }
        if nal_type == 5 || nal_type == 7 {
            cur_is_idr = true;
        }
    }
    if !cur.is_empty() {
        aus.push(AccessUnit {
            keyframe: cur_is_idr,
            data: cur,
        });
    }
    // An AU with no picture in it is not a frame; drop it.
    aus.retain(|au| !au.data.is_empty());
    aus
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_splits_into_frames_with_leading_keyframe() {
        let aus = parse_annexb(DEFAULT_FIXTURE);
        assert!(aus.len() >= 10, "expected several frames, got {}", aus.len());
        assert!(aus[0].keyframe, "stream must open on a keyframe");
        assert!(
            aus.iter().filter(|a| a.keyframe).count() >= 2,
            "fixture should contain a repeating IDR"
        );
        // Every AU must start with a start code, or the client's H.264 parser
        // will treat the concatenation as corrupt.
        for au in aus.iter() {
            assert_eq!(&au.data[..3], &[0, 0, 1]);
        }
    }
}
