//! DVI4/IMA ADPCM decoder for the pipe path.
//!
//! Provenance: this file is upstream neolink's `src/rtsp/adpcm.rs`, moved here
//! by `git mv` on 2026-09-10. It was **dead code where it sat**:
//! `src/rtsp/mod.rs` never declared `mod adpcm;`, and the `super::errors::Error`
//! it returned names a `src/rtsp/errors.rs` that does not exist in this tree,
//! so it could not have compiled. `neolink rtsp` decodes ADPCM with
//! GStreamer's `adpcmdec` inside `pipe_adpcm()` and is unaffected by the move.
//!
//! ## What changed, and why it had to
//!
//! The nibble arithmetic is untouched. The framing was wrong for this caller
//! and is rewritten:
//!
//! * It expected the **whole BC payload**, starting with the 4-byte sub-header
//!   `00 01 <half_block_size:u16>`. `crates/core/.../de.rs::bcmedia_adpcm`
//!   consumes those four bytes and hands out only what follows, so
//!   `BcMediaAdpcm::data` begins at the predictor state and the old magic check
//!   would have rejected every real frame. Verified against the checked-in
//!   capture `crates/core/src/bcmedia/samples/adpcm_0.raw`: payload_size 248,
//!   sub-header `00 01 7a 00`, `data` = 244 bytes = 4 predictor + 240 nibble
//!   pairs.
//! * `half_block_size` is not usable as a length. That same capture declares
//!   `0x7a` = 122, i.e. 244/2 (the whole payload halved), while `ser.rs` writes
//!   `(len - 4)/2` = 120 — de.rs already carries the comment "on some camera
//!   this value is just 2". The block length now comes from the slice.
//! * The block-length sanity check was `!bytes.len() % full_block_size == 0`,
//!   which parses as `(!len) % n == 0` and rejected nothing, and divided by a
//!   `full_block_size` it never checked for zero.
//!
//! Input is therefore one `BcMediaAdpcm::data`: a DVI4 block whose first two
//! bytes are the previous output sample (LE `i16`) and whose next two are the
//! step index (LE `u16`), followed by two 4-bit samples per byte. Output is
//! little-endian S16 PCM, which is `audio/x-raw,format=S16LE` for the encoder
//! in [`super::aac`].

/*
 This is a rust implementation of OKI and DVI/IMA ADPCM.
*/
use anyhow::{anyhow, Result};
use std::convert::TryInto;

struct AdpcmSetup {
    max_step_index: u32,
    steps: &'static [u32],
    max_sample_size: i32,
    changes: &'static [i32],
}

impl AdpcmSetup {
    // Unused, originally we thought BC might be using OKI but it is actually DVI4
    #[allow(dead_code)]
    fn new_oki() -> Self {
        Self {
            max_step_index: 48,
            steps: &[
                16, 17, 19, 21, 23, 25, 28, 31, 34, 37, 41, 45, 50, 55, 60, 66, 73, 80, 88, 97,
                107, 118, 130, 143, 157, 173, 190, 209, 230, 253, 279, 307, 337, 371, 408, 449,
                494, 544, 598, 658, 724, 796, 876, 963, 1060, 1166, 1282, 1411, 1552,
            ],
            changes: &[-1, -1, -1, -1, 2, 4, 6, 8, -1, -1, -1, -1, 2, 4, 6, 8],
            max_sample_size: 2048,
        }
    }

    // This is IMA format, but it is the same as DVI4 format except in the block header
    fn new_ima() -> Self {
        Self {
            max_step_index: 88,
            steps: &[
                7, 8, 9, 10, 11, 12, 13, 14, 16, 17, 19, 21, 23, 25, 28, 31, 34, 37, 41, 45, 50,
                55, 60, 66, 73, 80, 88, 97, 107, 118, 130, 143, 157, 173, 190, 209, 230, 253, 279,
                307, 337, 371, 408, 449, 494, 544, 598, 658, 724, 796, 876, 963, 1060, 1166, 1282,
                1411, 1552, 1707, 1878, 2066, 2272, 2499, 2749, 3024, 3327, 3660, 4026, 4428, 4871,
                5358, 5894, 6484, 7132, 7845, 8630, 9493, 10442, 11487, 12635, 13899, 15289, 16818,
                18500, 20350, 22385, 24623, 27086, 29794, 32767,
            ],
            changes: &[-1, -1, -1, -1, 2, 4, 6, 8, -1, -1, -1, -1, 2, 4, 6, 8],
            max_sample_size: 32768,
        }
    }
}

struct Nibble {
    // A nibble is a 4bit int
    data: u8, // This is the raw data for the nibble
}

impl Nibble {
    // Use u/i32 throughout to ensure that we always have enough
    // Headroom to do the math without needing `as` casting everywhere
    fn unsigned(&self) -> u32 {
        (self.data & 0b00001111) as u32 // Mask first 4 bits it just to be sure its in nibble range
    }

    #[allow(dead_code)]
    fn signed_magnitude(&self) -> u32 {
        (self.data & 0b00000111) as u32 // Mask of first 3 bits which are the magnitiude bits in signed int
    }

    #[allow(dead_code)]
    fn signed(&self) -> i32 {
        match self.data & 0b00001000 {
            // Sign bit is at the 4th bit
            0b00001000 => -(self.signed_magnitude() as i32),
            _ => self.signed_magnitude() as i32,
        }
    }

    fn from_byte(byte: &u8) -> [Self; 2] {
        // Two nibbles per byte
        [
            Self {
                data: (byte & 0b11110000) >> 4,
            },
            Self {
                data: byte & 0b00001111,
            },
        ]
    }
}

pub(super) fn adpcm_to_pcm(bytes: &[u8]) -> Result<Vec<u8>> {
    let context = AdpcmSetup::new_ima();

    let mut result: Vec<u8> = vec![]; // Stores the PCM byte array

    // ADPCM is not really a streamable format: each sample needs the state the
    // previous one left behind. Reolink solves it by writing that state — the
    // last output sample and the step index — into the head of every block, so
    // a block can be decoded on its own. That is the DVI4 block header, and it
    // is the first four bytes of `BcMediaAdpcm::data`.
    const BLOCK_HEADER: usize = 4;
    if bytes.len() <= BLOCK_HEADER {
        return Err(anyhow!(
            "ADPCM block of {} bytes carries no samples",
            bytes.len()
        ));
    }

    {
        // The one field that can be checked: the step index has to be a valid
        // index into the step table. A frame that fails this is not a DVI4
        // block, and decoding it would emit noise at full scale.
        let mut last_output = i16::from_le_bytes(
            bytes[0..2]
                .try_into()
                .expect("slice with incorrect length"),
        ) as i32;
        let mut step_index = u16::from_le_bytes(
            bytes[2..4]
                .try_into()
                .expect("slice with incorrect length"),
        ) as i32;
        if step_index > context.max_step_index as i32 {
            return Err(anyhow!(
                "ADPCM step index {step_index} is outside 0..={}",
                context.max_step_index
            ));
        }

        // To avoid casting to u8 <-> u16 <-> u32 and back all the time I just do all maths in u/i32
        // This gives enough headroom to do all calculations without overflow because adpcm puts artifical
        // limits on the sample sizes
        let mut step: u32;

        // The rest is all data to be decoded
        let data = &bytes[BLOCK_HEADER..];

        for byte in data {
            let nibbles: [Nibble; 2] = Nibble::from_byte(byte);
            for nibble in &nibbles {
                let unibble = nibble.unsigned();

                // Specifications say: Clamp it in max index range 0..context.max_step_index
                step_index = match step_index {
                    n if n < 0 => 0,
                    n if n > context.max_step_index as i32 => context.max_step_index as i32,
                    n => n,
                };

                // This is just Eulers approximation with a variable step size
                // **Adaptive** Differential PCM
                // Adaptive: because the step size is variable
                step = context.steps[step_index as usize];

                let raw_sample;
                /* == Non approxiate version ===
                // This is the full maths version
                // We don't use this one as we need to match the way the encoder
                // works if we want to use the state stored in the header.
                // I have Left it here as it is easier to understand then the bit shift version below
                let inibble = nibble.signed();

                // Calculate the delta (which is really what adpcm is all about)
                // Adaptive **Differential** PCM
                // Differential: Becuase its all about the difference (gradient)
                let diff = (step as i32) * (inibble) / 2 + (step as i32) / 8;

                // Eulers approxiation
                // Sample = Previous_Sample + difference*step_size
                raw_sample = last_output + diff;
                */

                // === Approximate version ==
                // Approximate form uses bit shift operators.
                // This is a legacy of the days when mult/divides were expensive
                // It is also the format used on low end CPUs like cameras
                let mut diff = step >> 3;
                if (unibble & 0b0100) == 0b0100 {
                    diff += step;
                }
                if (unibble & 0b0010) == 0b0010 {
                    diff += step >> 1;
                }
                if (unibble & 0b0001) == 0b0001 {
                    diff += step >> 2;
                }
                // Sign test
                if (unibble & 0b1000) == 0b1000 {
                    raw_sample = last_output - (diff as i32);
                } else {
                    raw_sample = last_output + (diff as i32);
                }

                // Specifications say: Clamp it in max sample range -context.max_sample_size..context.max_sample_size
                let sample = match raw_sample {
                    value if value > context.max_sample_size - 1 => context.max_sample_size - 1,
                    value if value < -context.max_sample_size => -context.max_sample_size,
                    value => value,
                };

                // PCM is really in i16 range
                // Some formats e.g. OKI are not in the full PCM range of values
                // To convert we must scale it to the i16 range
                // We also cast to i16 at this point ready for the conversion to u8 bytes of the output
                let scaled_sample = (sample as i32 * (std::i16::MAX as i32)
                    / (context.max_sample_size - 1) as i32)
                    as i16;

                // Get the results in bytes
                result.extend(scaled_sample.to_le_bytes().iter());

                // Increment the step index
                step_index = step_index as i32 + context.changes[unibble as usize];

                // cache the last_output ready for next run
                last_output = sample;
            }
        }
    }
    Ok(result)
}

/// Build one BC-shaped ADPCM block: a DVI4 block header (previous output,
/// step index) followed by `data_len` bytes of nibble pairs. This is the shape
/// of `BcMediaAdpcm::data`, i.e. what the deserialiser hands us. Shared with
/// [`super::aac`]'s end-to-end test.
#[cfg(test)]
pub(super) fn test_block(data_len: usize, fill: u8) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + data_len);
    out.extend_from_slice(&0i16.to_le_bytes()); // last_output
    out.extend_from_slice(&0u16.to_le_bytes()); // step_index
    out.resize(4 + data_len, fill);
    out
}

#[cfg(test)]
mod tests {
    use super::test_block as block;
    use super::*;

    fn peak(pcm: &[u8]) -> u16 {
        pcm.chunks_exact(2)
            .map(|c| i16::from_le_bytes([c[0], c[1]]).unsigned_abs())
            .max()
            .unwrap_or(0)
    }

    #[test]
    fn decodes_two_samples_per_byte() {
        // 124 data bytes -> 248 samples -> 496 bytes of S16LE PCM.
        let pcm = adpcm_to_pcm(&block(124, 0x35)).expect("decode");
        assert_eq!(pcm.len(), 124 * 2 * 2);
    }

    #[test]
    fn a_512_byte_block_is_1024_samples() {
        // `BcMediaAdpcm::block_size()` is `data.len() - 4`, so a block of 512
        // sample bytes arrives as 516 bytes of `data`.
        let raw = block(512, 0x77);
        assert_eq!(raw.len() - 4, 512);
        assert_eq!(adpcm_to_pcm(&raw).expect("decode").len() / 2, 1024);
    }

    /// The one real ADPCM frame in the tree, decoded end to end.
    ///
    /// `crates/core/src/bcmedia/samples/adpcm_0.raw` is a whole BcMedia unit:
    /// 4 byte magic, `payload_size` twice, then the 4 byte sub-header that
    /// `de.rs` strips (`00 01` + a `half_block_size` of 0x7a, which is 244/2
    /// and not the `(len-4)/2` = 120 that `ser.rs` writes — the field is why
    /// the length is taken from the slice instead).
    #[test]
    fn the_checked_in_capture_decodes() {
        const FRAME: &[u8] = include_bytes!("../../crates/core/src/bcmedia/samples/adpcm_0.raw");
        let payload_size = usize::from(u16::from_le_bytes([FRAME[4], FRAME[5]]));
        assert_eq!(payload_size, 248);
        assert_eq!(&FRAME[8..10], &[0x00, 0x01], "sub-header magic");
        // What `de.rs` puts in `BcMediaAdpcm::data`; its own test asserts 244.
        let data = &FRAME[12..12 + payload_size - 4];
        assert_eq!(data.len(), 244);
        assert_eq!(u16::from_le_bytes([data[2], data[3]]), 16, "step index");

        let pcm = adpcm_to_pcm(data).expect("decode");
        assert_eq!(pcm.len(), (244 - 4) * 2 * 2);
        assert!(peak(&pcm) > 0, "real capture decoded to pure silence");
    }

    #[test]
    fn silence_decodes_to_a_bounded_signal() {
        // Nibble 0 is the smallest positive step, so a run of 0x00 must stay
        // near the predictor rather than diverge.
        let pcm = adpcm_to_pcm(&block(64, 0x00)).expect("decode");
        assert!(peak(&pcm) < 8000, "unexpectedly loud silence: {}", peak(&pcm));
    }

    #[test]
    fn rejects_an_impossible_step_index() {
        let mut raw = block(64, 0x11);
        raw[2] = 0xFF;
        raw[3] = 0x00; // 255, past the 88-entry IMA step table
        assert!(adpcm_to_pcm(&raw).is_err());
    }

    #[test]
    fn rejects_a_block_with_no_samples() {
        assert!(adpcm_to_pcm(&[]).is_err());
        assert!(adpcm_to_pcm(&[0x00, 0x00, 0x00, 0x00]).is_err());
    }
}
