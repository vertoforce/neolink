//! A minimal MPEG-TS muxer for the single-consumer pipe.
//!
//! This is deliberately the smallest transport stream that a demuxer
//! (ffmpeg/go2rtc) will accept: a PAT, a PMT, and one PES stream per elementary
//! track, with a 90 kHz PTS on every access unit and a PCR carried in the
//! adaptation field of the first packet of each video PES.
//!
//! It is *not* a general purpose muxer. The simplifications are only safe
//! because the output goes down a local pipe to exactly one process:
//!
//! * There is no PCR interpolation. A PCR is emitted per video access unit
//!   (~15-30 Hz), which is well inside the 100 ms maximum PCR interval, so no
//!   separate PCR insertion pass is needed.
//! * The PSI is re-emitted on a timer rather than at a bit-rate derived
//!   interval; the consumer reads from byte zero so it only ever needs the
//!   first copy.
//! * `PES_packet_length` is left as 0 (unbounded) for video, which is legal
//!   for video streams in a transport stream and avoids having to split
//!   access units larger than 65535 bytes.
//!
//! Bytes are appended to a caller-owned `Vec<u8>` so the caller controls when
//! a syscall happens (one write per access unit).

use neolink_core::bcmedia::model::VideoType;
use std::convert::TryFrom;

/// PID carrying the Program Map Table.
const PMT_PID: u16 = 0x1000;
/// PID carrying the video elementary stream.
const VIDEO_PID: u16 = 0x0100;
/// PID carrying the audio elementary stream.
const AUDIO_PID: u16 = 0x0101;

/// `stream_type` for H.264 video (ISO/IEC 14496-10).
const STREAM_TYPE_H264: u8 = 0x1B;
/// `stream_type` for H.265 video (ISO/IEC 23008-2).
const STREAM_TYPE_H265: u8 = 0x24;
/// `stream_type` for AAC in ADTS framing (ISO/IEC 13818-7).
const STREAM_TYPE_AAC: u8 = 0x0F;

/// PES `stream_id` for the first video stream.
const STREAM_ID_VIDEO: u8 = 0xE0;
/// PES `stream_id` for the first audio stream.
const STREAM_ID_AUDIO: u8 = 0xC0;

/// Size of a transport stream packet.
const TS_PACKET_LEN: usize = 188;

/// Which audio codec, if any, is being carried.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AudioKind {
    /// AAC with ADTS headers, carried verbatim.
    Aac,
}

/// Incremental MPEG-TS writer.
pub(super) struct TsMuxer {
    video: VideoType,
    audio: Option<AudioKind>,
    video_cc: u8,
    audio_cc: u8,
    pat_cc: u8,
    pmt_cc: u8,
    /// PSI is repeated every this many video access units.
    psi_interval: u32,
    since_psi: u32,
}

impl TsMuxer {
    /// Build a muxer for a known video codec and optional audio codec.
    ///
    /// The codecs are fixed for the lifetime of the muxer: if the camera ever
    /// changes codec mid-stream the caller is expected to exit and let the
    /// supervisor respawn, which is cheaper than renegotiating a PMT that the
    /// consumer may not re-read.
    pub(super) fn new(video: VideoType, audio: Option<AudioKind>) -> Self {
        Self {
            video,
            audio,
            video_cc: 0,
            audio_cc: 0,
            pat_cc: 0,
            pmt_cc: 0,
            // ~2 s at 15 fps. Cheap insurance for a consumer that attaches
            // late (it should not, but a stray restart costs 376 bytes).
            psi_interval: 30,
            since_psi: u32::MAX,
        }
    }

    /// True when this muxer was built with an audio track in its PMT.
    pub(super) fn has_audio(&self) -> bool {
        self.audio.is_some()
    }

    /// Append a video access unit (Annex-B, with in-band parameter sets on
    /// key frames) at `pts_90k`.
    pub(super) fn write_video(&mut self, out: &mut Vec<u8>, data: &[u8], pts_90k: u64, key: bool) {
        if self.since_psi >= self.psi_interval {
            self.write_psi(out);
            self.since_psi = 0;
        }
        self.since_psi = self.since_psi.saturating_add(1);

        let pes = build_pes(STREAM_ID_VIDEO, pts_90k, data, true);
        // The PCR is deliberately identical to the PTS rather than PTS minus a
        // buffering delay: the consumer is a local demuxer that plays as fast
        // as bytes arrive, so there is no clock to slave and no reason to
        // introduce latency.
        let cc = &mut self.video_cc;
        write_pes_packets(out, VIDEO_PID, cc, &pes, Some(pts_90k), key);
    }

    /// Append an audio access unit at `pts_90k`. No-op when the muxer was
    /// built without audio.
    pub(super) fn write_audio(&mut self, out: &mut Vec<u8>, data: &[u8], pts_90k: u64) {
        if self.audio.is_none() {
            return;
        }
        let pes = build_pes(STREAM_ID_AUDIO, pts_90k, data, false);
        let cc = &mut self.audio_cc;
        write_pes_packets(out, AUDIO_PID, cc, &pes, None, false);
    }

    /// Append a PAT followed by a PMT.
    pub(super) fn write_psi(&mut self, out: &mut Vec<u8>) {
        let mut pat = Vec::with_capacity(16);
        pat.push(0x00); // table_id: program_association_section
        push_section_len(&mut pat, 13);
        pat.extend_from_slice(&[0x00, 0x01]); // transport_stream_id
        pat.extend_from_slice(&[0xC1, 0x00, 0x00]); // version 0, current, section 0/0
        pat.extend_from_slice(&[0x00, 0x01]); // program_number 1
        pat.extend_from_slice(&[0xE0 | (PMT_PID >> 8) as u8, (PMT_PID & 0xFF) as u8]);
        push_crc32(&mut pat);
        write_psi_packet(out, 0x0000, &mut self.pat_cc, &pat);

        let video_type = match self.video {
            VideoType::H264 => STREAM_TYPE_H264,
            VideoType::H265 => STREAM_TYPE_H265,
        };
        let stream_count = 1 + u16::from(self.audio.is_some());
        let mut pmt = Vec::with_capacity(32);
        pmt.push(0x02); // table_id: program_map_section
        push_section_len(&mut pmt, 9 + 5 * stream_count + 4);
        pmt.extend_from_slice(&[0x00, 0x01]); // program_number 1
        pmt.extend_from_slice(&[0xC1, 0x00, 0x00]); // version 0, current, section 0/0
        pmt.extend_from_slice(&[0xE0 | (VIDEO_PID >> 8) as u8, (VIDEO_PID & 0xFF) as u8]); // PCR_PID
        pmt.extend_from_slice(&[0xF0, 0x00]); // program_info_length 0
        push_pmt_stream(&mut pmt, video_type, VIDEO_PID);
        match self.audio {
            Some(AudioKind::Aac) => push_pmt_stream(&mut pmt, STREAM_TYPE_AAC, AUDIO_PID),
            None => {}
        }
        push_crc32(&mut pmt);
        write_psi_packet(out, PMT_PID, &mut self.pmt_cc, &pmt);
    }
}

/// Push the `section_syntax_indicator`/`section_length` pair of a PSI section.
fn push_section_len(section: &mut Vec<u8>, len: u16) {
    section.push(0xB0 | ((len >> 8) & 0x0F) as u8);
    section.push((len & 0xFF) as u8);
}

/// Push one elementary stream entry of a PMT.
fn push_pmt_stream(pmt: &mut Vec<u8>, stream_type: u8, pid: u16) {
    pmt.push(stream_type);
    pmt.push(0xE0 | (pid >> 8) as u8);
    pmt.push((pid & 0xFF) as u8);
    pmt.extend_from_slice(&[0xF0, 0x00]); // ES_info_length 0
}

/// Append the MPEG-2 section CRC of everything written so far.
fn push_crc32(section: &mut Vec<u8>) {
    let crc = mpeg_crc32(section);
    section.extend_from_slice(&crc.to_be_bytes());
}

/// MPEG-2 systems CRC-32: polynomial 0x04C11DB7, seed all-ones, no reflection
/// and no final inversion.
fn mpeg_crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for byte in data {
        crc ^= u32::from(*byte) << 24;
        for _ in 0..8 {
            crc = if crc & 0x8000_0000 != 0 {
                (crc << 1) ^ 0x04C1_1DB7
            } else {
                crc << 1
            };
        }
    }
    crc
}

/// Emit a complete PSI section as a single TS packet.
///
/// Every section this module produces is far below 183 bytes, so the
/// multi-packet section case cannot arise and is not implemented.
fn write_psi_packet(out: &mut Vec<u8>, pid: u16, cc: &mut u8, section: &[u8]) {
    debug_assert!(section.len() <= TS_PACKET_LEN - 5);
    let start = out.len();
    out.push(0x47);
    out.push(0x40 | ((pid >> 8) & 0x1F) as u8); // payload_unit_start_indicator
    out.push((pid & 0xFF) as u8);
    out.push(0x10 | (*cc & 0x0F)); // payload only
    *cc = cc.wrapping_add(1) & 0x0F;
    out.push(0x00); // pointer_field
    out.extend_from_slice(section);
    out.resize(start + TS_PACKET_LEN, 0xFF);
}

/// Build a PES packet (header + payload) for one access unit.
fn build_pes(stream_id: u8, pts_90k: u64, payload: &[u8], unbounded: bool) -> Vec<u8> {
    let mut pes = Vec::with_capacity(payload.len() + 14);
    pes.extend_from_slice(&[0x00, 0x00, 0x01, stream_id]);
    // PES_packet_length counts everything after this field. Video access units
    // routinely exceed 65535 bytes, and 0 ("unbounded, ends at the next start
    // code") is only permitted for video, so audio always carries a real
    // length.
    let len: u16 = if unbounded {
        0
    } else {
        u16::try_from(payload.len() + 8).unwrap_or(0)
    };
    pes.extend_from_slice(&len.to_be_bytes());
    pes.push(0x80); // '10' marker, not scrambled, no priority
    pes.push(0x80); // PTS only
    pes.push(0x05); // PES_header_data_length
    push_timestamp(&mut pes, 0b0010, pts_90k);
    pes.extend_from_slice(payload);
    pes
}

/// Push a 5-byte PES timestamp with the given 4-bit prefix.
fn push_timestamp(pes: &mut Vec<u8>, prefix: u8, ts: u64) {
    let ts = ts & 0x1_FFFF_FFFF; // 33 bits
    pes.push((prefix << 4) | (((ts >> 30) & 0x07) as u8) << 1 | 0x01);
    pes.push(((ts >> 22) & 0xFF) as u8);
    pes.push((((ts >> 15) & 0x7F) as u8) << 1 | 0x01);
    pes.push(((ts >> 7) & 0xFF) as u8);
    pes.push(((ts & 0x7F) as u8) << 1 | 0x01);
}

/// Encode a 42-bit PCR (33-bit 90 kHz base + 9-bit 27 MHz extension).
fn encode_pcr(pts_90k: u64) -> [u8; 6] {
    let base = pts_90k & 0x1_FFFF_FFFF;
    [
        ((base >> 25) & 0xFF) as u8,
        ((base >> 17) & 0xFF) as u8,
        ((base >> 9) & 0xFF) as u8,
        ((base >> 1) & 0xFF) as u8,
        (((base & 1) as u8) << 7) | 0x7E, // 6 reserved bits set, extension high bit 0
        0x00,                             // extension low bits
    ]
}

/// Split a PES packet across 188-byte TS packets, stuffing the last one via an
/// adaptation field.
fn write_pes_packets(
    out: &mut Vec<u8>,
    pid: u16,
    cc: &mut u8,
    pes: &[u8],
    pcr: Option<u64>,
    random_access: bool,
) {
    let mut offset = 0usize;
    let mut first = true;
    while offset < pes.len() {
        let remaining = pes.len() - offset;

        // Adaptation field body, i.e. everything after the length byte.
        let mut af: Vec<u8> = Vec::new();
        if first {
            let mut flags = 0u8;
            if random_access {
                flags |= 0x40;
            }
            if pcr.is_some() {
                flags |= 0x10;
            }
            if flags != 0 {
                af.push(flags);
                if let Some(pcr) = pcr {
                    af.extend_from_slice(&encode_pcr(pcr));
                }
            }
        }

        // Header is 4 bytes, plus the adaptation field length byte and body.
        let mut header_len = 4 + if af.is_empty() { 0 } else { 1 + af.len() };
        let mut payload_len = TS_PACKET_LEN - header_len;
        let mut af_len_byte: Option<u8> = if af.is_empty() {
            None
        } else {
            Some(af.len() as u8)
        };

        if remaining < payload_len {
            // Short tail: grow (or create) the adaptation field so the packet
            // still comes out at exactly 188 bytes.
            let stuff = payload_len - remaining;
            match af_len_byte {
                Some(len) => {
                    af_len_byte = Some(len + stuff as u8);
                    af.resize(af.len() + stuff, 0xFF);
                }
                None if stuff == 1 => {
                    // The length byte alone consumes the spare byte.
                    af_len_byte = Some(0);
                }
                None => {
                    af_len_byte = Some((stuff - 1) as u8);
                    af.push(0x00); // no flags
                    af.resize(stuff - 1, 0xFF);
                }
            }
            header_len = 4 + 1 + af.len();
            payload_len = TS_PACKET_LEN - header_len;
        }

        out.push(0x47);
        out.push(if first { 0x40 } else { 0x00 } | ((pid >> 8) & 0x1F) as u8);
        out.push((pid & 0xFF) as u8);
        let afc = if af_len_byte.is_some() { 0x30 } else { 0x10 };
        out.push(afc | (*cc & 0x0F));
        *cc = cc.wrapping_add(1) & 0x0F;
        if let Some(len) = af_len_byte {
            out.push(len);
            out.extend_from_slice(&af);
        }
        out.extend_from_slice(&pes[offset..offset + payload_len]);

        offset += payload_len;
        first = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The worked example from ISO/IEC 13818-1 tooling: ffmpeg's PAT for a
    /// single program on PID 0x1000 ends in this CRC.
    #[test]
    fn crc32_matches_known_section() {
        let section = [
            0x00u8, 0xB0, 0x0D, 0x00, 0x01, 0xC1, 0x00, 0x00, 0x00, 0x01, 0xF0, 0x00,
        ];
        assert_eq!(mpeg_crc32(&section), 0x2A_B1_04_B2);
    }

    #[test]
    fn every_packet_is_188_bytes_and_starts_with_sync() {
        let mut mux = TsMuxer::new(VideoType::H264, Some(AudioKind::Aac));
        let mut out = Vec::new();
        // A frame that lands exactly on a packet boundary, one that needs a
        // single stuffing byte, and a large one.
        for len in [1, 100, 176, 177, 178, 4096, 70000] {
            mux.write_video(&mut out, &vec![0xAAu8; len], 900_000, true);
            mux.write_audio(&mut out, &vec![0xBBu8; len.min(1000)], 900_000);
        }
        assert_eq!(out.len() % TS_PACKET_LEN, 0);
        for packet in out.chunks(TS_PACKET_LEN) {
            assert_eq!(packet[0], 0x47);
        }
    }

    #[test]
    fn continuity_counters_advance_per_pid() {
        let mut mux = TsMuxer::new(VideoType::H265, None);
        let mut out = Vec::new();
        for _ in 0..4 {
            mux.write_video(&mut out, &[0x01, 0x02, 0x03], 0, false);
        }
        let video_ccs: Vec<u8> = out
            .chunks(TS_PACKET_LEN)
            .filter(|p| u16::from_be_bytes([p[1] & 0x1F, p[2]]) == VIDEO_PID)
            .map(|p| p[3] & 0x0F)
            .collect();
        assert_eq!(video_ccs, vec![0, 1, 2, 3]);
    }

    #[test]
    fn pts_round_trips_through_the_pes_header() {
        let pts = 0x1_2345_6789u64;
        let pes = build_pes(STREAM_ID_VIDEO, pts, &[0u8; 4], true);
        let b = &pes[9..14];
        let decoded = (u64::from(b[0] & 0x0E) << 29)
            | (u64::from(b[1]) << 22)
            | (u64::from(b[2] & 0xFE) << 14)
            | (u64::from(b[3]) << 7)
            | (u64::from(b[4] & 0xFE) >> 1);
        assert_eq!(decoded, pts);
    }
}
