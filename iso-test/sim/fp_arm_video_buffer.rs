// A/B arm for 2542108 (video appsrc buffer bumped to a fixed 10 MB). NO
// shim: `buffer_size` exists on both trees with the same signature, so the
// arm calls the tree's own function and reports what it returns.
//
// Pre-2542108 (1b66587) it was `max(bitrate * 2 / 8, 4096)` — for our 6 Mbps
// 4K stream that is a few KB, i.e. under one I-frame of headroom.
#[cfg(test)]
mod fp_arm_video_buffer {
    use super::*;

    #[test]
    fn the_video_appsrc_buffer_holds_seconds_not_milliseconds() {
        // A 4K I-frame at 6 Mbps / 20 fps with an x8 I-frame ratio is ~240 KB.
        const IFRAME_BYTES: u32 = 240 * 1024;
        const TEN_MB: u32 = 10 * 1024 * 1024;
        let sized = buffer_size(6144);
        eprintln!(
            "[ARM video-buffer] buffer_size(6144 kbps) = {sized} bytes = \
             {:.2} x 240KB 4K I-frames = {:.2}s at 6 Mbps",
            f64::from(sized) / f64::from(IFRAME_BYTES),
            f64::from(sized) * 8.0 / 6_000_000.0
        );
        assert_eq!(
            sized, TEN_MB,
            "the video appsrc buffer must be a fixed 10 MB"
        );
        assert!(
            sized > IFRAME_BYTES * 40,
            "the buffer must absorb dozens of 4K I-frames, not a fraction of one"
        );
    }
}
