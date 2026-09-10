// A/B arm for 40fbd86 / fix 11 (video appsrc leaky-type=downstream) and
// 2542108 (10 MB video buffer). NO shim: this arm calls the tree's OWN
// pipe_h264 / pipe_h265 / pipe_aac / pipe_adpcm / buffer_size, so it
// exercises whatever that tree actually configures. Appended UNCHANGED to
// the fixed tree and to 40fbd86^ (d90ce64), where the video appsrc has no
// leaky-type and send_to_sources drops the NEWEST frame instead.
//
// Skips green (SKIP line) if the GStreamer element plugins are absent.
#[cfg(test)]
mod fp_arm_leaky_downstream {
    use super::*;

    fn ready(required: &[&str]) -> bool {
        if gstreamer::init().is_err() {
            return false;
        }
        required
            .iter()
            .all(|n| gstreamer::ElementFactory::find(n).is_some())
    }

    fn cfg() -> StreamConfig {
        StreamConfig {
            resolution: [3840, 2160],
            bitrate: 6144,
            fps: 20,
            bitrate_table: vec![6144],
            fps_table: vec![20],
            vid_type: Some(VideoType::H264),
            aud_type: Some(AudioType::Aac),
        }
    }

    fn leaky_of(src: &AppSrc) -> String {
        src.property_value("leaky-type")
            .serialize()
            .map(|s| s.to_string())
            .unwrap_or_else(|_| "<unserializable>".into())
    }

    #[test]
    fn the_video_appsrc_sheds_the_oldest_frame_not_the_newest() {
        if !ready(&["appsrc", "queue", "h264parse", "h265parse"]) {
            eprintln!(
                "SKIP the_video_appsrc_sheds_the_oldest_frame_not_the_newest: \
                 appsrc/queue/h264parse/h265parse not installed"
            );
            return;
        }
        let cfg = cfg();

        let bin = gstreamer::Bin::with_name("fp-arm-h264").upcast::<Element>();
        let v264 = pipe_h264(&bin, &cfg).expect("pipe_h264").appsrc;
        let bin = gstreamer::Bin::with_name("fp-arm-h265").upcast::<Element>();
        let v265 = pipe_h265(&bin, &cfg).expect("pipe_h265").appsrc;

        let mut audio: Option<(&str, String)> = None;
        let bin = gstreamer::Bin::with_name("fp-arm-aac").upcast::<Element>();
        if let Ok(l) = pipe_aac(&bin, &cfg) {
            audio = Some(("aac", leaky_of(&l.appsrc)));
        } else {
            let bin = gstreamer::Bin::with_name("fp-arm-adpcm").upcast::<Element>();
            if let Ok(l) = pipe_adpcm(&bin, 1024, &cfg) {
                audio = Some(("adpcm", leaky_of(&l.appsrc)));
            }
        }

        let (l264, l265) = (leaky_of(&v264), leaky_of(&v265));
        eprintln!(
            "[ARM leaky-downstream] configured video appsrc: h264 leaky-type={l264}, \
             h265 leaky-type={l265}, max-bytes={} ({} MB); audio={audio:?}",
            v264.max_bytes(),
            v264.max_bytes() / (1024 * 1024)
        );

        assert_eq!(
            l264, "downstream",
            "the h264 video appsrc must drop the OLDEST queued frame under pressure"
        );
        assert_eq!(
            l265, "downstream",
            "the h265 video appsrc must drop the OLDEST queued frame under pressure"
        );
        if let Some((kind, leaky)) = audio {
            assert_eq!(
                leaky, "none",
                "the {kind} audio appsrc keeps the simpler drop-NEWEST skip"
            );
        }
        assert_eq!(
            v264.max_bytes(),
            10 * 1024 * 1024,
            "the video appsrc buffer must be the fixed 10 MB (2542108)"
        );
    }
}
