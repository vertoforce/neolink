// A/B arm for fix 16 — appended UNCHANGED to both the fixed tree and a
// pre-fix16 tree by ab_base_vs_fixed.sh.
//
// It builds `appsrc ! rtph264pay ! fakesink`, registers the egress probe
// using the enclosing module's own `egress_probe_mask()` /
// `egress_packet_count()`, and asserts the invariant that the egress-stall
// watchdog and the fix 13 starvation exit both depend on:
//
//     after RTP has left pay0, egress_count > 0
//
// On a pre-fix16 tree those two items are supplied by prefix_shim_fix16.rs
// (the BUFFER-only mask + "+1 per invocation", lifted verbatim from that
// tree's inline add_probe call) and this arm FAILS with egress_count == 0
// for any frame larger than the MTU.
#[cfg(test)]
mod arm_egress_probe {
    use super::*;
    use gstreamer::prelude::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    /// Returns (egress_count_this_tree_would_have, real_pushes_on_the_pad).
    fn feed(nal_size: usize, mtu: u32, count: usize) -> Option<(u64, u64)> {
        if gstreamer::init().is_err() {
            return None;
        }
        for e in ["appsrc", "rtph264pay", "fakesink"] {
            if gstreamer::ElementFactory::find(e).is_none() {
                eprintln!("SKIP arm_egress_probe: GStreamer element {e} not installed");
                return None;
            }
        }
        let desc = format!(
            "appsrc name=src is-live=false format=time \
             caps=video/x-h264,stream-format=byte-stream,alignment=nal \
             ! rtph264pay mtu={mtu} name=pay0 ! fakesink sync=false"
        );
        let pipeline = gstreamer::parse::launch(&desc)
            .expect("pipeline parses")
            .downcast::<gstreamer::Pipeline>()
            .expect("is a Pipeline");
        let pay0 = pipeline.by_name("pay0").expect("pay0");
        let srcpad = pay0.static_pad("src").expect("pay0 src pad");

        // ---- THE TREE'S OWN EGRESS PROBE ----
        let egress_count = Arc::new(AtomicU64::new(0));
        {
            let egress_count_probe = egress_count.clone();
            let _ = srcpad.add_probe(egress_probe_mask(), move |_pad, info| {
                egress_count_probe
                    .fetch_add(egress_packet_count(info.data.as_ref()), Ordering::Relaxed);
                gstreamer::PadProbeReturn::Ok
            });
        }
        // ---- independent ground truth: pushes that really happened ----
        let pushes = Arc::new(AtomicU64::new(0));
        {
            let pushes_probe = pushes.clone();
            let _ = srcpad.add_probe(
                gstreamer::PadProbeType::BUFFER | gstreamer::PadProbeType::BUFFER_LIST,
                move |_pad, _info| {
                    pushes_probe.fetch_add(1, Ordering::Relaxed);
                    gstreamer::PadProbeReturn::Ok
                },
            );
        }

        pipeline
            .set_state(gstreamer::State::Playing)
            .expect("pipeline PLAYING");
        let src = pipeline
            .by_name("src")
            .expect("src")
            .downcast::<gstreamer_app::AppSrc>()
            .expect("is an appsrc");
        for i in 0..count {
            let mut nal = Vec::with_capacity(nal_size + 5);
            nal.extend_from_slice(&[0x00, 0x00, 0x00, 0x01, 0x65]);
            nal.resize(nal_size, (i % 251) as u8 | 0x01);
            let mut buf = gstreamer::Buffer::from_mut_slice(nal);
            {
                let b = buf.get_mut().unwrap();
                b.set_pts(gstreamer::ClockTime::from_mseconds(33 * i as u64));
                b.set_duration(gstreamer::ClockTime::from_mseconds(33));
            }
            src.push_buffer(buf).expect("appsrc accepts the NAL");
        }
        let _ = src.end_of_stream();
        let bus = pipeline.bus().expect("bus");
        let _ = bus.timed_pop_filtered(
            gstreamer::ClockTime::from_seconds(60),
            &[gstreamer::MessageType::Eos, gstreamer::MessageType::Error],
        );
        let _ = pipeline.set_state(gstreamer::State::Null);
        Some((
            egress_count.load(Ordering::Relaxed),
            pushes.load(Ordering::Relaxed),
        ))
    }

    /// Frames larger than the MTU — what every production camera produces.
    #[test]
    fn egress_counter_advances_when_rtp_leaves_pay0() {
        let Some((counted, pushes)) = feed(6000, 1400, 60) else {
            return;
        };
        eprintln!(
            "[ARM egress-probe] FRAGMENTING 60 x 6000-byte NAL, mtu=1400: \
             real pay0 pushes={pushes} | this tree's egress_count={counted}"
        );
        assert!(
            counted > 0,
            "egress_count never advanced ({counted}) across {pushes} real pay0 pushes — \
             the egress-stall and fix 13 starvation watchdogs can never arm here"
        );
    }

    /// Control: frames that fit the MTU. Both trees pass this one, which is
    /// exactly why the blindness stayed hidden.
    #[test]
    fn egress_counter_advances_on_a_non_fragmenting_pipeline() {
        let Some((counted, pushes)) = feed(800, 1400, 60) else {
            return;
        };
        eprintln!(
            "[ARM egress-probe] CONTROL 60 x 800-byte NAL, mtu=1400: \
             real pay0 pushes={pushes} | this tree's egress_count={counted}"
        );
        assert!(counted > 0, "control pipeline counted {counted}");
    }

    /// fix 16 half two: the egress-stall and fix 13 starvation exits must
    /// only arm while pay0 is PLAYING. A cached shared media left PAUSED by a
    /// DESCRIBE-only client (an ffprobe health check, a consumer that died
    /// before PLAY) otherwise looks exactly like an egress stall, and EOS'ing
    /// an unowned cached media leaves the post-EOS corpse every later
    /// DESCRIBE reuses.
    #[test]
    fn stall_exits_do_not_arm_on_a_paused_cached_media() {
        if gstreamer::init().is_err() || gstreamer::ElementFactory::find("rtph264pay").is_none() {
            eprintln!("SKIP arm_egress_probe: rtph264pay not installed");
            return;
        }
        let el = gstreamer::ElementFactory::make("rtph264pay")
            .build()
            .expect("rtph264pay builds");
        el.set_state(gstreamer::State::Paused).expect("pay0 PAUSED");
        let armed_paused = pay0_playing(Some(&el));
        el.set_state(gstreamer::State::Playing)
            .expect("pay0 PLAYING");
        let armed_playing = pay0_playing(Some(&el));
        let armed_absent = pay0_playing(None);
        let _ = el.set_state(gstreamer::State::Null);

        eprintln!(
            "[ARM egress-probe] stall exits armed: pay0 PAUSED={armed_paused}, \
             pay0 PLAYING={armed_playing}, no pay0={armed_absent}"
        );
        assert!(
            !armed_paused,
            "a PAUSED (cached, DESCRIBE-only) pipeline must not arm the stall exits"
        );
        assert!(!armed_absent, "a splash pipeline with no pay0 must not arm them");
        assert!(armed_playing, "a PLAYING pipeline must arm them");
    }
}
