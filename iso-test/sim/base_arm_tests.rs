// ===========================================================================
// BASE-TREE ARM (commit 8708608, upstream/master + PRs #373/#400/#399/#398,
// no fixes). Appended by the fix-E regression-test work purely to MEASURE
// the base behaviour. Not part of the base commit.
// ===========================================================================
#[cfg(test)]
mod base_arm_tests {
    use super::*;
    use gstreamer_rtsp_server::{RTSPMediaFactory, RTSPSessionPool};
    use std::time::{Duration as StdDuration, Instant};

    fn gst_ready() -> bool {
        use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
        use std::sync::Once;
        static INIT: Once = Once::new();
        static OK: AtomicBool = AtomicBool::new(false);
        INIT.call_once(|| OK.store(gstreamer::init().is_ok(), AtomicOrdering::SeqCst));
        OK.load(AtomicOrdering::SeqCst)
    }

    /// Same library measurement as the fixed tree: the unit is milliseconds
    /// and `extra_timeout` is folded in.
    #[test]
    fn base_next_timeout_usec_units() {
        if !gst_ready() {
            eprintln!("SKIP base_next_timeout_usec_units");
            return;
        }
        let pool = RTSPSessionPool::new();
        let s = pool.create().expect("session");
        s.set_timeout(30);
        s.touch();
        let r = s.next_timeout_usec(glib::monotonic_time());
        eprintln!(
            "BASE MEASURED: timeout=30s extra_timeout={}s -> next_timeout_usec={r}",
            s.extra_timeout()
        );
        // This is the value the base sweep logs and then IGNORES.
        assert!(r > 30_000 && r < 36_000);
    }

    /// The base sweep's own decision, lifted verbatim from the loop above:
    /// it computes `remaining` and always returns Keep. Nothing is ever reaped.
    #[test]
    fn base_sweep_never_reaps() {
        if !gst_ready() {
            eprintln!("SKIP base_sweep_never_reaps");
            return;
        }
        let pool = RTSPSessionPool::new();
        for _ in 0..3 {
            let s = pool.create().expect("session");
            s.set_timeout(1);
            s.set_extra_timeout(0);
            s.touch();
        }
        std::thread::sleep(StdDuration::from_millis(50));
        let before = pool.n_sessions();
        let mut seen = 0usize;
        // >>> verbatim base sweep body (8708608 src/rtsp/gst/server.rs:105) <<<
        pool.filter(Some(&mut |_, session| {
            let remaining = session.next_timeout_usec(glib::monotonic_time());
            log::debug!(
                "{:?}: {}/{}",
                session.sessionid(),
                remaining,
                session.timeout(),
            );
            seen += 1;
            RTSPFilterResult::Keep
        }));
        let after = pool.n_sessions();
        eprintln!(
            "BASE sweep over {seen} deep-stale session(s) (timeout=1s, remaining<=1000ms): n_sessions {before} -> {after} — reaped 0"
        );
        assert_eq!(before, 3);
        assert_eq!(after, 3, "the base sweep reaps nothing, ever");
    }

    /// `RTSPSessionPool::filter()` returns the Ref'd set, not the Removed set.
    #[test]
    fn base_pool_filter_returns_refd_not_removed() {
        if !gst_ready() {
            eprintln!("SKIP base_pool_filter_returns_refd_not_removed");
            return;
        }
        let pool = RTSPSessionPool::new();
        for _ in 0..3 {
            pool.create().expect("session");
        }
        let before = pool.n_sessions();
        let returned = pool.filter(Some(&mut |_, _| RTSPFilterResult::Remove));
        eprintln!(
            "BASE filter(Remove): n_sessions {before} -> {}; returned {} (the value a naive reap would log)",
            pool.n_sessions(),
            returned.len()
        );
        assert_eq!(returned.len(), 0);
        assert_eq!(pool.n_sessions(), 0);
    }

    /// The fix 9 scenario on the BASE tree: a real consumer whose session is
    /// removed from the pool keeps its TCP connection and streams nothing —
    /// the zombie. The base has no client-close step at all.
    #[test]
    fn base_live_reap_only_zombies_the_consumer() {
        if !gst_ready()
            || !["videotestsrc", "rtpvrawpay", "rtspsrc", "fakesink"]
                .iter()
                .all(|e| gstreamer::ElementFactory::find(e).is_some())
        {
            eprintln!("SKIP base_live_reap_only_zombies_the_consumer: no GStreamer runtime");
            return;
        }
        let server = NeoRtspServer::new().expect("server");
        server.set_address("127.0.0.1");
        server.set_service("0");
        let mounts = server.mount_points().expect("mounts");
        let f = RTSPMediaFactory::new();
        f.set_launch("( videotestsrc is-live=true ! video/x-raw,format=I420,width=64,height=48,framerate=10/1 ! rtpvrawpay name=pay0 pt=96 )");
        f.set_shared(true);
        f.set_transport_mode(gstreamer_rtsp_server::RTSPTransportMode::PLAY);
        f.set_suspend_mode(gstreamer_rtsp_server::RTSPSuspendMode::None);
        f.add_role_from_structure(
            &gstreamer::Structure::builder("anonymous")
                .field(gstreamer_rtsp_server::RTSP_PERM_MEDIA_FACTORY_ACCESS, true)
                .field(gstreamer_rtsp_server::RTSP_PERM_MEDIA_FACTORY_CONSTRUCT, true)
                .build(),
        );
        mounts.add_factory("/testcam/main", f);
        server.attach(None).expect("attach");
        let port = server.bound_port();
        let ml = glib::MainLoop::new(None, false);
        let ml2 = ml.clone();
        let h = std::thread::spawn(move || ml2.run());
        std::thread::sleep(StdDuration::from_millis(200));

        let client = gstreamer::parse::launch(&format!(
            "rtspsrc location=rtsp://127.0.0.1:{port}/testcam/main protocols=tcp latency=0 ! fakesink sync=false"
        ))
        .expect("client");
        client.set_state(gstreamer::State::Playing).expect("play");
        let t0 = Instant::now();
        loop {
            let (_, cur, _) = client.state(Some(gstreamer::ClockTime::from_mseconds(200)));
            if cur == gstreamer::State::Playing || t0.elapsed() > StdDuration::from_secs(15) {
                eprintln!("BASE rig: consumer state {cur:?} after {:?}", t0.elapsed());
                break;
            }
        }
        let n = server.session_pool().map(|p| p.n_sessions()).unwrap_or(0);
        eprintln!("BASE live: consumer attached, n_sessions={n}");
        assert_eq!(n, 1);

        // The only mechanism the base has: remove the session from the pool.
        let mut reaped = 0usize;
        server.session_pool().unwrap().filter(Some(&mut |_, _| {
            reaped += 1;
            RTSPFilterResult::Remove
        }));
        let owners = server
            .client_filter(Some(&mut |_, _| RTSPFilterResult::Ref))
            .iter()
            .filter(|c| {
                !c.session_filter(Some(&mut |_, _| RTSPFilterResult::Ref))
                    .is_empty()
            })
            .count();
        eprintln!(
            "BASE live: pool-reaped {reaped}, n_sessions now {}, clients still owning a session: {owners}",
            server.session_pool().map(|p| p.n_sessions()).unwrap_or(0)
        );

        // Does the consumer notice?
        let bus = client.bus().expect("bus");
        let t0 = Instant::now();
        let mut ended = None;
        while t0.elapsed() < StdDuration::from_secs(4) {
            while let Some(msg) = bus.pop() {
                if matches!(
                    msg.view(),
                    gstreamer::MessageView::Error(_) | gstreamer::MessageView::Eos(_)
                ) {
                    ended = Some(t0.elapsed());
                }
            }
            std::thread::sleep(StdDuration::from_millis(25));
        }
        eprintln!(
            "BASE live: consumer after 4000ms -> ended={ended:?} (None = still connected = ZOMBIE)"
        );
        let _ = client.set_state(gstreamer::State::Null);
        ml.quit();
        let _ = h.join();
        assert_eq!(reaped, 1);
        assert_eq!(owners, 0, "the owning client is unmatchable after the reap");
        assert!(ended.is_none(), "BASE: the consumer zombies");
    }
}
