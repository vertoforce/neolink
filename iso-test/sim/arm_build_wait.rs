// A/B arm for fix 6 — appended UNCHANGED to both the fixed tree and a
// pre-fix6 tree by ab_base_vs_fixed.sh.
//
// Models gst-rtsp-server's SINGLE shared glib main-loop thread (the thread
// create_element runs on) serving two cameras' DESCRIBEs back to back:
// camera A is mid-reconnect and its per-camera build task never replies,
// camera B is healthy and replies immediately. The wait is the enclosing
// module's own `await_build_reply` / `BUILD_REPLY_TIMEOUT`.
//
// On a pre-fix6 tree those are supplied by prefix_shim_fix6.rs (an
// UNBOUNDED blocking receive, as that tree's `new_element.blocking_recv()`),
// and camera B is never reached.
#[cfg(test)]
mod arm_build_wait {
    use super::*;
    use std::sync::mpsc as stdmpsc;
    use std::time::Instant;

    #[test]
    fn a_wedged_camera_does_not_freeze_describe_for_the_others() {
        let outer = BUILD_REPLY_TIMEOUT + Duration::from_secs(3);

        let (a_tx, a_rx) = stdmpsc::sync_channel::<u8>(1);
        let (b_tx, b_rx) = stdmpsc::sync_channel::<u8>(1);
        let (done_tx, done_rx) = stdmpsc::channel::<(Duration, Option<Duration>)>();

        std::thread::Builder::new()
            .name("fx-glib-loop".into())
            .spawn(move || {
                // ---- DESCRIBE for camera A (build task never replies) ----
                let a_start = Instant::now();
                let _ = await_build_reply(&a_rx, BUILD_REPLY_TIMEOUT);
                let a_wait = a_start.elapsed();
                // ---- DESCRIBE for camera B (healthy) ----
                let b_start = Instant::now();
                let served = await_build_reply(&b_rx, BUILD_REPLY_TIMEOUT).is_ok();
                let _ = done_tx.send((a_wait, served.then(|| b_start.elapsed())));
            })
            .expect("spawn");

        b_tx.send(1).expect("camera B's build task replies");
        let res = done_rx.recv_timeout(outer);
        // Release camera A only now, so the unbounded arm really blocks.
        drop(a_tx);

        match res {
            Ok((a_wait, b)) => {
                eprintln!(
                    "[ARM build-wait] camera A DESCRIBE returned after {a_wait:?}; \
                     camera B served after {b:?}"
                );
                assert!(
                    a_wait < BUILD_REPLY_TIMEOUT + Duration::from_secs(2),
                    "camera A's DESCRIBE held the shared glib loop for {:?}",
                    a_wait
                );
                assert!(b.is_some(), "camera B was never served");
            }
            Err(_) => {
                eprintln!(
                    "[ARM build-wait] shared glib loop still parked in camera A's receive \
                     after {outer:?} — camera B never served"
                );
                panic!(
                    "one wedged camera froze DESCRIBE for every camera: the shared loop did \
                     not return within {:?}",
                    outer
                );
            }
        }
    }
}
