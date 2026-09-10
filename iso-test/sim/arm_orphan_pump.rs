// A/B arm for fix 15 — appended UNCHANGED to both the fixed tree and a
// pre-fix15 tree by ab_base_vs_fixed.sh. Covers the two load-bearing
// halves of the orphan frame-pump leak, both through the enclosing module's
// own items:
//
//   1. deliver_build(): a build whose DESCRIBE already timed out must be
//      DiscardedRequesterGone so the caller spawns no frame-pump.
//   2. OrphanDetachedTicks: a pump whose appsrc has left the bin must exit
//      on empty ticks, without needing a frame.
//
// On a pre-fix15 tree both are supplied by prefix_shim_fix15.rs (always
// Delivered; ticks never exit), which is what that tree does.
#[cfg(test)]
mod arm_orphan_pump {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc as stdmpsc;
    use std::sync::Arc;

    fn named_thread_count(name: &str) -> usize {
        let Ok(dir) = std::fs::read_dir("/proc/self/task") else {
            return 0;
        };
        dir.filter_map(|e| e.ok())
            .filter(|e| {
                std::fs::read_to_string(e.path().join("comm"))
                    .map(|c| c.trim() == name)
                    .unwrap_or(false)
            })
            .count()
    }

    /// N builds that complete after their DESCRIBE gave up at
    /// BUILD_REPLY_TIMEOUT. Counts the frame-pump threads this tree leaves
    /// behind.
    #[test]
    fn a_build_whose_describe_timed_out_leaves_no_frame_pump() {
        const TIMED_OUT: usize = 16;
        let stop = Arc::new(AtomicBool::new(false));
        let baseline = named_thread_count("fx-orphan-pump");

        let mut pumps = Vec::new();
        let mut subscriptions = Vec::new();
        for _ in 0..TIMED_OUT {
            let (reply, rx) = stdmpsc::sync_channel::<Vec<u8>>(1);
            drop(rx); // the DESCRIBE gave up (fix 6 bounded wait)
            let (media_tx, media_rx) = stdmpsc::channel::<u8>();
            match deliver_build(reply, vec![0u8; 4096]) {
                BuildDelivery::DiscardedRequesterGone => {
                    drop(media_rx);
                    drop(media_tx);
                    continue;
                }
                BuildDelivery::Delivered => {}
            }
            // The stream() task keeps media_tx alive, so media_rx is EMPTY
            // but OPEN forever: no pre-fix15 pump exit path can ever run.
            subscriptions.push(media_tx);
            let stop = stop.clone();
            pumps.push(
                std::thread::Builder::new()
                    .name("fx-orphan-pump".into())
                    .spawn(move || {
                        while !stop.load(Ordering::Relaxed) {
                            match media_rx.recv_timeout(Duration::from_millis(20)) {
                                Ok(_) | Err(stdmpsc::RecvTimeoutError::Timeout) => {}
                                Err(stdmpsc::RecvTimeoutError::Disconnected) => break,
                            }
                        }
                    })
                    .expect("spawn"),
            );
        }
        std::thread::sleep(Duration::from_millis(200));
        let leaked = named_thread_count("fx-orphan-pump") - baseline;
        eprintln!(
            "[ARM orphan-pump] {TIMED_OUT} builds whose DESCRIBE timed out: \
             live orphan frame-pump threads={leaked}"
        );

        stop.store(true, Ordering::Relaxed);
        drop(subscriptions);
        for h in pumps {
            let _ = h.join();
        }
        assert_eq!(
            leaked, 0,
            "{leaked} orphan frame-pump thread(s) left behind by {TIMED_OUT} timed-out builds"
        );
    }

    /// A pump whose appsrc left the bin must be reaped from empty ticks
    /// alone.
    #[test]
    fn a_detached_pump_is_reaped_from_empty_ticks() {
        let mut t = OrphanDetachedTicks::default();
        let mut exited_after = None;
        for tick in 1..=1000u32 {
            if t.observe(true) {
                exited_after = Some(tick);
                break;
            }
        }
        eprintln!(
            "[ARM orphan-pump] detached-but-idle pump: exited after {:?} empty ticks \
             ({:?} ms)",
            exited_after,
            exited_after.map(|t| u64::from(t) * PUMP_RECV_TICK_MS)
        );
        assert!(
            exited_after.is_some(),
            "a detached pump was never reaped across 1000 empty ticks \
             ({} s of wall clock)",
            1000 * PUMP_RECV_TICK_MS / 1000
        );
        // An attached tick must reset the run (no premature reap).
        let mut t = OrphanDetachedTicks::default();
        assert!(!t.observe(true));
        assert!(!t.observe(true));
        assert!(!t.observe(false), "an attached tick must reset the run");
    }
}
