// A/B arm for fix 14 — appended UNCHANGED to both the fixed tree and a
// pre-fix14 tree by ab_base_vs_fixed.sh.
//
// Table-tests the enclosing module's own DESCRIBE liveness predicate. On a
// pre-fix14 tree it is supplied by prefix_shim_fix14.rs, where it is
// hard-false: that tree has NO gate, so create_element proceeds for every
// camera, dead or not — which is what served the never-connected camera the
// 20 s splash whose post-EOS media tripped the get_rates SIGABRT.
#[cfg(test)]
mod arm_liveness_gate {
    use super::*;
    use std::sync::atomic::AtomicU64;
    use std::time::Instant;

    fn dead(last_seen_ms_ago: Option<u64>, factory_age: Duration) -> bool {
        let now = crate::common::now_epoch_ms();
        let last = Arc::new(AtomicU64::new(match last_seen_ms_ago {
            None => 0,
            Some(ago) => now.saturating_sub(ago),
        }));
        let armed_at = Instant::now()
            .checked_sub(factory_age)
            .expect("monotonic clock is old enough");
        liveness_gate_dead(&last, &armed_at)
    }

    #[test]
    fn a_dead_camera_is_gated_and_a_healthy_one_is_not() {
        let long_ago = Duration::from_secs(3600);
        let cases: &[(&str, Option<u64>, Duration, bool)] = &[
            ("never connected, factory 14s old", None, Duration::from_secs(14), false),
            ("never connected, factory 16s old", None, Duration::from_secs(16), true),
            ("connected, last frame 59s ago", Some(59_000), long_ago, false),
            ("connected, last frame 61s ago", Some(61_000), long_ago, true),
            ("connected, reconnect success 5s ago", Some(5_000), long_ago, false),
            (
                "idle-cycle window: 1x FRAME_STALENESS_MS + 5s",
                Some(crate::common::FRAME_STALENESS_MS + 5_000),
                long_ago,
                false,
            ),
        ];
        let mut bad = Vec::new();
        for (label, last_seen, age, expect_dead) in cases {
            let got = dead(*last_seen, *age);
            eprintln!("[ARM liveness-gate] {label}: dead={got} (expected {expect_dead})");
            if got != *expect_dead {
                bad.push(*label);
            }
        }
        assert!(bad.is_empty(), "DESCRIBE liveness gate wrong for: {:?}", bad);
    }
}
