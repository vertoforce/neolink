// A/B arm for a78b2da (EOS on sustained errors — Layer 2 self-recovery).
// Appended UNCHANGED to the fixed tree and to a78b2da^ (5b37294); there
// fp_prefix_shim_a78b2da.rs supplies the count-and-log-only error arm that
// tree implements.
//
// The load-bearing property is the CONTRAST: EOS_THRESHOLD-1 failures
// followed by a success must NOT fire, EOS_THRESHOLD consecutive failures
// must fire exactly once, and the pump must then exit after POST_EOS_GRACE.
#[cfg(test)]
mod fp_arm_eos_threshold {
    use super::*;

    fn run_errors(n: u32) -> (usize, Option<usize>, Option<usize>) {
        let mut st = PumpFailureState::default();
        let mut eos = 0usize;
        let mut eos_at = None;
        let mut exit_at = None;
        for i in 1..=n {
            let a = st.on_error(false);
            if a == PumpAction::SignalEosErrors {
                eos += 1;
                eos_at.get_or_insert(i as usize);
            }
            if matches!(
                a,
                PumpAction::ExitDetached
                    | PumpAction::ExitAfterErrorEos
                    | PumpAction::ExitAfterBackPressureEos
            ) {
                exit_at = Some(i as usize);
                break;
            }
        }
        (eos, eos_at, exit_at)
    }

    #[test]
    fn sustained_errors_eos_and_a_near_miss_does_not() {
        // NO-FIRE side: one short of the threshold, then a success.
        let mut st = PumpFailureState::default();
        let mut near_miss_eos = 0usize;
        for _ in 0..(EOS_THRESHOLD - 1) {
            if st.on_error(false) == PumpAction::SignalEosErrors {
                near_miss_eos += 1;
            }
        }
        st.on_sent();

        // FIRE side.
        let (eos, eos_at, exit_at) = run_errors(EOS_THRESHOLD + POST_EOS_GRACE);

        eprintln!(
            "[ARM eos-threshold] {} errors then a success -> {near_miss_eos} EOS; \
             {} consecutive errors -> {eos} EOS (first at push #{eos_at:?}), exit at #{exit_at:?}",
            EOS_THRESHOLD - 1,
            EOS_THRESHOLD + POST_EOS_GRACE
        );

        assert_eq!(
            near_miss_eos,
            0,
            "{} errors then a recovery must NOT fire EOS",
            EOS_THRESHOLD - 1
        );
        assert_eq!(
            eos_at,
            Some(EOS_THRESHOLD as usize),
            "EOS must fire on error #{EOS_THRESHOLD}"
        );
        assert_eq!(eos, 1, "EOS is one-shot per cascade");
        assert_eq!(
            exit_at,
            Some((EOS_THRESHOLD + POST_EOS_GRACE) as usize),
            "the pump must exit EOS_THRESHOLD+POST_EOS_GRACE pushes into the cascade"
        );
    }
}
