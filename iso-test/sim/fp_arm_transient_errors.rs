// A/B arm for 14e8129 (frame-pump resilient to transient send errors).
// Appended UNCHANGED to the fixed tree and to 14e8129^ (2542108); on the
// pre-fix tree fp_prefix_shim_14e8129.rs supplies the state machine that
// tree actually implements (`r?` — die on the first error).
//
// The measured property: a burst of transient "App source is closed" errors
// during a pipeline state transition, followed by recovery, must leave the
// pump alive and pumping.
#[cfg(test)]
mod fp_arm_transient_errors {
    use super::*;

    #[derive(Debug, Clone, Copy)]
    enum Push {
        Sent,
        Error { detached: bool },
    }

    fn is_exit(a: PumpAction) -> bool {
        matches!(
            a,
            PumpAction::ExitDetached
                | PumpAction::ExitAfterErrorEos
                | PumpAction::ExitAfterBackPressureEos
        )
    }

    /// Returns how many pushes the pump survived before it broke out of the
    /// loop (== pushes.len() when it never did).
    fn survived(pushes: &[Push]) -> usize {
        let mut st = PumpFailureState::default();
        for (i, p) in pushes.iter().enumerate() {
            let a = match p {
                Push::Sent => {
                    st.on_sent();
                    PumpAction::Continue
                }
                Push::Error { detached } => st.on_error(*detached),
            };
            if is_exit(a) {
                return i;
            }
        }
        pushes.len()
    }

    #[test]
    fn a_transient_send_error_burst_does_not_kill_the_frame_pump() {
        const BURST: usize = 8;
        const HEALTHY: usize = 1000;
        let mut pushes: Vec<Push> = (0..BURST).map(|_| Push::Error { detached: true }).collect();
        pushes.extend(std::iter::repeat_n(Push::Sent, HEALTHY));

        let lived = survived(&pushes);
        eprintln!(
            "[ARM transient-errors] {BURST} transient errors then {HEALTHY} healthy frames: \
             pump survived {lived} of {} pushes",
            pushes.len()
        );
        assert_eq!(
            lived,
            pushes.len(),
            "a {BURST}-error transient killed the frame-pump after {lived} pushes"
        );

        // And a SINGLE error must be a no-op for the pump's lifetime.
        let one = [Push::Error { detached: false }];
        assert_eq!(
            survived(&one),
            1,
            "one send error must not end the frame-pump"
        );
    }
}
