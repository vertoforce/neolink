// A/B arm for cd4b78b (back-pressure watchdog). Appended UNCHANGED to the
// fixed tree and to cd4b78b^ (681cc41); there fp_prefix_shim_cd4b78b.rs
// supplies the tree's actual behaviour — a full appsrc looked like a healthy
// send, so nothing could fire.
//
// Scenario: the silent wedge. Frames keep arriving from the camera and every
// push "succeeds", but the appsrc queue is full because the RTSP consumer
// (ffmpeg in poll()) stopped draining. No client churn, no factory rebuild,
// no error. Something must notice.
#[cfg(test)]
mod fp_arm_backpressure {
    use super::*;

    #[test]
    fn a_wedged_consumer_is_eosd_and_the_pump_exits() {
        const RUN: u32 = BACKPRESSURE_EOS_THRESHOLD + POST_EOS_GRACE + 25;

        let mut st = PumpFailureState::default();
        let mut eos_at = None;
        let mut exit_at = None;
        for i in 1..=RUN {
            let a = st.on_backpressure();
            if a == PumpAction::SignalEosBackPressure {
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

        // NO-FIRE side: a slow-but-recovering consumer.
        let mut st2 = PumpFailureState::default();
        let mut near_miss = 0usize;
        for _ in 0..(BACKPRESSURE_EOS_THRESHOLD - 1) {
            if st2.on_backpressure() == PumpAction::SignalEosBackPressure {
                near_miss += 1;
            }
        }
        st2.on_sent();

        eprintln!(
            "[ARM backpressure] {RUN} blocked pushes into a wedged consumer: \
             EOS at #{eos_at:?}, exit at #{exit_at:?} (~{:.1}s @20fps); \
             {} blocked pushes then a drain -> {near_miss} EOS",
            f64::from(BACKPRESSURE_EOS_THRESHOLD + POST_EOS_GRACE) / 20.0,
            BACKPRESSURE_EOS_THRESHOLD - 1
        );

        assert_eq!(near_miss, 0, "a consumer that catches up must not be EOS'd");
        assert_eq!(
            eos_at,
            Some(BACKPRESSURE_EOS_THRESHOLD as usize),
            "sustained back-pressure must EOS at BACKPRESSURE_EOS_THRESHOLD"
        );
        assert_eq!(
            exit_at,
            Some((BACKPRESSURE_EOS_THRESHOLD + POST_EOS_GRACE) as usize),
            "the pump must then exit after POST_EOS_GRACE"
        );
    }
}
