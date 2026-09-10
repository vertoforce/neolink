// A/B arm for c34b277 (DETACHED_FAST_EXIT_THRESHOLD). Appended UNCHANGED to
// the fixed tree and to c34b277^ (7fe3ef3), where fp_prefix_shim_c34b277.rs
// supplies the tree's generic-error-only handling.
//
// Scenario: the last client left, gst-rtsp-server took the bin to NULL and
// removed our appsrc. Every push from here on fails with the terminal "App
// source is closed". The pump must give up FAST — it is holding per-thread
// BufferPool socketpairs and a BC start_video subscription that a whole
// rebuild cycle wants back.
#[cfg(test)]
mod fp_arm_detached_fast_exit {
    use super::*;

    #[test]
    fn a_terminally_detached_appsrc_exits_the_pump_fast() {
        let mut st = PumpFailureState::default();
        let mut exit_at = None;
        let mut how = None;
        for i in 1..=(EOS_THRESHOLD + POST_EOS_GRACE + 50) {
            let a = st.on_error(true);
            if matches!(
                a,
                PumpAction::ExitDetached
                    | PumpAction::ExitAfterErrorEos
                    | PumpAction::ExitAfterBackPressureEos
            ) {
                exit_at = Some(i as usize);
                how = Some(a);
                break;
            }
        }
        let generic = (EOS_THRESHOLD + POST_EOS_GRACE) as usize;
        eprintln!(
            "[ARM detached-fast-exit] permanently-detached appsrc: pump exited after \
             {exit_at:?} failed pushes via {how:?} (~{:.2}s @20fps); the generic \
             error window is {generic} pushes (~{:.2}s)",
            exit_at.unwrap_or(0) as f64 / 20.0,
            generic as f64 / 20.0
        );

        assert_eq!(
            exit_at,
            Some(DETACHED_FAST_EXIT_THRESHOLD as usize),
            "a terminal detach must exit at DETACHED_FAST_EXIT_THRESHOLD, \
             not ride out the generic {generic}-push error window"
        );
        assert_eq!(
            how,
            Some(PumpAction::ExitDetached),
            "the terminal detach exits on its own path, without firing EOS at a \
             dead appsrc"
        );

        // A non-detached error in the middle resets the detach run: a pool
        // hiccup must not be mistaken for a full unprepare.
        let mut st = PumpFailureState::default();
        for _ in 0..(DETACHED_FAST_EXIT_THRESHOLD - 1) {
            assert_ne!(st.on_error(true), PumpAction::ExitDetached);
        }
        assert_ne!(st.on_error(false), PumpAction::ExitDetached);
        for _ in 0..(DETACHED_FAST_EXIT_THRESHOLD - 1) {
            assert_ne!(
                st.on_error(true),
                PumpAction::ExitDetached,
                "the detach run must have restarted from zero"
            );
        }
    }
}
