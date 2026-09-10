// Appended ONLY to a pre-a78b2da tree (a78b2da^ = 5b37294 "remove thread
// exit — let segment-watchdog handle stuck streams"), so
// fp_arm_eos_threshold.rs can run there unchanged.
//
// That tree's error arm, verbatim:
//
//     Err(e) => {
//         consecutive_errors += 1;
//         if consecutive_errors.is_power_of_two() {
//             log::info!("... send error #{consecutive_errors} ...");
//         }
//     }
//
// It counts and logs and does nothing else: no EOS, no exit. A wedged appsrc
// is left to the external segment-watchdog. The shim encodes exactly that.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
enum PumpAction {
    Continue,
    ExitDetached,
    SignalEosErrors,
    ExitAfterErrorEos,
    SignalEosBackPressure,
    ExitAfterBackPressureEos,
}

#[cfg(test)]
#[allow(dead_code)]
const EOS_THRESHOLD: u32 = 100;
#[cfg(test)]
#[allow(dead_code)]
const POST_EOS_GRACE: u32 = 50;

#[cfg(test)]
#[derive(Debug, Default)]
struct PumpFailureState {
    consecutive_errors: u32,
}

#[cfg(test)]
#[allow(dead_code)]
impl PumpFailureState {
    fn on_sent(&mut self) -> (u32, u32) {
        let prior = (self.consecutive_errors, 0);
        self.consecutive_errors = 0;
        prior
    }

    fn on_backpressure(&mut self) -> PumpAction {
        PumpAction::Continue
    }

    fn on_error(&mut self, _detached: bool) -> PumpAction {
        self.consecutive_errors += 1;
        PumpAction::Continue
    }

    fn mark_eos_signaled(&mut self) {}

    fn consecutive_errors(&self) -> u32 {
        self.consecutive_errors
    }
}
