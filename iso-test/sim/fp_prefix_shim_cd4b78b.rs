// Appended ONLY to a pre-cd4b78b tree (cd4b78b^ = 681cc41), so
// fp_arm_backpressure.rs can run there unchanged.
//
// On that tree `send_to_sources` returns `AnyResult<()>` and `send_to_appsrc`
// swallows the full-queue case:
//
//     if let Err(e) = appsrc.push_buffer(buf) {
//         ... /* Flushing is not surfaced */
//     }
//     Ok(())
//
// so an appsrc whose queue is full because the RTSP consumer stopped draining
// looks EXACTLY like a healthy send: it resets consecutive_errors and no
// watchdog can ever fire. There is no back-pressure counter and no
// BACKPRESSURE_EOS_THRESHOLD. The shim encodes that: every blocked push is
// simply Continue.
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
const BACKPRESSURE_EOS_THRESHOLD: u32 = 400;
#[cfg(test)]
#[allow(dead_code)]
const POST_EOS_GRACE: u32 = 50;
#[cfg(test)]
#[allow(dead_code)]
const EOS_THRESHOLD: u32 = 100;

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

    /// A full appsrc was indistinguishable from a healthy send, and it even
    /// CLEARED the error budget.
    fn on_backpressure(&mut self) -> PumpAction {
        self.consecutive_errors = 0;
        PumpAction::Continue
    }

    fn on_error(&mut self, _detached: bool) -> PumpAction {
        self.consecutive_errors += 1;
        if self.consecutive_errors == EOS_THRESHOLD {
            return PumpAction::SignalEosErrors;
        }
        PumpAction::Continue
    }

    fn mark_eos_signaled(&mut self) {}

    fn consecutive_errors(&self) -> u32 {
        self.consecutive_errors
    }
}
