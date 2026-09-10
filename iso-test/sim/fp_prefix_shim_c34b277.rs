// Appended ONLY to a pre-c34b277 tree (c34b277^ = 7fe3ef3), so
// fp_arm_detached_fast_exit.rs can run there unchanged.
//
// That tree has no terminal-detach special case at all: the error arm is
//
//     Err(e) => {
//         consecutive_errors += 1;
//         if consecutive_errors.is_power_of_two() { log::info!(...); }
//         if consecutive_errors == EOS_THRESHOLD && !eos_signaled { ...EOS... }
//         if eos_signaled && consecutive_errors >= EOS_THRESHOLD + POST_EOS_GRACE {
//             break;
//         }
//     }
//
// so "App source is closed" — which for THIS pipeline is terminal, the appsrc
// has left the bin on a full unprepare and can never come back — is treated as
// just another transient error and the doomed thread holds its BufferPool
// socketpairs (~12/cycle) and its BC start_video subscription for the full
// EOS_THRESHOLD + POST_EOS_GRACE window. The shim ignores `detached` exactly
// as that tree does.
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
#[allow(dead_code)]
const DETACHED_FAST_EXIT_THRESHOLD: u32 = 15;

#[cfg(test)]
#[derive(Debug, Default)]
struct PumpFailureState {
    consecutive_errors: u32,
    eos_signaled: bool,
}

#[cfg(test)]
#[allow(dead_code)]
impl PumpFailureState {
    fn on_sent(&mut self) -> (u32, u32) {
        let prior = (self.consecutive_errors, 0);
        self.consecutive_errors = 0;
        self.eos_signaled = false;
        prior
    }

    fn on_backpressure(&mut self) -> PumpAction {
        PumpAction::Continue
    }

    /// `detached` is ignored — pre-c34b277 there was no such distinction.
    fn on_error(&mut self, _detached: bool) -> PumpAction {
        self.consecutive_errors += 1;
        if self.consecutive_errors == EOS_THRESHOLD && !self.eos_signaled {
            self.eos_signaled = true;
            return PumpAction::SignalEosErrors;
        }
        if self.eos_signaled && self.consecutive_errors >= EOS_THRESHOLD + POST_EOS_GRACE {
            return PumpAction::ExitAfterErrorEos;
        }
        PumpAction::Continue
    }

    fn mark_eos_signaled(&mut self) {
        self.eos_signaled = true;
    }

    fn consecutive_errors(&self) -> u32 {
        self.consecutive_errors
    }
}
