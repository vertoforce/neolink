// Appended ONLY to a pre-14e8129 tree (14e8129^ = 2542108), so
// fp_arm_transient_errors.rs can run there unchanged.
//
// That tree's frame-pump body, verbatim:
//
//     while let Some(data) = media_rx.blocking_recv() {
//         let r = send_to_sources(data, &mut pools, &vid_src, &aud_src,
//                                 &mut vid_ts, &mut aud_ts, &stream_config);
//         if let Err(r) = &r {
//             log::info!("Failed to send to source: {r:?}");
//         }
//         r?;                       // <-- ONE error ends the thread
//     }
//
// i.e. there is no counter, no tolerance and no EOS: the very first failed
// push propagates out of the closure and the frame-pump thread is gone. The
// shim encodes exactly that.
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
        // Pre-cd4b78b there was no back-pressure signal at all.
        PumpAction::Continue
    }

    fn on_error(&mut self, _detached: bool) -> PumpAction {
        // `r?` — the thread dies on the first error, whatever it was.
        self.consecutive_errors += 1;
        PumpAction::ExitAfterErrorEos
    }

    fn mark_eos_signaled(&mut self) {}

    fn consecutive_errors(&self) -> u32 {
        self.consecutive_errors
    }
}
