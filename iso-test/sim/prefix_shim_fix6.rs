// Appended ONLY to a pre-fix6 tree, so arm_build_wait.rs can run there
// unchanged. This is that tree's create_element wait:
//
//     let (reply, new_element) = tokio::sync::oneshot::channel();
//     ...
//     let element = new_element.blocking_recv()?;      // UNBOUNDED
//
// (std mpsc here instead of tokio oneshot purely for signature
// compatibility; both are an unbounded blocking receive on the shared glib
// main-loop thread, which is the defect.)
#[cfg(test)]
const BUILD_REPLY_TIMEOUT: Duration = Duration::from_secs(8);

#[cfg(test)]
fn await_build_reply<T>(
    rx: &std::sync::mpsc::Receiver<T>,
    _timeout: Duration,
) -> Result<T, std::sync::mpsc::RecvTimeoutError> {
    rx.recv()
        .map_err(|_| std::sync::mpsc::RecvTimeoutError::Disconnected)
}
