// Appended ONLY to a pre-fix15 tree, so arm_orphan_pump.rs can run there
// unchanged. Both items encode what that tree does:
//
//   * the build task sends the bin and carries on to spawn the frame-pump
//     regardless of whether the send succeeded:
//         let _ = reply.send(element);
//         ... std::thread::spawn(move || { /* frame pump */ })
//   * the frame-pump has NO tick-side detach check at all — the fix 4
//     terminal test only runs inside the push path, so an empty media_rx
//     means it never runs.
#[cfg(test)]
#[derive(Debug, PartialEq, Eq)]
enum BuildDelivery {
    Delivered,
    #[allow(dead_code)]
    DiscardedRequesterGone,
}

#[cfg(test)]
fn deliver_build<T>(reply: std::sync::mpsc::SyncSender<T>, element: T) -> BuildDelivery {
    let _ = reply.send(element);
    BuildDelivery::Delivered
}

#[cfg(test)]
#[derive(Default, Debug)]
struct OrphanDetachedTicks;

#[cfg(test)]
impl OrphanDetachedTicks {
    fn observe(&mut self, _detached: bool) -> bool {
        false
    }
}
