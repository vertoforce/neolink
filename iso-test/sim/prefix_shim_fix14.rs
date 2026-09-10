// Appended ONLY to a pre-fix14 tree, so arm_liveness_gate.rs can run there
// unchanged. That tree has NO DESCRIBE liveness gate at all: create_element
// (and make_dummy_factory's callback) run unconditionally for every camera,
// so nothing is ever treated as dead.
#[cfg(test)]
fn liveness_gate_dead(
    _last_frame_at: &Arc<AtomicU64>,
    _armed_at: &std::time::Instant,
) -> bool {
    false
}
