// Appended ONLY to a pre-fix16 tree, so arm_egress_probe.rs can run there
// unchanged. Both items are the pre-fix16 egress probe lifted verbatim from
// that tree's inline `srcpad.add_probe(...)` call in make_factory:
//
//     let _ = srcpad.add_probe(
//         gstreamer::PadProbeType::BUFFER,
//         move |_pad, _info| {
//             egress_count_probe.fetch_add(1, Ordering::Relaxed);
//             gstreamer::PadProbeReturn::Ok
//         },
//     );
#[cfg(test)]
fn egress_probe_mask() -> gstreamer::PadProbeType {
    gstreamer::PadProbeType::BUFFER
}

#[cfg(test)]
fn egress_packet_count(_data: Option<&gstreamer::PadProbeData>) -> u64 {
    1
}

// Pre-fix16 there is no PLAYING gate at all: the egress-stall and fix 13
// starvation exits fire on staleness alone, whatever state the media is in.
#[cfg(test)]
fn pay0_playing(_pay0: Option<&Element>) -> bool {
    true
}
