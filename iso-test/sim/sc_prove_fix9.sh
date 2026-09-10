#!/bin/bash
# sc_prove_fix9.sh — the fix9 zombie-client-kick proof, re-pointed from the
# production camera at iso-test/prove_fix9.sh onto the fake camera in
# iso-test/sim/fakecam. Nothing here touches a real camera or a shared docker
# network: a fakecam container and the neolink container under test share an
# --internal network and every container is named neolink-sc-*.
#
# Usage: sc_prove_fix9.sh <image> [Z|N1|N2|all]     (default: all)
#
# Differences from the prod harness, and why:
#   * The camera is a fakecam container, not camera_d at a LAN address.
#   * GATE Z does not touch the camera at all in either version -- the stall it
#     needs is on the EGRESS side (a paused consumer), so no fault injection is
#     substituted. `SC_FREEZE_CAM=1` additionally freezes the camera mid-pause,
#     which adds the fix13 starvation path to the teardown; off by default so
#     the gate stays faithful to the prod script.
#   * The prod script attaches to an already-running `neolink-iso`; this one
#     starts its own pair so the A/B can run unattended on both images.
#
# GATE Z (zombie): paused-then-unpaused no-timeout consumer must EXIT within
#   ZOMBIE_EXIT_WAIT of unpause (fixed) instead of hanging (base control).
# GATE N1 (churn): 4 full drop/reconnect cycles -> fresh probe still streams,
#   FD/THR/SOCK flat (no kick-loop regression).
# GATE N2 (steady): 2 healthy consumers for 90s -> zero "kicked" log lines
#   (no false kick), both consumers stay up.
set -u
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/sc_lib.sh"

IMG="${1:-neolink:clean-master-test}"
MODE="${2:-all}"
TAG="${IMG##*:}"
CAMC="neolink-sc-cam9-${TAG}"
NEO="neolink-sc-neo9-${TAG}"
CPRE="neolink-sc-c9-${TAG}-"   # tag-scoped so both images can run concurrently
ZOMBIE_EXIT_WAIT="${ZOMBIE_EXIT_WAIT:-30}"
ZOMBIE_PAUSE_SECS="${ZOMBIE_PAUSE_SECS:-45}"  # pause window; must exceed frame-staleness(30s)+session-sweep(~30s) for a clean pre-unpause teardown

sc_build_fakecam_image || exit 1
sc_ensure_net
TOML="$(mktemp /tmp/sc_fix9.XXXXXX.toml)"
sc_write_toml "$TOML" "$CAMC"
sc_start_fakecam "$CAMC" || { echo "fakecam did not start"; exit 1; }
sc_start_neolink "$NEO" "$IMG" "$TOML" || { echo "neolink did not start"; exit 1; }
URL="$(sc_url "$NEO")"

echo "########## sc_fix9 PROOF image=$IMG cam=fakecam($CAMC) neolink_pid=$SC_NEO_PID mode=$MODE ##########"
sc_log "t0: $(sc_metrics)"

if [ "$MODE" = Z ] || [ "$MODE" = all ]; then
  sc_log "=== GATE Z: freeze consumer -> server-side teardown -> unpause -> must exit, not hang ==="
  docker rm -f ${CPRE}zb >/dev/null 2>&1
  # NO -timeout flag: models go2rtc (blocks on read forever unless peer closes).
  docker run -d --name ${CPRE}zb --network "$SC_NET" "$SC_FFIMG" \
    -rtsp_transport tcp -i "$URL" -t 600 -f null - >/dev/null 2>&1
  sleep 15
  sc_log "consumer attached (frames=$(sc_frames_of ${CPRE}zb)); pausing it (egress will stall)"
  docker pause ${CPRE}zb >/dev/null
  if [ -n "${SC_FREEZE_CAM:-}" ]; then sleep 10; sc_log "GATE Z: also freezing the camera -> $(sc_ctl "$CAMC" freeze)"; fi
  sleep "$ZOMBIE_PAUSE_SECS"
  sc_log "${ZOMBIE_PAUSE_SECS}s elapsed since pause; teardown/kick should have fired. Unpausing."
  docker unpause ${CPRE}zb >/dev/null
  EXITED=timeout
  for i in $(seq 1 "$ZOMBIE_EXIT_WAIT"); do
    ST=$(docker inspect -f '{{.State.Status}}' ${CPRE}zb 2>/dev/null || echo gone)
    if [ "$ST" != "running" ] && [ "$ST" != "paused" ]; then EXITED="${i}s (status=$ST)"; break; fi
    sleep 1
  done
  sc_log "GATE Z result: consumer after unpause -> exited=$EXITED (fixed expects exit; base hangs = 'timeout')"
  sc_log "GATE Z neolink log evidence:"
  docker logs --since 3m "$NEO" 2>&1 | grep -E "kicked|closed .*client|egress stalled|back-pressure|exiting frame-pump|stale-session|starvation" | cut -c1-170 | tail -12
  docker rm -f ${CPRE}zb >/dev/null 2>&1
  [ -n "${SC_FREEZE_CAM:-}" ] && sc_ctl "$CAMC" normal >/dev/null
  sleep 5
  sc_log "GATE Z recovery probe (10s): frames=$(sc_probe "$URL" 10)"
fi

if [ "$MODE" = N1 ] || [ "$MODE" = all ]; then
  sc_log "=== GATE N1: 4 full drop/reconnect cycles, FD/THR/SOCK must stay flat ==="
  sc_log "cycle0: $(sc_metrics)"
  for c in 1 2 3 4; do
    docker run -d --name "${CPRE}lk_$c" --network "$SC_NET" "$SC_FFIMG" \
      -rtsp_transport tcp -i "$URL" -t 8 -f null - >/dev/null 2>&1
    sleep 6
    docker kill "${CPRE}lk_$c" >/dev/null 2>&1; docker rm -f "${CPRE}lk_$c" >/dev/null 2>&1
    sleep 4
    sc_log "cycle$c: $(sc_metrics)"
  done
  sc_log "GATE N1 recovery probe (10s): frames=$(sc_probe "$URL" 10)"
fi

if [ "$MODE" = N2 ] || [ "$MODE" = all ]; then
  sc_log "=== GATE N2: 2 healthy consumers, 90s, zero kicks expected ==="
  for n in 1 2; do
    docker rm -f "${CPRE}ng_$n" >/dev/null 2>&1
    docker run -d --name "${CPRE}ng_$n" --network "$SC_NET" "$SC_FFIMG" \
      -rtsp_transport tcp -i "$URL" -t 120 -f null - >/dev/null 2>&1
  done
  sleep 90
  UP=$(docker ps --filter "name=^${CPRE}ng_" -q | wc -l)
  KICKS=$(docker logs --since 95s "$NEO" 2>&1 | grep -c "kicked")
  sc_log "GATE N2 result: consumers_up=$UP/2 kick_lines_last95s=$KICKS frames=$(sc_frames_of "${CPRE}ng_1")/$(sc_frames_of "${CPRE}ng_2") (expect 2 up, 0 kicks) $(sc_metrics)"
  sc_cleanup_consumers "$CPRE"
fi

sc_cleanup_consumers "$CPRE"
sc_log "done; $NEO and $CAMC left running (docker rm -f $NEO $CAMC to clean up)"
echo "########## sc_fix9 PROOF COMPLETE ##########"
