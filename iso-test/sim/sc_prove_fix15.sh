#!/bin/bash
# sc_prove_fix15.sh — the fix15 orphan frame-pump leak proof, re-pointed
# from the production camera at iso-test/prove_fix15.sh onto the fake camera
# in iso-test/sim/fakecam. No real camera, no shared docker network: a fakecam
# container and the neolink container under test share an --internal network
# and every container is named neolink-sc-*.
#
# Usage: sc_prove_fix15.sh <image> [orphan|negative|both]   (default: both)
#
# Differences from the prod harness, and why:
#   * Fault injection is `ctl.py freeze` on the fakecam instead of an iptables
#     DROP of the camera IP inside the test container's netns. `freeze` is the
#     closest analogue: video frames stop while the BC/TCP connection and the
#     ping path stay alive, so a pipeline build parks in its learn phase
#     waiting for a first frame -- which is the thing GATE O needs. An iptables
#     DROP also kills the pings; `die` is the analogue of that and is NOT used
#     here because it lets the client reconnect immediately.
#   * There is no audio in the fakecam, so the Defect-1 side of GATE N ("audio
#     stalled" must never appear) is vacuous here and is reported as such.
#
# GATE O (orphan repro). Camera frozen -> consumers dropped -> 3 DESCRIBEs
#   during the stall -> unfreeze -> healthy consumer -> measure the neolink
#   process: Threads / FDs / socket FDs / VmRSS. PASS = flat across cycles.
# GATE N (negative). 3 healthy consumers for NEG_SECS with one churned every
#   25s: expect ZERO pump exits, zero kicks, flat THR/FD and a clean cadence.
set -u
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/sc_lib.sh"

IMG="${1:-neolink:clean-master-test}"
MODE="${2:-both}"
TAG="${IMG##*:}"
CAMC="neolink-sc-cam15-${TAG}"
NEO="neolink-sc-neo15-${TAG}"
CPRE="neolink-sc-c15-${TAG}-"  # tag-scoped so both images can run concurrently
CYCLES="${CYCLES:-4}"
NEG_SECS="${NEG_SECS:-120}"

sc_build_fakecam_image || exit 1
sc_ensure_net
TOML="$(mktemp /tmp/sc_fix15.XXXXXX.toml)"
sc_write_toml "$TOML" "$CAMC"
sc_start_fakecam "$CAMC" || { echo "fakecam did not start"; exit 1; }
sc_start_neolink "$NEO" "$IMG" "$TOML" || { echo "neolink did not start"; exit 1; }
URL="$(sc_url "$NEO")"

consumer(){ sc_consumer "${CPRE}$1" "$URL" "$2" -timeout 8000000; }
count(){ docker logs "$NEO" 2>&1 | grep -cE "$1"; }
# NOTE (inherited from the prod harness): ffmpeg's -timeout never fires on a
# frame-starved neolink session -- gst-rtsp-server keeps sending RTCP over the
# interleaved TCP channel, so the socket is never idle. Bound the probe
# externally with `timeout -k` and a named container that can be force-removed.
describe_probe(){
  local n="${CPRE}probe_$RANDOM"
  timeout -k 3 14 docker run --rm --name "$n" --network "$SC_NET" --entrypoint ffprobe "$SC_FFIMG" \
    -rtsp_transport tcp -i "$URL" >/dev/null 2>&1
  local rc=$?
  docker rm -f "$n" >/dev/null 2>&1
  echo "describe_rc=$rc"
}

echo "########## sc_fix15 PROOF image=$IMG cam=fakecam($CAMC) neolink_pid=$SC_NEO_PID mode=$MODE ##########"
sc_log "t0 (camera connected): $(sc_metrics)  wchan: $(sc_wchan)"
sc_log "warm-up consumer 15s"
consumer warm 15; sleep 22; sc_cleanup_consumers "$CPRE"; sleep 8
sc_log "post-warmup idle: $(sc_metrics)  pump_exits=$(count 'exiting frame-pump|fast-exiting')"

if [ "$MODE" = orphan ] || [ "$MODE" = both ]; then
  sc_log "=== GATE O: $CYCLES x (freeze camera, 3 DESCRIBEs during the stall, unfreeze, healthy consumer, measure) ==="
  for c in $(seq 1 "$CYCLES"); do
    T0=$(count 'pipeline build did not reply')
    consumer pre_$c 120                    # a live pump exists when the hiccup starts (prod shape)
    sleep 10
    sc_ctl "$CAMC" freeze >/dev/null; sc_log "cycle$c: camera FROZEN (frames stop, BC session alive)"
    # Prod shape: the serving pump had already exited and the media was
    # unprepared before the reconnect DESCRIBEs arrived. A still-attached
    # consumer would be served from the cached prepared media (no
    # create_element at all), so drop it now to force real builds.
    sc_cleanup_consumers "$CPRE"
    sleep 3
    for p in 1 2 3; do sc_log "cycle$c: DESCRIBE $p -> $(describe_probe)"; sleep 2; done
    sleep 6
    sc_ctl "$CAMC" normal >/dev/null
    sc_log "cycle$c: camera UNFROZEN; build timeouts this cycle: $(( $(count 'pipeline build did not reply') - T0 ))"
    for i in $(seq 1 40); do
      docker logs --since 45s "$NEO" 2>&1 | grep -q "Connected and logged in" && break; sleep 1
    done
    sleep 5
    consumer post_$c 15; sleep 22; sc_cleanup_consumers "$CPRE"; sleep 10
    sc_log "cycle$c: $(sc_metrics)  timeouts_total=$(count 'pipeline build did not reply') discard15=$(count 'not spawning a frame-pump') orphan15=$(count 'orphan pipeline') detach_exits=$(count 'fast-exiting') channel_closed=$(count 'frame channel closed')"
  done
  sc_log "GATE O: waiting 75s idle so any orphan guard can fire, then final measure"
  sleep 75
  sc_log "GATE O FINAL: $(sc_metrics)  timeouts_total=$(count 'pipeline build did not reply') discard15=$(count 'not spawning a frame-pump') orphan15=$(count 'orphan pipeline') detach_exits=$(count 'fast-exiting') channel_closed=$(count 'frame channel closed')"
  sc_log "GATE O: thread wchan distribution (hrtimer_nanosleep = frame-pump threads): $(sc_wchan)"
fi

if [ "$MODE" = negative ] || [ "$MODE" = both ]; then
  sc_log "=== GATE N: 3 consumers for ${NEG_SECS}s, consumer #3 killed+restarted every 25s ==="
  consumer warm2 90
  for i in $(seq 1 40); do docker logs --since 45s "$NEO" 2>&1 | grep -q "New BufferPool" && break; sleep 1; done
  sleep 5
  E0=$(count 'exiting frame-pump|fast-exiting'); K0=$(count kicked); A0=$(count 'audio stalled')
  sc_log "GATE N start (media warmed): $(sc_metrics)"
  consumer n1 $((NEG_SECS+30)); consumer n2 $((NEG_SECS+30)); consumer n3 $((NEG_SECS+30))
  sleep 10; docker rm -f "${CPRE}warm2" >/dev/null 2>&1
  END=$(( $(date +%s) + NEG_SECS ))
  while [ "$(date +%s)" -lt "$END" ]; do
    sleep 25
    docker rm -f "${CPRE}n3" >/dev/null 2>&1; sleep 2; consumer n3 $((NEG_SECS+30))
    sc_log "GATE N: churned n3; $(sc_metrics)"
  done
  sc_log "GATE N cadence probe (60s ffprobe packets):"
  timeout -k 5 90 docker run --rm --name "${CPRE}cad" --network "$SC_NET" --entrypoint ffprobe "$SC_FFIMG" \
    -rtsp_transport tcp -timeout 10000000 -i "$URL" -read_intervals "%+60" \
    -show_entries packet=stream_index,pts_time -of csv=p=0 2>/dev/null \
    | awk -F, '{ if ($1 in last) { g=$2-last[$1]; if (g>max[$1]) max[$1]=g } last[$1]=$2; n[$1]++ } END { for (s in n) printf "  stream %s: pkts=%d max_gap=%.3fs\n", s, n[s], max[s] }'
  docker rm -f "${CPRE}cad" >/dev/null 2>&1
  UP=$(docker ps --filter "name=^${CPRE}n" -q | wc -l)
  sc_log "GATE N RESULT: consumers_up=$UP/3 pump_exits=$(( $(count 'exiting frame-pump|fast-exiting') - E0 )) kicks=$(( $(count kicked) - K0 )) audio_stalled=$(( $(count 'audio stalled') - A0 ))[vacuous: fakecam has no audio] orphan15=$(count 'orphan pipeline') $(sc_metrics)  wchan: $(sc_wchan)"
  sc_cleanup_consumers "$CPRE"
fi

sc_ctl "$CAMC" normal >/dev/null 2>&1
sc_cleanup_consumers "$CPRE"
sc_log "done; $NEO and $CAMC left running (docker rm -f $NEO $CAMC to clean up)"
echo "########## sc_fix15 PROOF COMPLETE ##########"
