#!/bin/bash
# sc_prove_fix16.sh — the fix16 proof (pay0 egress-probe un-blinding +
# PLAYING gate on the starvation/egress exits), re-pointed from the production
# camera at iso-test/prove_fix16.sh onto the fake camera in
# iso-test/sim/fakecam. No real camera, no shared docker network.
#
# Usage: sc_prove_fix16.sh <image> [starve|paused|negative|all]  (default all)
#
# Differences from the prod harness, and why:
#   * Fault injection is `ctl.py freeze` on the fakecam instead of an iptables
#     DROP of the camera IP in the test container's netns. `freeze` stops
#     frames while keeping the BC session and the ping path alive, which is
#     exactly the "camera-frame starvation" input GATE S wants.
#   * GATE S needs a pipeline whose pay0 output is GstBufferLists, because the
#     defect fix16 fixes is a BUFFER-only pad probe that cannot see them.
#     The fakecam's stock 640x360 test pattern does NOT produce one: MEASURED
#     with the exact element chain neolink builds (h264parse ! rtph264pay),
#     the stock fixture gives 33 plain BUFFER pushes and 2 BUFFER_LISTs, so a
#     BUFFER-only probe is wide awake. SC_BIGFRAME=1 (default) generates a
#     1280x720 / 4 Mbit fixture with AUD+SEI stripped, which measures 60
#     BUFFER_LISTs to 8 plain BUFFERs (the 8 are the SPS/PPS pairs at each
#     IDR). That is as close to the prod "100% buffer lists" shape as an H.264
#     fixture can get: parameter-set NALs are always smaller than the RTP MTU
#     and are always pushed as plain buffers, so the BUFFER-only probe can
#     never be made to count exactly zero here. READ THE HARNESS OUTPUT WITH
#     THAT IN MIND -- see the header note printed at run time.
#
# GATE S (starvation positive): 3 consumers streaming, then freeze the camera.
#   Expected on the fixed image: "camera-frame starvation" at ~30s
#   (FRAME_STALENESS_MS), EOS, pump exit after the grace window, kick of the
#   attached clients -> all 3 consumers exit within ~75s.
# GATE P (PAUSED cached media must survive): a DESCRIBE-only RTSP client leaves
#   the shared media prepared+PAUSED in the factory cache. Wait 40s (> the 15s
#   egress-stall window), then a real consumer must stream from it with no
#   egress-stall / starvation exit in between.
# GATE N (negative): 3 consumers for NEG_SECS, one churned every 25s: 0 pump
#   exits, 0 kicks, flat THR/FD, clean cadence.
set -u
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/sc_lib.sh"

IMG="${1:-neolink:clean-master-test}"
MODE="${2:-all}"
TAG="${IMG##*:}"
CAMC="neolink-sc-cam16-${TAG}"
NEO="neolink-sc-neo16-${TAG}"
CPRE="neolink-sc-c16-${TAG}-"  # tag-scoped so both images can run concurrently
NEG_SECS="${NEG_SECS:-120}"
SC_BIGFRAME="${SC_BIGFRAME:-1}"
FIXDIR="${SC_FIXDIR:-/tmp/sc_fixtures}"

# Generates the buffer-list fixture described in the header. Documented command
# so the numbers above can be reproduced; nothing about it is camera-derived.
sc_make_bigframe(){
  mkdir -p "$FIXDIR"
  [ -s "$FIXDIR/bigframe.h264" ] && return 0
  docker run --rm -v "$FIXDIR":/out "$SC_FFIMG" \
    -f lavfi -i testsrc=size=1280x720:rate=15 -t 4 -threads 1 \
    -c:v libx264 -preset veryfast -profile:v baseline -pix_fmt yuv420p -bf 0 \
    -b:v 4000k -minrate 4000k -maxrate 4000k -bufsize 300k \
    -x264-params "repeat-headers=1:keyint=15:min-keyint=15:scenecut=0:slices=1:sliced-threads=0:threads=1:aud=0:nal-hrd=cbr" \
    -bsf:v "filter_units=remove_types=6|9|12" \
    -f h264 -y /out/bigframe.h264 >/dev/null 2>&1
}

# Counts BUFFER vs BUFFER_LIST pushes out of pay0 for a given Annex-B fixture,
# using the exact element chain build_h264() creates. This is the measurement
# that decides whether GATE S can isolate the fix16 defect at all.
sc_payloader_split(){
  local host_dir="$1" file="$2"
  docker run --rm --network none -v "$host_dir":/pl:ro --entrypoint sh "$IMG" -c \
    "GST_DEBUG=GST_SCHEDULING:5 gst-launch-1.0 -q filesrc location=/pl/$file ! h264parse ! rtph264pay name=pay0 ! fakesink name=fs 2>&1 \
     | sed 's/\x1b\[[0-9;]*m//g' | grep 'fs:sink' | grep -oE 'calling chain(list)?function' | sort | uniq -c | tr -s ' \n' ' '"
}

sc_build_fakecam_image || exit 1
sc_ensure_net
CAMARGS=()
if [ "$SC_BIGFRAME" = 1 ]; then
  sc_make_bigframe
  CAMARGS=(-v "$FIXDIR":/fix:ro)
fi
TOML="$(mktemp /tmp/sc_fix16.XXXXXX.toml)"
sc_write_toml "$TOML" "$CAMC"

docker rm -f "$CAMC" >/dev/null 2>&1
docker run -d --name "$CAMC" --network "$SC_NET" -e RUST_LOG="${SC_CAMLOG:-info}" "${CAMARGS[@]:-}" \
  "$SC_FAKECAM_IMG" --bind 0.0.0.0:9000 --control 0.0.0.0:9010 \
  --username "$SC_USER" --password "$SC_PASS" --fps "$SC_FPS" \
  $( [ "$SC_BIGFRAME" = 1 ] && echo "--h264 /fix/bigframe.h264 --width 1280 --height 720" ) >/dev/null || exit 1
for i in $(seq 1 30); do docker logs "$CAMC" 2>&1 | grep -q "control port on" && break; sleep 1; done

sc_start_neolink "$NEO" "$IMG" "$TOML" || { echo "neolink did not start"; exit 1; }
URL="$(sc_url "$NEO")"
NEOIP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$NEO")

consumer(){ sc_consumer "${CPRE}$1" "$URL" "$2"; }
count(){ docker logs "$NEO" 2>&1 | grep -cE "$1"; }
up_count(){ docker ps --filter "name=^${CPRE}$1" -q | wc -l; }
warm(){ consumer warm 90; for i in $(seq 1 40); do docker logs --since 45s "$NEO" 2>&1 | grep -q "New BufferPool" && break; sleep 1; done; sleep 5; }
# DESCRIBE-only client: raw RTSP over /dev/tcp from the host to the container IP.
describe_only(){
  exec 3<>"/dev/tcp/$NEOIP/8554" || return 1
  printf 'OPTIONS rtsp://%s:8554/%s/main RTSP/1.0\r\nCSeq: 1\r\n\r\n' "$NEOIP" "$SC_CAM" >&3
  printf 'DESCRIBE rtsp://%s:8554/%s/main RTSP/1.0\r\nCSeq: 2\r\nAccept: application/sdp\r\n\r\n' "$NEOIP" "$SC_CAM" >&3
  timeout 12 cat <&3 | grep -m2 -E "^RTSP/1.0|m=video" | tr '\r\n' '  '
  exec 3>&- 3<&-
}

echo "########## sc_fix16 PROOF image=$IMG cam=fakecam($CAMC) neolink_pid=$SC_NEO_PID mode=$MODE bigframe=$SC_BIGFRAME ##########"
echo "### pay0 push split for the fixture in use (BUFFER vs BUFFER_LIST, h264parse ! rtph264pay):"
if [ "$SC_BIGFRAME" = 1 ]; then
  echo "###   bigframe.h264 : $(sc_payloader_split "$FIXDIR" bigframe.h264)"
else
  echo "###   stock testpattern.h264 : $(sc_payloader_split "$SC_ROOT/iso-test/sim/fakecam/data" testpattern.h264)"
fi
echo "### A non-zero 'chainfunction' count means a BUFFER-only pad probe still counts egress here,"
echo "### so GATE S cannot isolate the BUFFER_LIST blindness on its own -- see the header comment."
sc_log "t0: $(sc_metrics)"

if [ "$MODE" = starve ] || [ "$MODE" = all ]; then
  sc_log "=== GATE S: 3 consumers, then freeze the camera; starvation exit + kick expected on the fixed image ==="
  warm
  consumer s1 300; consumer s2 300; consumer s3 300; sleep 12; docker rm -f "${CPRE}warm" >/dev/null 2>&1
  sc_log "GATE S consumers attached: up=$(up_count s)/3  $(sc_metrics)"
  ST0=$(count 'camera-frame starvation'); EG0=$(count 'egress stalled'); K0=$(count 'kicked'); EX0=$(count 'exiting frame-pump')
  sc_ctl "$CAMC" freeze >/dev/null; TB=$(date +%s); sc_log "GATE S: camera FROZEN"
  RESULT="no-exit-within-75s"
  for i in $(seq 1 75); do
    sleep 1
    if [ "$(up_count s)" -eq 0 ]; then RESULT="all 3 consumers exited $(( $(date +%s) - TB ))s after freeze"; break; fi
  done
  sc_log "GATE S RESULT: $RESULT; starvation_fires=$(( $(count 'camera-frame starvation') - ST0 )) egress_stall_fires=$(( $(count 'egress stalled') - EG0 )) kick_lines=$(( $(count 'kicked') - K0 )) pump_exits=$(( $(count 'exiting frame-pump') - EX0 )) consumers_up=$(up_count s)/3"
  docker logs --since 100s "$NEO" 2>&1 | grep -E "starvation|egress|kick|exiting frame-pump|declaring camera dead|stale-session" | cut -c1-160 | head -10
  sc_ctl "$CAMC" normal >/dev/null; sc_log "GATE S: camera UNFROZEN"; sc_cleanup_consumers "$CPRE"
  for i in $(seq 1 40); do docker logs --since 45s "$NEO" 2>&1 | grep -q "Connected and logged in" && break; sleep 1; done; sleep 5
  consumer rec 15; sleep 24; sc_log "GATE S recovery probe frames=$(sc_frames_of "${CPRE}rec")  $(sc_metrics)"; sc_cleanup_consumers "$CPRE"; sleep 8
fi

if [ "$MODE" = paused ] || [ "$MODE" = all ]; then
  sc_log "=== GATE P: DESCRIBE-only client leaves a PAUSED cached media; it must survive 40s and then serve a real consumer ==="
  warm; docker rm -f "${CPRE}warm" >/dev/null 2>&1; sleep 12   # media unprepared -> fresh build on next DESCRIBE
  EG0=$(count 'egress stalled'); ST0=$(count 'camera-frame starvation'); EX0=$(count 'exiting frame-pump|fast-exiting')
  sc_log "GATE P describe-only reply: $(describe_only)"
  sleep 40
  sc_log "GATE P after 40s: egress_stall_fires=$(( $(count 'egress stalled') - EG0 )) starvation_fires=$(( $(count 'camera-frame starvation') - ST0 )) pump_exits=$(( $(count 'exiting frame-pump|fast-exiting') - EX0 ))  $(sc_metrics)"
  consumer p1 15; sleep 24
  sc_log "GATE P RESULT: consumer frames=$(sc_frames_of "${CPRE}p1") (expect >0); egress_stall_fires=$(( $(count 'egress stalled') - EG0 )) pump_exits=$(( $(count 'exiting frame-pump|fast-exiting') - EX0 ))"
  sc_cleanup_consumers "$CPRE"; sleep 8
fi

if [ "$MODE" = negative ] || [ "$MODE" = all ]; then
  sc_log "=== GATE N: 3 consumers for ${NEG_SECS}s, consumer #3 churned every 25s ==="
  warm
  E0=$(count 'exiting frame-pump|fast-exiting'); K0=$(count kicked); EG0=$(count 'egress stalled'); ST0=$(count 'camera-frame starvation')
  sc_log "GATE N start (media warmed): $(sc_metrics)"
  consumer n1 $((NEG_SECS+90)); consumer n2 $((NEG_SECS+90)); consumer n3 $((NEG_SECS+90))
  sleep 10; docker rm -f "${CPRE}warm" >/dev/null 2>&1
  END=$(( $(date +%s) + NEG_SECS ))
  while [ "$(date +%s)" -lt "$END" ]; do
    sleep 25
    docker rm -f "${CPRE}n3" >/dev/null 2>&1; sleep 2; consumer n3 $((NEG_SECS+90))
    sc_log "GATE N: churned n3; $(sc_metrics)"
  done
  sc_log "GATE N cadence probe (60s ffprobe packets):"
  timeout -k 5 90 docker run --rm --name "${CPRE}cad" --network "$SC_NET" --entrypoint ffprobe "$SC_FFIMG" \
    -rtsp_transport tcp -i "$URL" -read_intervals "%+60" \
    -show_entries packet=stream_index,pts_time -of csv=p=0 2>/dev/null \
    | awk -F, '{ if ($1 in last) { g=$2-last[$1]; if (g>max[$1]) max[$1]=g } last[$1]=$2; n[$1]++ } END { for (s in n) printf "  stream %s: pkts=%d max_gap=%.3fs\n", s, n[s], max[s] }'
  docker rm -f "${CPRE}cad" >/dev/null 2>&1
  sc_log "GATE N RESULT: consumers_up=$(up_count n)/3 pump_exits=$(( $(count 'exiting frame-pump|fast-exiting') - E0 )) kicks=$(( $(count kicked) - K0 )) egress_stall=$(( $(count 'egress stalled') - EG0 )) starvation=$(( $(count 'camera-frame starvation') - ST0 )) $(sc_metrics)"
  sc_cleanup_consumers "$CPRE"
fi

sc_ctl "$CAMC" normal >/dev/null 2>&1
sc_cleanup_consumers "$CPRE"
sc_log "done; $NEO and $CAMC left running (docker rm -f $NEO $CAMC to clean up)"
echo "########## sc_fix16 PROOF COMPLETE ##########"
