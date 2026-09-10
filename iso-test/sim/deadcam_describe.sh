#!/bin/bash
# Dead-camera DESCRIBE behaviour, with no camera of any kind.
#
# The camera in the generated config points at 192.0.2.1 — RFC 5737 TEST-NET-1,
# which is not routable — and the container runs on an `--internal` docker
# network, so nothing leaves the host and no real device is contacted. The
# camera therefore never connects, which is exactly the state fix14
# (8269d71) addresses:
#
#   * base: a never-connected camera is served the dummy factory's "Stream not
#     Ready" splash (videotestsrc, 500 buffers = 20 s, then EOS), and each
#     DESCRIBE occupies the single shared glib main loop while it builds.
#   * fixed: the Rust DESCRIBE liveness gate fast-fails a camera that has been
#     never-connected for more than NEVER_CONNECTED_GRACE_MS (15 s).
#
# What it measures, for each image: per-DESCRIBE latency after the grace
# window, how many of them succeed, whether the neolink process survives a
# DESCRIBE/SETUP/PLAY hammer, and the process PID before and after (a SIGABRT
# changes it or kills the container).
#
# Usage: deadcam_describe.sh <image> [<image> ...]
# Prints one TSV row per image:
#   image  describes  ok  mean_ms  max_ms  hammer_attaches  aborts  survived
set -u
NET=neolink-sim-isolated
HAMMER_SECS="${HAMMER_SECS:-45}"
GRACE_WAIT="${GRACE_WAIT:-30}"   # > NEVER_CONNECTED_GRACE_MS (15 s) and > the 20 s splash EOS
DESCRIBES="${DESCRIBES:-10}"
HERE="$(cd "$(dirname "$0")" && pwd)"
HAMMER="${HAMMER:-$HERE/deadcam_hammer.py}"

docker network inspect "$NET" >/dev/null 2>&1 || docker network create --internal "$NET" >/dev/null

TOML=$(mktemp /tmp/deadcam-XXXX.toml)
cat > "$TOML" <<TOMLEOF
bind = "0.0.0.0"
bind_port = 8554

[[cameras]]
name = "testcam"
address = "192.0.2.1:9000"
username = "admin"
password = "password123"
stream = "main"
push_notifications = false
TOMLEOF

printf 'image\tdescribes\tok\tmean_ms\tmax_ms\thammer_attaches\taborts\tsurvived\n'

for IMG in "$@"; do
  NEO="neolink-deadcam-${IMG##*:}"
  docker rm -f "$NEO" >/dev/null 2>&1
  docker run -d --name "$NEO" --network "$NET" \
    -v "$TOML":/etc/neolink.toml:ro \
    -e RUST_LOG=neolink=info,neolink_core=info \
    "$IMG" neolink rtsp --config /etc/neolink.toml >/dev/null

  CPID=$(docker inspect -f '{{.State.Pid}}' "$NEO")
  NEOIP=$(docker inspect -f "{{(index .NetworkSettings.Networks \"$NET\").IPAddress}}" "$NEO")
  sleep "$GRACE_WAIT"
  PID_BEFORE=$(pgrep -P "$CPID" neolink | head -1)

  # Sequential DESCRIBEs from the host into the isolated network.
  read -r DESC OK MEAN MAX <<<"$(python3 - "$NEOIP" "$DESCRIBES" <<'PY'
import socket, sys, time
host, n = sys.argv[1], int(sys.argv[2])
url = f"rtsp://{host}:8554/testcam/main"
lat, ok = [], 0
for i in range(n):
    t0 = time.time()
    try:
        s = socket.create_connection((host, 8554), timeout=30)
        s.sendall(f"DESCRIBE {url} RTSP/1.0\r\nCSeq: {i}\r\nAccept: application/sdp\r\n\r\n".encode())
        s.settimeout(30)
        buf = b""
        while b"\r\n\r\n" not in buf:
            c = s.recv(4096)
            if not c:
                break
            buf += c
        if buf.startswith(b"RTSP/1.0 200"):
            ok += 1
        s.close()
    except Exception:
        pass
    lat.append((time.time() - t0) * 1000)
print(len(lat), ok, round(sum(lat)/len(lat), 1), round(max(lat), 1))
PY
)"

  HAM=$(python3 "$HAMMER" "$NEOIP" 8554 "rtsp://$NEOIP:8554/testcam/main" "$HAMMER_SECS" 2>/dev/null | tail -1)
  ATTACHES=$(sed -n 's/.*attaches=\([0-9]*\).*/\1/p' <<<"$HAM"); ATTACHES=${ATTACHES:-0}
  ABORTS=$(docker logs "$NEO" 2>&1 | grep -ci "assertion failed\|SIGABRT\|core dumped" || true)
  STATUS=$(docker inspect -f '{{.State.Status}}' "$NEO" 2>/dev/null || echo gone)
  PID_AFTER=$(pgrep -P "$CPID" neolink | head -1)
  SURVIVED=no
  [ "$STATUS" = running ] && [ -n "$PID_AFTER" ] && [ "$PID_AFTER" = "$PID_BEFORE" ] && SURVIVED=yes

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$IMG" "$DESC" "$OK" "$MEAN" "$MAX" "$ATTACHES" "$ABORTS" "$SURVIVED"
  docker rm -f "$NEO" >/dev/null 2>&1
done

rm -f "$TOML"

# ---------------------------------------------------------------------------
# MEASURED 2026-09-10, both images built from the same trixie / stock
# gst-rtsp-server 1.26.2 Dockerfile so that only the Rust differs:
#
#   image                    describes ok mean_ms max_ms hammer_attaches aborts survived
#   neolink:base-prs-test    10        4  11.6    59.1   17550           0      yes
#   neolink:clean-master-test 10       0  4.2     18.5   17781           0      yes
#
# base-prs-test = commit 8708608 (upstream master + PRs #373/#400/#399/#398),
# i.e. no fix14 DESCRIBE liveness gate.
#
# Reading:
#  * The gate works. A camera that has never connected is served a 200 OK by
#    the base 4 times out of 10 (the dummy factory's splash, until its EOS) and
#    0 times out of 10 with the gate, and answers in 4.2 ms mean / 18.5 ms max
#    instead of 11.6 / 59.1.
#  * The SIGABRT half of fix14 does NOT reproduce here, on either image:
#    ~17,500 DESCRIBE/SETUP/PLAY attaches each, 0 aborts, same PID throughout.
#    That is the expected result and not a failure of the test — the crash
#    vector was Debian bookworm's gst-rtsp-server 1.22 g_assert(FALSE) in
#    gst_rtsp_media_get_rates, and both images run stock 1.26.2, which carries
#    gstreamer MR !7731. The C-layer claim can only be A/B'd across library
#    versions, which is what iso-test/prove_upstream_gst.sh GATE A does.
#  * NOT MEASURED here: whether a DESCRIBE for a *healthy* camera still answers
#    while another camera is dead. That needs a second, live camera.
# ---------------------------------------------------------------------------
