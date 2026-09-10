#!/bin/bash
# Does one unresponsive camera freeze DESCRIBE for every other camera?
#
# fix6 (3ddaff6): the single shared glib main-loop thread's create_element
# did an UNBOUNDED blocking recv waiting for the per-camera task to build its
# bin. A camera stuck mid-connect held that thread forever, so RTSP froze for
# ALL cameras (the 2026-06-18 all-camera CLOSE_WAIT wedge). The fix bounds the
# wait at BUILD_REPLY_TIMEOUT (8 s) and fails that one DESCRIBE cleanly.
#
# No camera and no BC protocol needed: the "unresponsive camera" is a TCP
# listener that accepts the connection and then never sends a byte, which is
# what a camera whose uplink has collapsed looks like. Everything runs on an
# --internal docker network.
#
# Usage: hung_camera_describe.sh <image> [<image> ...]
# TSV per image:
#   image  hung_ms  hung_result  other_ms  other_result
# `*_ms` is how long the DESCRIBE took; CLIENT_TIMEOUT bounds it, so a value at
# the timeout with result=timeout means "never answered".
set -u
NET=neolink-sim-isolated
CLIENT_TIMEOUT="${CLIENT_TIMEOUT:-45}"
# Probe early, inside NEVER_CONNECTED_GRACE_MS (15 s), so the fix14 liveness
# gate is not yet what is being measured.
PROBE_AT="${PROBE_AT:-6}"

docker network inspect "$NET" >/dev/null 2>&1 || docker network create --internal "$NET" >/dev/null

docker rm -f neolink-sim-blackhole >/dev/null 2>&1
docker run -d --name neolink-sim-blackhole --network "$NET" python:3.12-slim \
  python -c "
import socket
s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(('0.0.0.0', 9000)); s.listen(64)
held = []
while True:
    c, _ = s.accept(); held.append(c)   # accept, then never say anything
" >/dev/null
BH=$(docker inspect -f "{{(index .NetworkSettings.Networks \"$NET\").IPAddress}}" neolink-sim-blackhole)

TOML=$(mktemp /tmp/hungcam-XXXX.toml)
cat > "$TOML" <<TOMLEOF
bind = "0.0.0.0"
bind_port = 8554

[[cameras]]
name = "hungcam"
address = "$BH:9000"
username = "admin"
password = "password123"
stream = "main"
push_notifications = false

[[cameras]]
name = "othercam"
address = "192.0.2.1:9000"
username = "admin"
password = "password123"
stream = "main"
push_notifications = false
TOMLEOF

printf 'image\thung_ms\thung_result\tother_ms\tother_result\n'
for IMG in "$@"; do
  NEO="neolink-hungcam-${IMG##*:}"
  docker rm -f "$NEO" >/dev/null 2>&1
  docker run -d --name "$NEO" --network "$NET" \
    -v "$TOML":/etc/neolink.toml:ro \
    -e RUST_LOG=neolink=info,neolink_core=info \
    "$IMG" neolink rtsp --config /etc/neolink.toml >/dev/null
  NEOIP=$(docker inspect -f "{{(index .NetworkSettings.Networks \"$NET\").IPAddress}}" "$NEO")
  sleep "$PROBE_AT"

  python3 - "$NEOIP" "$CLIENT_TIMEOUT" "$IMG" <<'PY'
import socket, sys, threading, time
host, timeout, img = sys.argv[1], float(sys.argv[2]), sys.argv[3]
results = {}

def describe(cam):
    url = f"rtsp://{host}:8554/{cam}/main"
    t0 = time.time()
    try:
        s = socket.create_connection((host, 8554), timeout=timeout)
        s.settimeout(timeout)
        s.sendall(f"DESCRIBE {url} RTSP/1.0\r\nCSeq: 1\r\nAccept: application/sdp\r\n\r\n".encode())
        buf = b""
        while b"\r\n\r\n" not in buf:
            c = s.recv(4096)
            if not c:
                results[cam] = (time.time() - t0, "eof"); return
            buf += c
        code = buf.split(b" ")[1].decode(errors="replace")
        results[cam] = (time.time() - t0, f"rtsp{code}")
        s.close()
    except socket.timeout:
        results[cam] = (time.time() - t0, "timeout")
    except Exception as e:
        results[cam] = (time.time() - t0, type(e).__name__)

# The hung camera's DESCRIBE goes first and is still in flight when the other
# camera's DESCRIBE arrives -- that overlap is the whole point.
hung = threading.Thread(target=describe, args=("hungcam",))
hung.start()
time.sleep(1.0)
other = threading.Thread(target=describe, args=("othercam",))
other.start()
hung.join(); other.join()
h, o = results["hungcam"], results["othercam"]
print(f"{img}\t{round(h[0]*1000)}\t{h[1]}\t{round(o[0]*1000)}\t{o[1]}")
PY
  docker rm -f "$NEO" >/dev/null 2>&1
done
docker rm -f neolink-sim-blackhole >/dev/null 2>&1
rm -f "$TOML"

# ---------------------------------------------------------------------------
# MEASURED 2026-09-10 — this scenario does NOT reproduce the fix6 defect:
#
#   image                     hung_ms hung_result other_ms other_result
#   neolink:base-prs-test     42      rtsp200     20       rtsp200
#   neolink:clean-master-test 44      rtsp200     23       rtsp200
#
# Both images answer both DESCRIBEs in tens of milliseconds. A camera that only
# ever gets as far as a TCP accept is never *connected*, so its DESCRIBE is
# served by the dummy factory's splash without going anywhere near the
# create_element path whose unbounded reply wait fix6 bounds. The blocking
# state needs a camera that connected, logged in, and then stopped answering
# mid-build — which is what iso-test/sim/fakecam's `freeze` and `hang` control
# modes provide, and what the sc_prove_* harnesses drive.
#
# Kept as the negative control for that scenario: "a camera stuck at TCP
# accept" is NOT the fix6 trigger, on either tree. The bounded wait itself is
# measured directly by iso-test/sim/arm_build_wait.rs (base: shared loop still
# parked at 11 s, second camera never served; fixed: 8.000103 s then the second
# camera served 1.08 us later).
# ---------------------------------------------------------------------------
