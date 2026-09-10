#!/bin/bash
# Shared plumbing for the sc_* scenario harnesses: build the fakecam image,
# stand a fakecam + neolink pair up on an isolated docker network, inject
# faults through the fakecam control port, and read the neolink process's
# /proc counters from the host.
#
# Everything it creates is named neolink-sc-* and lives on an --internal
# network, so nothing here can reach anything off the host.
#
# Source it, do not run it:  . "$(dirname "$0")/sc_lib.sh"
set -u

SC_NET="${SC_NET:-neolink-sim-isolated}"
SC_FAKECAM_IMG="${SC_FAKECAM_IMG:-neolink-sc-fakecam:latest}"
SC_BUILDER_IMG="${SC_BUILDER_IMG:-neolink:devbuild}"
SC_FFIMG="${SC_FFIMG:-jrottenberg/ffmpeg:6.1-ubuntu}"
SC_CAM="${SC_CAM:-testcam}"
SC_USER="${SC_USER:-admin}"
SC_PASS="${SC_PASS:-password123}"
SC_FPS="${SC_FPS:-15}"
# Repo root, derived from this file's location (iso-test/sim/sc_lib.sh).
SC_ROOT="${SC_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"

sc_log(){ echo "[$(date -u +%H:%M:%S)] $*"; }

# ---------------------------------------------------------------- image build
# Compiles iso-test/sim/fakecam in $SC_BUILDER_IMG (needs a Rust toolchain and
# the repo mounted), stages the binary + data/ + ctl.py, and builds
# $SC_FAKECAM_IMG from sc_fakecam.Dockerfile. Skipped if the image exists and
# SC_FORCE_BUILD is unset.
sc_build_fakecam_image(){
  if [ -z "${SC_FORCE_BUILD:-}" ] && docker image inspect "$SC_FAKECAM_IMG" >/dev/null 2>&1; then
    sc_log "fakecam image $SC_FAKECAM_IMG already present"; return 0
  fi
  local bin="$SC_ROOT/target/sc-fakecam/release/fakecam"
  if [ -n "${SC_FORCE_BUILD:-}" ] || [ ! -x "$bin" ]; then
    sc_log "building fakecam in $SC_BUILDER_IMG"
    docker run --rm -v "$SC_ROOT":/src -w /src -e CARGO_TARGET_DIR=/src/target/sc-fakecam \
      --entrypoint sh "$SC_BUILDER_IMG" \
      -c 'cargo build --release --manifest-path iso-test/sim/fakecam/Cargo.toml' >/dev/null || return 1
  fi
  local stage; stage=$(mktemp -d)
  cp "$bin" "$stage/fakecam"
  cp "$SC_ROOT/iso-test/sim/fakecam/ctl.py" "$stage/ctl.py"
  cp -r "$SC_ROOT/iso-test/sim/fakecam/data" "$stage/data"
  cp "$SC_ROOT/iso-test/sim/sc_fakecam.Dockerfile" "$stage/Dockerfile"
  docker build -q -t "$SC_FAKECAM_IMG" "$stage" >/dev/null || { rm -rf "$stage"; return 1; }
  rm -rf "$stage"
  sc_log "built $SC_FAKECAM_IMG"
}

sc_ensure_net(){
  docker network inspect "$SC_NET" >/dev/null 2>&1 || \
    docker network create --internal "$SC_NET" >/dev/null
}

# --------------------------------------------------------------- the fake cam
# sc_start_fakecam <name> [extra fakecam args...]
sc_start_fakecam(){
  local name="$1"; shift
  docker rm -f "$name" >/dev/null 2>&1
  docker run -d --name "$name" --network "$SC_NET" -e RUST_LOG="${SC_CAMLOG:-info}" \
    "$SC_FAKECAM_IMG" --bind 0.0.0.0:9000 --control 0.0.0.0:9010 \
    --username "$SC_USER" --password "$SC_PASS" --fps "$SC_FPS" "$@" >/dev/null || return 1
  local i
  for i in $(seq 1 30); do
    docker logs "$name" 2>&1 | grep -q "control port on" && return 0
    sleep 1
  done
  docker logs "$name" 2>&1 | tail -5
  return 1
}

# sc_ctl <fakecam-name> <cmd> [cmd...]   e.g. sc_ctl cam freeze / sc_ctl cam 10,normal
sc_ctl(){ local n="$1"; shift; docker exec "$n" python3 /usr/local/bin/ctl.py 127.0.0.1:9010 "$@" 2>&1 | tail -1; }

# ----------------------------------------------------------------- neolink
# Writes a throwaway config pointing at the fakecam container. Nothing in it is
# real: container hostname, generic camera name, throwaway password.
sc_write_toml(){
  local path="$1" camhost="$2"
  cat > "$path" <<EOF
bind = "0.0.0.0"
bind_port = 8554

[[cameras]]
name = "$SC_CAM"
address = "$camhost:9000"
username = "$SC_USER"
password = "$SC_PASS"
stream = "main"
discovery = "none"
max_encryption = "bcencrypt"
EOF
}

# sc_start_neolink <name> <image> <toml-on-host>
# Exports SC_NEO_CPID (container init pid) and SC_NEO_PID (the neolink process).
sc_start_neolink(){
  local name="$1" img="$2" toml="$3"
  docker rm -f "$name" >/dev/null 2>&1
  docker run -d --name "$name" --network "$SC_NET" \
    -v "$toml":/etc/neolink.toml:ro \
    -e RUST_LOG="${SC_NEOLOG:-neolink=info,neolink_core=info}" \
    "$img" neolink rtsp --config /etc/neolink.toml >/dev/null || return 1
  local i
  for i in $(seq 1 60); do
    docker logs "$name" 2>&1 | grep -q "${SC_CAM}: Connected and logged in" && break
    sleep 1
  done
  SC_NEO_CPID=$(docker inspect -f '{{.State.Pid}}' "$name")
  SC_NEO_PID=$(pgrep -P "$SC_NEO_CPID" neolink | head -1)
  [ -n "$SC_NEO_PID" ] || { docker logs "$name" 2>&1 | tail -5; return 1; }
  export SC_NEO_CPID SC_NEO_PID
  return 0
}

# ----------------------------------------------------------------- /proc
sc_thr(){ grep Threads "/proc/${SC_NEO_PID}/status" 2>/dev/null | awk '{print $2}'; }
sc_fd(){ ls "/proc/${SC_NEO_PID}/fd" 2>/dev/null | wc -l; }
sc_sock(){ ls -l "/proc/${SC_NEO_PID}/fd" 2>/dev/null | grep -c socket; }
sc_rss(){ grep VmRSS "/proc/${SC_NEO_PID}/status" 2>/dev/null | awk '{print $2}'; }
sc_metrics(){ echo "THR=$(sc_thr) FD=$(sc_fd) SOCK=$(sc_sock) RSS_kB=$(sc_rss)"; }
sc_wchan(){ local t; for t in /proc/${SC_NEO_PID}/task/*; do cat "$t/wchan" 2>/dev/null; echo; done \
  | sort | uniq -c | sort -rn | head -4 | tr -s ' \n' ' '; }

# ----------------------------------------------------------------- consumers
sc_url(){ echo "rtsp://$1:8554/${SC_CAM}/main"; }
# sc_consumer <name> <url> <secs> [extra input opts...]  -- detached ffmpeg
sc_consumer(){
  local n="$1" url="$2" secs="$3"; shift 3
  docker rm -f "$n" >/dev/null 2>&1
  docker run -d --name "$n" --network "$SC_NET" "$SC_FFIMG" \
    -rtsp_transport tcp "$@" -i "$url" -t "$secs" -f null - >/dev/null 2>&1
}
sc_frames_of(){ docker logs "$1" 2>&1 | tr '\r' '\n' | grep -oE 'frame= *[0-9]+' | tail -1 | grep -oE '[0-9]+' | tail -1 || echo 0; }
# sc_probe <url> <secs> -- blocking ffmpeg, echoes the frame count it managed
sc_probe(){
  local url="$1" secs="$2" n="neolink-sc-probe-$RANDOM"
  timeout -k 5 $((secs + 20)) docker run --rm --name "$n" --network "$SC_NET" "$SC_FFIMG" \
    -rtsp_transport tcp -i "$url" -t "$secs" -f null - 2>&1 \
    | tr '\r' '\n' | grep -oE 'frame= *[0-9]+' | tail -1 | grep -oE '[0-9]+' | tail -1 || echo 0
  docker rm -f "$n" >/dev/null 2>&1
}
sc_count(){ docker logs "$2" 2>&1 | grep -cE "$1"; }
sc_cleanup_consumers(){ docker ps -aq --filter "name=^$1" | xargs -r docker rm -f >/dev/null 2>&1; }
