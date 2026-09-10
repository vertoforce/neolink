#!/bin/bash
# Fix E regression gates — stale-session reap (7fe3ef3) + zombie-client kick
# (fix9, d90ce64) — run in a throwaway container. No camera, no LAN: the
# live arms stand up a gst-rtsp-server on 127.0.0.1 with an ephemeral port and
# a videotestsrc factory mounted at /testcam/main (+ /testcam alias), then
# drive it with an in-process rtspsrc consumer.
#
# The tests themselves live in src/rtsp/gst/server.rs (`mod tests`) because
# neolink is a binary crate with no lib target.
#
# GATES (all measured, both arms in the same process):
#   claim 1  staleness arithmetic is in ms
#            - next_timeout_usec() returns ms-to-expiry incl. extra_timeout
#              (measured 34999 for set_timeout(30) + extra_timeout 5)
#            - fixed predicate: 0 wrong verdicts / 7 idle points
#            - pre-fix (ms vs us): 4 wrong / 7, all on HEALTHY sessions
#   claim 2  reap counts Removed, not Ref
#            - pool.filter(Remove) destroys 3 sessions, returns 0
#            - client.session_filter(Remove) detaches 1, returns 0
#   claim 3  close owner BEFORE pool-reaping
#            - reap-first: owning client unmatchable (0 owners), consumer
#              still connected after 4000 ms  = ZOMBIE
#            - close-then-reap: consumer disconnected in <1 ms
#   claim 4  kick is exact-path matched
#            - live matches(): alias media at /testcam returns Some(8) for
#              /testcam/main, /testcam/main2, /testcam/sub -> the naive
#              "any match" predicate over-kicks (5 hits vs 2 exact)
#            - kick(/testcam/main2) kills nobody; kick(/testcam/main) kills
#              only the /testcam/main consumer
#
# usage:
#   iso-test/sim/run_fix_e_tests.sh                 # fixed tree (this worktree)
#   iso-test/sim/run_fix_e_tests.sh --base <sha>    # A/B against a base commit
#
# env: CTR (container name, default neolink-srv-fixe), IMAGE (default
#      neolink:devbuild — the Dockerfile `build` stage, warm target/ cache).
set -euo pipefail

REPO="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
IMAGE="${IMAGE:-neolink:devbuild}"
MODE=fixed
BASE_SHA=""
if [ "${1:-}" = "--base" ]; then
  MODE=base
  BASE_SHA="${2:?--base needs a commit sha}"
fi
CTR="${CTR:-neolink-srv-fixe-$MODE}"

SRC="$REPO"
if [ "$MODE" = base ]; then
  SRC="$(mktemp -d)/neolink-base"
  mkdir -p "$SRC"
  git -C "$REPO" archive "$BASE_SHA" | tar -x -C "$SRC"   # read-only extract
  # The base tree has none of the fix-E code, so it cannot host the fixed
  # tests. Append the base-arm module instead: the same rig, driven only by
  # the mechanisms the base actually has (pool reap, no client close).
  cat "$REPO/iso-test/sim/base_arm_tests.rs" >> "$SRC/src/rtsp/gst/server.rs"
  echo "== base tree $BASE_SHA extracted to $SRC (+ iso-test/sim/base_arm_tests.rs) =="
fi

docker inspect "$CTR" >/dev/null 2>&1 || docker run -d --name "$CTR" "$IMAGE" sleep infinity >/dev/null
docker start "$CTR" >/dev/null 2>&1 || true

# The live arms need real elements: videotestsrc/rtpvrawpay/rtspsrc/fakesink.
# Without them every live test SKIPs (prints SKIP and returns green).
docker exec "$CTR" sh -c 'gst-inspect-1.0 rtpvrawpay >/dev/null 2>&1 || (apt-get update -qq && apt-get install -y -qq gstreamer1.0-plugins-base gstreamer1.0-plugins-good)' >/dev/null 2>&1 || true

docker exec "$CTR" sh -c 'rm -rf /usr/local/src/neolink/src /usr/local/src/neolink/crates'
docker cp -q "$SRC/src"    "$CTR:/usr/local/src/neolink/src"
docker cp -q "$SRC/crates" "$CTR:/usr/local/src/neolink/crates"
for f in Cargo.toml Cargo.lock build.rs rustfmt.toml; do
  [ -f "$SRC/$f" ] && docker cp -q "$SRC/$f" "$CTR:/usr/local/src/neolink/$f"
done
[ -d "$SRC/dissector" ] && docker cp -q "$SRC/dissector" "$CTR:/usr/local/src/neolink/dissector"

# --test-threads=1: the live arms bind a port and drive the process-wide
# default glib main context.
FILTER="rtsp::gst::server"
[ "$MODE" = base ] && FILTER="base_"
echo "== cargo test --release --bin neolink $FILTER (mode=$MODE, ctr=$CTR) =="
docker exec -w /usr/local/src/neolink "$CTR" \
  cargo test --release --bin neolink "$FILTER" -- --test-threads=1 --nocapture
