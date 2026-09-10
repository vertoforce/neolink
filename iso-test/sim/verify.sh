#!/bin/bash
# iso-test/sim/verify.sh — one entrypoint for verifying this fork.
#
# Tiers:
#   --quick    the unit tier only: cargo test --release --workspace, run in a
#              container built from this repo's Dockerfile build stage.
#   (default)  the unit tier, then the container scenarios that drive a fake
#              camera into each wedge, then the unit-level A/B arms that run
#              the same test against a tree with the fix and a tree without.
#
# usage: verify.sh [--quick] [--image <neolink image>] [--keep]
#
#   --image   skip building the release image and test the given one instead.
#   --keep    leave the containers and logs behind for inspection.
#
# Everything it starts is named neolink-verify-* or neolink-sc-* and lives on
# an --internal docker network. Nothing here contacts a real camera.
#
# Verdicts: the unit tier and the A/B arms report themselves, so they are a
# real PASS or FAIL. Two container scenarios carry a decisive log line and are
# scored on it. The rest print measurements a human reads, so they are marked
# REVIEW with the path to their log rather than being scored automatically.
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SIM="$REPO/iso-test/sim"
QUICK=0
KEEP=0
IMAGE=""
BUILD_IMG="${VERIFY_BUILD_IMAGE:-neolink:verify-build}"
LOGDIR="${VERIFY_LOGDIR:-$REPO/target/verify-logs}"

while [ $# -gt 0 ]; do
  case "$1" in
    --quick) QUICK=1 ;;
    --keep)  KEEP=1 ;;
    --image) IMAGE="${2:?--image needs an image}"; shift ;;
    -h|--help) sed -n '2,20p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
done

mkdir -p "$LOGDIR"
RESULTS=()
record(){ RESULTS+=("$1|$2|$3"); }
log(){ echo "[$(date -u +%H:%M:%S)] $*"; }

cleanup(){
  [ "$KEEP" = 1 ] && { log "keeping containers and $LOGDIR"; return; }
  local ids
  ids="$(docker ps -aq --filter 'name=^neolink-verify-' --filter 'name=^neolink-sc-' 2>/dev/null)"
  [ -n "$ids" ] && docker rm -f $ids >/dev/null 2>&1
  return 0
}
trap cleanup EXIT INT TERM

# ------------------------------------------------------------------ unit tier
build_stage_image(){
  if docker image inspect "$BUILD_IMG" >/dev/null 2>&1; then
    log "build image $BUILD_IMG already present"; return 0
  fi
  log "building $BUILD_IMG (Dockerfile build stage)"
  docker build -f "$REPO/Dockerfile" --target build -t "$BUILD_IMG" "$REPO" \
    > "$LOGDIR/build-stage.log" 2>&1
}

unit_tier(){
  local out="$LOGDIR/cargo-test.log" rc passed
  build_stage_image || { record "build image" FAIL "$LOGDIR/build-stage.log"; return 1; }
  log "cargo test --release --workspace"
  docker run --rm -w /usr/local/src/neolink "$BUILD_IMG" \
    cargo test --release --workspace > "$out" 2>&1
  rc=$?
  passed="$(grep -oE 'test result: ok\. [0-9]+ passed' "$out" \
            | grep -oE '^[0-9]+|[0-9]+ passed' | grep -oE '[0-9]+' \
            | awk '{n+=$1} END{print n+0}')"
  if [ "$rc" -eq 0 ]; then
    record "cargo test --release --workspace" PASS "${passed:-?} tests"
  else
    record "cargo test --release --workspace" FAIL "$out"
  fi
  return $rc
}

# ------------------------------------------------------------- container tier
release_image(){
  if [ -n "$IMAGE" ]; then echo "$IMAGE"; return 0; fi
  local img="neolink:verify"
  if ! docker image inspect "$img" >/dev/null 2>&1; then
    log "building the release image $img"
    docker build -f "$REPO/Dockerfile" -t "$img" "$REPO" \
      > "$LOGDIR/build-release.log" 2>&1 || return 1
  fi
  echo "$img"
}

# scenario <name> <script> <args...>; scored on the markers below when present
scenario(){
  local name="$1" script="$2"; shift 2
  local out="$LOGDIR/$name.log"
  log "scenario $name"
  ( cd "$REPO" && SC_BUILDER_IMG="$BUILD_IMG" "$SIM/$script" "$@" ) > "$out" 2>&1
  local rc=$?
  if [ $rc -ne 0 ]; then record "$name" FAIL "$out"; return; fi
  case "$name" in
    zombie-kick)
      if grep -q 'exited=timeout' "$out"; then record "$name" FAIL "consumer hung: $out"
      elif grep -qE 'exited=[0-9]+s' "$out"; then record "$name" PASS "consumer closed"
      else record "$name" REVIEW "$out"; fi ;;
    starvation-exit)
      if grep -q 'RESULT: no-exit-within' "$out"; then record "$name" FAIL "no teardown: $out"
      elif grep -q 'consumers exited' "$out"; then record "$name" PASS "teardown observed"
      else record "$name" REVIEW "$out"; fi ;;
    *) record "$name" REVIEW "$out" ;;
  esac
}

# ab <name> <driver>; each arm reports CONFIRMED / UNCONFIRMED / BROKEN
ab(){
  local name="$1" driver="$2"
  local out="$LOGDIR/$name.log"
  log "A/B $name"
  ( cd "$REPO" && AB_IMAGE="$BUILD_IMG" "$SIM/$driver" all "neolink-verify-${name}" ) \
    > "$out" 2>&1
  local confirmed broken unconfirmed
  confirmed=$(grep -c ': CONFIRMED' "$out")
  broken=$(grep -c ': BROKEN' "$out")
  unconfirmed=$(grep -c ': UNCONFIRMED' "$out")
  if [ "$broken" -gt 0 ]; then
    record "$name" FAIL "$broken arms fail with the fix: $out"
  elif [ "$unconfirmed" -gt 0 ]; then
    record "$name" REVIEW "$confirmed confirmed, $unconfirmed unconfirmed: $out"
  elif [ "$confirmed" -gt 0 ]; then
    record "$name" PASS "$confirmed arms confirmed"
  else
    record "$name" FAIL "no verdict lines: $out"
  fi
}

container_tier(){
  local img
  img="$(release_image)" || { record "release image" FAIL "$LOGDIR/build-release.log"; return 1; }
  log "release image under test: $img"
  scenario zombie-kick      sc_prove_fix9.sh  "$img"
  scenario orphan-pump      sc_prove_fix15.sh "$img"
  scenario starvation-exit  sc_prove_fix16.sh "$img"
  ab watchdog-arms ab_base_vs_fixed.sh
  ab frame-pump-arms fp_ab.sh
}

# ----------------------------------------------------------------------- main
unit_tier
if [ "$QUICK" = 0 ]; then container_tier; fi

echo
printf '%-34s %-7s %s\n' "GATE" "RESULT" "NOTE"
printf '%-34s %-7s %s\n' "----------------------------------" "-------" "----"
fail=0
for r in "${RESULTS[@]}"; do
  IFS='|' read -r n s d <<<"$r"
  printf '%-34s %-7s %s\n' "$n" "$s" "$d"
  [ "$s" = FAIL ] && fail=1
done
echo
if [ "$fail" = 0 ]; then echo "VERIFY: PASS"; else echo "VERIFY: FAIL"; fi
exit "$fail"
