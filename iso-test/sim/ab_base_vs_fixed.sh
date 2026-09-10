#!/bin/bash
# A/B a fix regression arm against the tree that has the fix and the tree
# that does not.
#
# Each arm (arm_*.rs) is a `#[cfg(test)] mod` that is appended UNCHANGED to
# both trees' src/rtsp/factory.rs. It only calls items of the enclosing
# module, so it exercises whatever that tree actually does. A pre-fix tree
# does not define those items, so the matching prefix_shim_*.rs supplies them
# — each shim is the pre-fix expression lifted verbatim from that tree, and
# the header comment of every shim cites the code it was copied from.
#
# Expected outcome: PASS on the fixed tree, FAIL on the pre-fix tree.
#
# Needs: an image with the crate's build deps and a warm target cache
# (neolink:devbuild) plus the GStreamer runtime plugins:
#   apt-get install -y gstreamer1.0-plugins-base gstreamer1.0-plugins-good \
#                      gstreamer1.0-plugins-bad gstreamer1.0-plugins-ugly
# Arms skip themselves (green) when a needed element is missing.
#
# usage: ab_base_vs_fixed.sh <fix> [<container-prefix>]
#        <fix> in: fix6 | fix14 | fix15 | fix16 | all
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SIM="$REPO/iso-test/sim"
DEVTEST="$(dirname "$REPO")/devtest.sh"
PREFIX="${2:-neolink-fx}"
IMAGE="${AB_IMAGE:-neolink:devbuild}"

# fix -> "arm-file:shim-file:fixed-rev:prefix-rev:test-filter"
declare -A PLAN=(
  [fix6]="arm_build_wait.rs:prefix_shim_fix6.rs:WORKTREE:3ddaff6^:arm_build_wait"
  [fix14]="arm_liveness_gate.rs:prefix_shim_fix14.rs:WORKTREE:8269d71^:arm_liveness_gate"
  [fix15]="arm_orphan_pump.rs:prefix_shim_fix15.rs:WORKTREE:c4e03a0^:arm_orphan_pump"
  [fix16]="arm_egress_probe.rs:prefix_shim_fix16.rs:WORKTREE:b0e6d6c^:arm_egress_probe"
)

ensure_ctr() {
  local name="$1"
  if ! docker inspect "$name" >/dev/null 2>&1; then
    docker run -d --name "$name" "$IMAGE" sleep infinity >/dev/null
    docker exec "$name" sh -c 'apt-get update -qq && apt-get install -y -qq \
      gstreamer1.0-plugins-base gstreamer1.0-plugins-good \
      gstreamer1.0-plugins-bad gstreamer1.0-plugins-ugly' >/dev/null 2>&1
  elif [ "$(docker inspect -f '{{.State.Running}}' "$name")" != "true" ]; then
    docker start "$name" >/dev/null
  fi
}

run_one() {
  local fix="$1"
  IFS=: read -r arm shim fixed_rev prefix_rev filter <<<"${PLAN[$fix]}"
  echo "=============== $fix ==============="

  local fixed_dir prefix_dir
  fixed_dir="$(mktemp -d "/tmp/nl-ab-$fix-fixed.XXXX")"
  prefix_dir="$(mktemp -d "/tmp/nl-ab-$fix-prefix.XXXX")"
  # WORKTREE = HEAD plus the working tree's current src/ and crates/, so the
  # A/B can be run before the fix + tests are committed.
  if [ "$fixed_rev" = "WORKTREE" ]; then
    git -C "$REPO" archive HEAD | tar -x -C "$fixed_dir"
    rm -rf "$fixed_dir/src" "$fixed_dir/crates"
    cp -a "$REPO/src" "$REPO/crates" "$fixed_dir/"
  else
    git -C "$REPO" archive "$fixed_rev" | tar -x -C "$fixed_dir"
  fi
  git -C "$REPO" archive "$prefix_rev" | tar -x -C "$prefix_dir"

  # The fixed tree carries the in-repo regression tests already; append only
  # the shared arm. The pre-fix tree needs the shim as well.
  cat "$SIM/$arm"                >> "$fixed_dir/src/rtsp/factory.rs"
  cat "$SIM/$shim" "$SIM/$arm"   >> "$prefix_dir/src/rtsp/factory.rs"

  ensure_ctr "$PREFIX-dev"
  ensure_ctr "$PREFIX-base"

  echo "--- $fix: FIXED tree ($fixed_rev) ---"
  "$DEVTEST" "$PREFIX-dev" "$fixed_dir" -- test --release --bin neolink -- \
    --nocapture "$filter" 2>&1 | grep -E '^\[ARM|^test |test result'
  local fixed_rc=${PIPESTATUS[0]}

  echo "--- $fix: PRE-FIX tree ($prefix_rev) ---"
  "$DEVTEST" "$PREFIX-base" "$prefix_dir" -- test --release --bin neolink -- \
    --nocapture "$filter" 2>&1 | grep -E '^\[ARM|^test |test result'
  local prefix_rc=${PIPESTATUS[0]}

  echo "--- $fix verdict: fixed_rc=$fixed_rc prefix_rc=$prefix_rc"
  if [ "$fixed_rc" -eq 0 ] && [ "$prefix_rc" -ne 0 ]; then
    echo "$fix: CONFIRMED (arm passes with the fix, fails without it)"
  elif [ "$fixed_rc" -ne 0 ]; then
    echo "$fix: BROKEN — the arm fails on the fixed tree"
  else
    echo "$fix: UNCONFIRMED — the arm also passes on the pre-fix tree"
  fi
  rm -rf "$fixed_dir" "$prefix_dir"
}

case "${1:?usage: ab_base_vs_fixed.sh <fix6|fix14|fix15|fix16|all> [container-prefix]}" in
  all) for f in fix6 fix14 fix15 fix16; do run_one "$f"; done ;;
  *)   run_one "$1" ;;
esac
