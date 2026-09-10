#!/bin/bash
# A/B the frame-pump resilience regression arms against the tree that has the
# fix and the tree that does not. Same mechanics as ab_base_vs_fixed.sh (read
# that file's header first); this is a separate driver purely so the
# frame-pump arms can be added without touching the fix6/14/15/16 plan.
#
# Each arm (fp_arm_*.rs) is a `#[cfg(test)] mod` appended UNCHANGED to both
# trees' src/rtsp/factory.rs. It only calls items of the enclosing module, so
# it exercises whatever that tree actually does. Where a pre-fix tree lacks
# those items, the matching fp_prefix_shim_*.rs supplies them — each shim is
# the pre-fix expression lifted verbatim from that tree, cited in its header.
# A shim of "-" means the arm needs none: it calls production functions that
# exist unchanged on both trees.
#
# Expected outcome: PASS on the fixed tree, FAIL on the pre-fix tree.
#
# Needs: an image with the crate's build deps and a warm target cache
# (neolink:devbuild) plus the GStreamer runtime plugins:
#   apt-get install -y gstreamer1.0-plugins-base gstreamer1.0-plugins-good \
#                      gstreamer1.0-plugins-bad gstreamer1.0-plugins-ugly
# Arms skip themselves (green) when a needed element is missing.
#
# usage: fp_ab.sh <fix> [<container-prefix>]
#        <fix> in: 14e8129 | a78b2da | cd4b78b | c34b277 | 40fbd86 | 2542108 | all
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SIM="$REPO/iso-test/sim"
DEVTEST="$(dirname "$REPO")/devtest.sh"
PREFIX="${2:-neolink-fp}"
IMAGE="${AB_IMAGE:-neolink:devbuild}"

# fix -> "arm-file:shim-file:fixed-rev:prefix-rev:test-filter"
declare -A PLAN=(
  [14e8129]="fp_arm_transient_errors.rs:fp_prefix_shim_14e8129.rs:WORKTREE:14e8129^:fp_arm_transient_errors"
  [a78b2da]="fp_arm_eos_threshold.rs:fp_prefix_shim_a78b2da.rs:WORKTREE:a78b2da^:fp_arm_eos_threshold"
  [cd4b78b]="fp_arm_backpressure.rs:fp_prefix_shim_cd4b78b.rs:WORKTREE:cd4b78b^:fp_arm_backpressure"
  [c34b277]="fp_arm_detached_fast_exit.rs:fp_prefix_shim_c34b277.rs:WORKTREE:c34b277^:fp_arm_detached_fast_exit"
  [40fbd86]="fp_arm_leaky_downstream.rs:-:WORKTREE:40fbd86^:fp_arm_leaky_downstream"
  [2542108]="fp_arm_video_buffer.rs:-:WORKTREE:2542108^:fp_arm_video_buffer"
)
ORDER=(14e8129 a78b2da cd4b78b c34b277 40fbd86 2542108)

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
  fixed_dir="$(mktemp -d "/tmp/nl-fp-$fix-fixed.XXXX")"
  prefix_dir="$(mktemp -d "/tmp/nl-fp-$fix-prefix.XXXX")"
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
  # the shared arm. The pre-fix tree needs the shim as well (when it has one).
  cat "$SIM/$arm" >> "$fixed_dir/src/rtsp/factory.rs"
  if [ "$shim" = "-" ]; then
    cat "$SIM/$arm" >> "$prefix_dir/src/rtsp/factory.rs"
  else
    cat "$SIM/$shim" "$SIM/$arm" >> "$prefix_dir/src/rtsp/factory.rs"
  fi

  ensure_ctr "$PREFIX-dev"
  ensure_ctr "$PREFIX-base"

  echo "--- $fix: FIXED tree ($fixed_rev) ---"
  "$DEVTEST" "$PREFIX-dev" "$fixed_dir" -- test --release --bin neolink -- \
    --nocapture "$filter" 2>&1 | grep -E '^\[ARM|^SKIP|^test |test result'
  local fixed_rc=${PIPESTATUS[0]}

  echo "--- $fix: PRE-FIX tree ($prefix_rev) ---"
  "$DEVTEST" "$PREFIX-base" "$prefix_dir" -- test --release --bin neolink -- \
    --nocapture "$filter" 2>&1 | grep -E '^\[ARM|^SKIP|^test |test result'
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

case "${1:?usage: fp_ab.sh <14e8129|a78b2da|cd4b78b|c34b277|40fbd86|2542108|all> [container-prefix]}" in
  all) for f in "${ORDER[@]}"; do run_one "$f"; done ;;
  *)   run_one "$1" ;;
esac
