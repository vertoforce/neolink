#!/bin/bash
# Resolve the "pre-fix" revision an A/B arm compares against.
#
# The A/B plans name each fix by the commit that introduced it, so the base
# side of the comparison is that commit's parent. Those commit ids belong to
# the branch the fix was written on; a tree that re-committed the same fixes
# under different ids cannot resolve them. This maps each one to a subject
# search instead, so the drivers work in either history.
#
# Source it, do not run it:  . "$(dirname "$0")/ab_rev.sh"
# Needs $REPO set to the repository root.

declare -A AB_SUBJECT=(
  [14e8129]="make the frame pump survive transient send errors"
  [a78b2da]="make the frame pump survive transient send errors"
  [2542108]="make the frame pump survive transient send errors"
  [cd4b78b]="back-pressure watchdog and session reap on client close"
  [c34b277]="exit the frame pump fast once its appsrc is detached"
  [40fbd86]="drop the oldest video frame under pressure"
  [3ddaff6]="bound the create_element reply wait"
  [8269d71]="gate DESCRIBE on camera liveness"
  [c4e03a0]="remove the audio-stall exit and reap orphan frame pumps"
  [b0e6d6c]="un-blind the pay0 probe and gate the stall exits on PLAYING"
)

# ab_resolve_rev <rev-expression>
# Echoes a revision that exists in $REPO, or the input unchanged when it
# already does. Returns 1 when neither the id nor a subject match is found.
ab_resolve_rev() {
  local expr="$1" id="${1%^}" subject sha
  if git -C "$REPO" rev-parse --verify -q "${expr}^{commit}" >/dev/null 2>&1; then
    echo "$expr"; return 0
  fi
  subject="${AB_SUBJECT[$id]:-}"
  if [ -z "$subject" ]; then
    echo "ab_resolve_rev: no subject mapping for $id" >&2; return 1
  fi
  sha="$(git -C "$REPO" rev-parse --verify -q ":/${subject}" 2>/dev/null)" || {
    echo "ab_resolve_rev: no commit matching '$subject'" >&2; return 1
  }
  # Three of the frame-pump fixes ship as one commit in a squashed history, so
  # the base side there is that whole commit's parent rather than each fix's
  # own parent. The arm still A/Bs fix against no-fix; it is just less
  # isolated. Say so rather than letting the caller assume otherwise.
  if [ "${expr}" != "${id}" ]; then echo "${sha}^"; else echo "${sha}"; fi
}
