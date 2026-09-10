#!/bin/bash
# Sync a source tree into a long-lived dev container and run cargo in it.
#
# The A/B drivers in this directory call this to run the same test arm against
# two different trees without rebuilding a container per tree.
#
# usage: devtest.sh <container-name> [<srcdir>] -- <cargo args...>
#
# The container must already exist and carry the crate's build dependencies
# and, ideally, a warm target/release cache; ab_base_vs_fixed.sh and fp_ab.sh
# create it from $AB_IMAGE. Build that image with
#   docker build -f Dockerfile --target build -t neolink:devbuild .
set -euo pipefail
CTR="${1:?container name}"; shift
SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
if [ "${1:-}" != "--" ]; then SRC="$1"; shift; fi
[ "${1:-}" = "--" ] && shift
docker exec "$CTR" sh -c 'rm -rf /usr/local/src/neolink/src /usr/local/src/neolink/crates /usr/local/src/neolink/tests'
docker cp -q "$SRC/src"    "$CTR:/usr/local/src/neolink/src"    2>/dev/null || docker cp "$SRC/src"    "$CTR:/usr/local/src/neolink/src"
docker cp -q "$SRC/crates" "$CTR:/usr/local/src/neolink/crates" 2>/dev/null || docker cp "$SRC/crates" "$CTR:/usr/local/src/neolink/crates"
for f in Cargo.toml Cargo.lock build.rs rustfmt.toml; do
  [ -f "$SRC/$f" ] && docker cp "$SRC/$f" "$CTR:/usr/local/src/neolink/$f" >/dev/null
done
[ -d "$SRC/dissector" ] && docker cp "$SRC/dissector" "$CTR:/usr/local/src/neolink/dissector" >/dev/null
if [ -d "$SRC/iso-test" ]; then docker exec "$CTR" rm -rf /usr/local/src/neolink/iso-test; docker cp "$SRC/iso-test" "$CTR:/usr/local/src/neolink/iso-test" >/dev/null; fi
exec docker exec -w /usr/local/src/neolink "$CTR" cargo "$@"
