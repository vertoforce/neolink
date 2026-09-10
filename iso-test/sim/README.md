# iso-test/sim — simulation harnesses

Everything here runs against a **fake camera, a bare GStreamer pipeline, or
nothing at all**. No file in this directory contacts a real camera, and none
carries an address, camera name or credential from a real deployment.

The rule for every harness: a fix is only CONFIRMED when the same scenario
shows the bug **present on the base tree** and **absent on the fixed tree**,
with a number. Where that could not be shown, the harness says so in a comment
rather than being deleted — a recorded negative is worth more than a missing
one.

The base tree throughout is commit `8708608`: upstream `master` plus PRs
\#373, \#400, \#399 and \#398, with none of this fork's own commits. For
single-fix comparisons the fix's own parent commit is used instead, which is
what `ab_base_vs_fixed.sh` does.

## Running them

`verify.sh` is the entrypoint. `verify.sh --quick` runs the unit tier, which is
`cargo test --release --workspace` inside a container built from this repo's
Dockerfile build stage. `verify.sh` with no flag runs that tier and then the
container scenarios and the A/B arms, and prints a PASS/FAIL table. Gates that
report themselves are scored; gates that print measurements for a human to read
are marked REVIEW with the path to their log.

## The fake camera

`fakecam/` completes a real Baichuan login, serves a canned H.264 test pattern
at a configurable frame rate, and takes fault-injection commands on a control
port. `freeze` stops sending media while the connection and the pings stay
healthy, which is the wedge most of these fixes are about. `die` drops the
connection outright, and `hang` accepts the TCP connection and never replies,
which is the negative control. It emits no audio, so any audio-path assertion
in a scenario that uses it is vacuous.

## The two-tree A/B driver

`ab_base_vs_fixed.sh` and `fp_ab.sh` take one arm file, append it unchanged to
`src/rtsp/factory.rs` in two trees, and run it in both. The fixed tree is the
working tree and the base tree is the fix's own parent commit, so the arm sees
the same code the fix changed and nothing else. An arm only calls items of the
module it is appended to, so where the base tree lacks an item the matching
`prefix_shim_*.rs` supplies the pre-fix expression, lifted verbatim from that
tree and cited in the shim's header. A fix is CONFIRMED when the arm passes on
the fixed tree and fails on the base tree, and the driver says UNCONFIRMED
rather than PASS when both trees agree.

## Adding a scenario for a new fix

For a fix whose behaviour is a predicate or a state machine, write the arm as a
`#[cfg(test)] mod` in `<name>_arm_<fix>.rs` that calls only production items,
add a shim if the base tree lacks one of them, and add the row to the driver's
`PLAN` array as `arm:shim:fixed-rev:base-rev:test-filter`. For a fix whose
behaviour only shows up in a running process, copy the closest `sc_prove_*.sh`,
source `sc_lib.sh` for the container plumbing, and drive the fake camera into
the wedge with `sc_ctl`. Then add the new gate to `verify.sh` with a decisive
log line to score it on, and add a row here saying what it does and does not
prove. The rule is unchanged: a fix is only CONFIRMED with a number on both
sides of the comparison, and a scenario that cannot show one says so in its
header rather than being deleted.

## The pieces

| Path | What it is |
|---|---|
| `verify.sh` | The entrypoint. `--quick` for the unit tier, no flag for every tier, PASS/FAIL table at the end. |
| `devtest.sh` | Copies a source tree into a long-lived container and runs cargo in it, so an arm can be run against two trees without rebuilding a container per tree. |
| `ab_rev.sh` | Resolves each A/B plan's base revision, by commit id where it exists and by commit subject where the same fix was re-committed under another id. |
| `fakecam/` | A fake Baichuan camera. Completes a real BC login, streams a canned H.264 test pattern, and injects faults (`freeze`, `die`, `hang`) on a control port. See its own README. |
| `ab_base_vs_fixed.sh` | Two-tree A/B driver. Appends the same `arm_*.rs` to the fixed tree and to the tree at a fix's parent commit and runs it in both; a `prefix_shim_*.rs` supplies the pre-fix expression the old tree lacks. |
| `arm_*.rs` / `prefix_shim_*.rs` | The arms and shims that driver uses (fix 6, fix 14, fix 15, fix 16). |
| `run_fix_e_tests.sh`, `base_arm_tests.rs` | The same idea for the stale-session reap and zombie-client kick, whose fixed tests cannot compile against the base tree at all. |
| `fp_ab.sh`, `fp_arm_*.rs`, `fp_prefix_shim_*.rs` | The same idea again for the six frame-pump resilience fixes. |
| `sc_lib.sh`, `sc_prove_fix{9,15,16}.sh`, `sc_fakecam.Dockerfile` | `iso-test/prove_fix{9,15,16}.sh` re-pointed from the production camera onto a fakecam container. |
| `deadcam_describe.sh` | Dead-camera DESCRIBE behaviour, using an unroutable RFC 5737 address on an `--internal` network. |
| `hung_camera_describe.sh` | Negative control: a camera stuck at TCP accept is **not** the fix 6 trigger. |
| `suspend-churn/` | Negative result: on stock gst-rtsp-server 1.26.2, client churn against a shared factory never reaches `gst_rtsp_media_suspend` at all, in either suspend mode. |

## Where the code runs

Everything expects a container built from the Dockerfile's `build` stage
(Debian trixie, gst-rtsp-server 1.26.2 dev headers, a warm `target/release`):

```bash
docker build -f Dockerfile --target build -t neolink:devbuild .
```

Tests that need real GStreamer *elements* skip themselves with a `SKIP` line
when the plugins are absent, so `cargo test` stays green without them; the A/B
drivers install `gstreamer1.0-plugins-{base,good,bad,ugly}` into their
containers so the arms actually run.

The harness crates (`fakecam/`, `suspend-churn/`) are deliberately **excluded**
from the neolink workspace and `iso-test/` is in `.dockerignore`, so none of
this is compiled into the release image. Build them explicitly:

```bash
cargo build --release --manifest-path iso-test/sim/fakecam/Cargo.toml
```

## A trap worth knowing

`ab_base_vs_fixed.sh` copies the **live working tree** for its fixed side. If
anything is mid-edit, the result is meaningless — and it fails in the
convincing direction, reporting a fix as broken. Verify against a clean
`git archive HEAD` extract before believing a failure.

## What is not covered here

Three things are deliberately recorded as not-proven rather than quietly passed:

* **`SuspendMode::None`** — `suspend-churn/` cannot reach
  `gst_rtsp_media_suspend` at all on stock gst-rtsp-server 1.26.2, in either
  mode. The fix 4 analysis was done against 1.22 and neolink's own factory.
* **fix 14's SIGABRT** — does not reproduce on either image, because both run
  1.26.2, which carries gstreamer MR !7731. Only
  `iso-test/prove_upstream_gst.sh` GATE A can A/B that, across library
  versions.
* **fix 16's probe blindness, end to end** — `sc_prove_fix16.sh` confirms
  the starvation exit but not the blindness mechanism: a single H.264 stream
  always emits some sub-MTU pushes, so a BUFFER-only probe never reads zero
  against the fake camera. `fp_`/`arm_egress_probe.rs` is the evidence for
  that.

The fake camera emits **no audio**, so any audio-path assertion in these
harnesses is vacuous.
