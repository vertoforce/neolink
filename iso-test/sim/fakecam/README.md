# fakecam — a fake Baichuan camera

`fakecam` is a Baichuan (Reolink BC) server that speaks enough of the protocol
for an unmodified `neolink` to log in and pull H.264 video from it over TCP,
and that can be told to fail on command. It exists so the reconnect,
liveness-watchdog and RTSP-egress behaviour of this fork can be exercised
end-to-end without pointing anything at a real camera.

It listens on loopback, serves a canned 640x360 test pattern in a loop, and
takes fault-injection commands on a second TCP port.

## What it implements

Verified end to end against the release `neolink` binary built from this tree:

* **TCP transport on port 9000.** The legacy `LoginMsg` probe that
  `Discovery::check_tcp` sends before the real connection is answered too, so
  neolink's "TCP Discovery success" step passes.
* **The two-stage login.** Stage one is the legacy class-`0x6514` message; the
  reply is a class-`0x6614` modern message carrying an `Encryption` XML with a
  fresh nonce and a `0xdd__` response code that names the negotiated
  encryption. Stage two is the modern class-`0x6414` message whose `LoginUser`
  hashes are checked against `md5(user+nonce)` and `md5(pass+nonce)`,
  truncated to 31 uppercase hex digits, exactly as `neolink_core` builds them.
  Success returns `DeviceInfo`; a bad hash returns `400`.
* **Encryption negotiation:** `none` (`0xdd00`), `bcencrypt` (`0xdd01`), `aes`
  (`0xdd02`) and `fullaes` (`0xdd12`), picked with `--encryption`. The camera
  announces the lower of what it is configured for and what the client asked
  for. Default is `bcencrypt`. All four have been driven end to end, but note
  that `fullaes` is not faithful: a real camera encrypts the media payload
  too, and this one does not. It works only because the client falls back to
  the raw payload when the extension carries no `encryptLen`.
* **The messages neolink sends after login:** `AbilityInfo` (151) — the reply
  advertises `preview_rw`, without which `start_video` refuses to run —
  `Version` (80), `StreamInfoList` (146), `Uid` (114), `SystemGeneral`
  get/set (104/105), `MotionRequest` (31), `Logout` (2).
* **Liveness.** `MSG_ID_PING` (93) is answered with a `LinkType` XML, which is
  what neolink's `get_linktype()` watchdog wants; a bare 200 is not enough.
* **Video.** `Preview` (3) is answered the way a real camera does: one packet
  that is both the `200` acknowledgement and the first binary payload, with a
  `binaryData=1` extension that puts the client's decoder into binary mode for
  that `msg_num`, carrying a `BcMedia` `InfoV2` header. After that, one BC
  packet per access unit at `--fps`, IDRs as `BcMedia::Iframe` and the rest as
  `BcMedia::Pframe`. `VideoStop` (4) stops the pump.
* **Anything else** gets a `400`, which is how a real camera reports a feature
  it does not have and what neolink treats as a soft, retryable failure.

The BC and BcMedia framing is not hand-rolled: `neolink_core::test_server`
(behind the crate's non-default `test-server` feature) re-exposes the crate's
own `Bc`/`BcMedia` codecs with the direction reversed, so the simulator cannot
drift from the wire format the client parses. The one exception is legacy
(class `0x6514`) framing, which `test_server::ServerCodec::decode` does
itself — the client-side parser never consumes the 1772 bytes of padding in a
legacy login body and reads 64 bytes past the end of a body-less
`LoginUpgrade`. Neither matters to a client, which never receives a legacy
message, but both desynchronise a server on the first packet.

## What it does NOT implement

* **UDP / P2P / relay discovery.** TCP port 9000 only. Configs must use
  `address = "127.0.0.1:9000"` and `discovery = "none"`.
* **Audio.** No AAC or ADPCM packets, so `--format ts` produces a video-only
  MPEG-TS after its 3 s audio-learning timeout (measured: `ffprobe` reports a
  single h264 stream). Use `--format h26x` when you just want bytes.
* **H.265.** The fixture and the frame pump are H.264 only.
* **Talk-back, PTZ, floodlight, battery, motion events, snapshots, push
  notifications, firmware upgrade, user management, `Support` (199).** All
  return `400`.
* **Camera-initiated messages**, including the UDP keepalive (234) and motion
  alarms (33). Nothing is ever sent unsolicited.
* **Per-stream content.** `subStream` and `externStream` are accepted and get
  the same 640x360 frames as `mainStream`.
* **Multi-channel/NVR behaviour.** The request's `channel_id` is echoed back
  but nothing is keyed on it.
* **Real timestamps.** The clock reply is a fixed 2026-01-01T00:00:00 UTC.
  BcMedia microsecond stamps start at 0 on every `start_video` and advance by
  `1_000_000 / fps`.

## The test pattern

`data/testpattern.h264` is 2 s of `testsrc` at 640x360/15fps (33 KB), encoded
by x264 as constrained baseline, one slice per frame, no B-frames, IDR every
15 frames, with SPS/PPS repeated before every IDR. The parameter-set repeat is
what lets a client that attaches mid-loop start decoding at the next IDR. It
was produced with:

```bash
ffmpeg -f lavfi -i testsrc=size=640x360:rate=15 -t 2 -threads 1 \
  -c:v libx264 -preset veryfast -tune zerolatency -profile:v baseline \
  -pix_fmt yuv420p -bf 0 \
  -x264-params repeat-headers=1:keyint=15:min-keyint=15:scenecut=0:slices=1:sliced-threads=0:threads=1 \
  -f h264 data/testpattern.h264
```

Point `--h264` at any other Annex-B file to use it instead; it is split into
access units at runtime and looped.

## Running it

```bash
cargo build --release --manifest-path iso-test/sim/fakecam/Cargo.toml
./target/release/fakecam                 # 127.0.0.1:9000, control on :9010
```

Useful flags: `--bind`, `--control`, `--username`, `--password`,
`--encryption {none|bcencrypt|aes|fullaes}`, `--fps`, `--width`, `--height`,
`--h264 <file>`, `--mode {normal|freeze|hang}`. `RUST_LOG=debug` shows every
message id it handles.

`testcam.toml` in this directory is a matching neolink config. Nothing in it
is real: loopback address, generic camera name, throwaway password.

```bash
# Pipe mode: no RTSP server, media on stdout, logs on stderr.
./target/release/neolink stream --config iso-test/sim/fakecam/testcam.toml \
    testcam --format h26x > /tmp/out.h264

# RTSP mode: serves rtsp://127.0.0.1:8554/testcam/main
./target/release/neolink rtsp --config iso-test/sim/fakecam/testcam.toml
```

## Fault injection

Commands go to the control port, one per line. `ctl.py` is a dependency-free
client:

```bash
python3 iso-test/sim/fakecam/ctl.py status
python3 iso-test/sim/fakecam/ctl.py freeze
python3 iso-test/sim/fakecam/ctl.py 127.0.0.1:9010 10,freeze 30,normal   # timed
```

| Command  | Effect | What it reproduces |
|----------|--------|--------------------|
| `normal` | Answer everything, stream video. | Healthy camera. |
| `freeze` | Stop sending video frames; keep answering pings and every other control message. | A camera whose BC session is alive while its encoder is wedged — the "pings OK but no frames" mode that `fix 13` exists for. |
| `die`    | Drop every live BC connection now, then go back to accepting new ones. One-shot, so what gets exercised is the client's reconnect. | A camera that resets its TCP connection. |
| `hang`   | Accept TCP connections but never write a byte back, on new and existing connections alike. | A camera that is reachable at the socket level and dead above it. From cold this stalls neolink at TCP discovery; applied mid-session it stalls pings and frames without closing the socket. |

`hang` is sticky: send `normal` to leave it. `die` leaves the mode alone.
