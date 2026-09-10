# Fork of neolink with stability and bug fixes

Neolink bridges Reolink cameras that only speak the Baichuan protocol (port 9000) to RTSP clients like Frigate, Shinobi and Blue Iris.

## Why this fork

Upstream has had no commit since 2025-01-30 and its open pull requests are unreviewed.

Run 24/7, upstream leaks memory and file descriptors and wedges in ways no watchdog sees.

This fork is upstream master plus four open pull requests taken verbatim, plus our own fixes below, each traced to a root cause against a real camera and checked base-versus-fixed in simulation.

## Architecture

Before (upstream):

```
                    per RTSP client, a whole new stack
                 +------------------------------------------+
   camera -----> | BC session -> pump -> appsrc -> pipeline  | --> client 1
          -----> | BC session -> pump -> appsrc -> pipeline  | --> client 2
          -----> | BC session -> pump -> appsrc -> pipeline  | --> ffprobe
                 +------------------------------------------+
                        ^                  ^
                        |          new BufferPool per frame
                  BC ping only          (one socketpair per frame)

   liveness:  BC ping only              (blind to frames stopping)
   sessions:  never closed on expiry    (client blocks on read forever)
   DESCRIBE:  waits forever             (one dead camera freezes all)
   result:    RSS and FDs climb until OOM
```

After (this fork), the simple path:

```
                 +---------------------------------------------+
   camera -----> | BC session -> MPEG-TS muxer -> stdout        | --> go2rtc exec:
                 +---------------------------------------------+
                   exit 0 on stdout close, non-zero on no frames;
                   the supervisor respawns, nothing else to watch
```

After (this fork), the RTSP path kept for multi-client setups:

```
                       one shared pipeline per camera
                 +---------------------------------------------+
   camera -----> | BC session -> pump -> appsrc -> pipeline ->  | --> client 1
                 |                (drop oldest)           pay0  | --> client 2
                 +---------------------------------------------+
                        ^            ^                  ^
                  frame-arrival   bucketed pools   egress watchdog
                  watchdog 30 s   1 MB max         15 s on pay0
                        |                               |
                        +-----------> EOS <-------------+
                                       |
                        reap session, close client TCP,
                        rebuild within an 8 s bound
```

The RTSP path is not simpler than upstream, it is the same structure with less state per client and watchdogs that turn every silent stall into a rebuild.

The pipe path is simpler than both: one process per camera and the supervisor is the only watchdog.

## Fixes

"Verified" means the fix ran in production against four cameras with the symptom gone.

| Fix | Symptom | Upstream issues | Status |
|---|---|---|---|
| Shared pipeline per camera ([#400](https://github.com/QuantumEntangledAndy/neolink/pull/400) by [@joshkautz](https://github.com/joshkautz), verbatim) | Every client opened its own pipeline and camera session, so FDs and memory grew per connect. | [#202](https://github.com/QuantumEntangledAndy/neolink/issues/202), [#215](https://github.com/QuantumEntangledAndy/neolink/issues/215), [#286](https://github.com/QuantumEntangledAndy/neolink/issues/286), [#366](https://github.com/QuantumEntangledAndy/neolink/issues/366), [#370](https://github.com/QuantumEntangledAndy/neolink/issues/370), [#380](https://github.com/QuantumEntangledAndy/neolink/issues/380) | Verified |
| Bucketed buffer pools ([#373](https://github.com/QuantumEntangledAndy/neolink/pull/373) by [@wafgo](https://github.com/wafgo), verbatim) | A new buffer pool per frame meant a new socketpair per frame, 2.8 MB/s growth until OOM. | [#202](https://github.com/QuantumEntangledAndy/neolink/issues/202), [#286](https://github.com/QuantumEntangledAndy/neolink/issues/286), [#366](https://github.com/QuantumEntangledAndy/neolink/issues/366), [#370](https://github.com/QuantumEntangledAndy/neolink/issues/370), [#380](https://github.com/QuantumEntangledAndy/neolink/issues/380), [#382](https://github.com/QuantumEntangledAndy/neolink/issues/382), [#411](https://github.com/QuantumEntangledAndy/neolink/issues/411) | Verified |
| Non-blocking channel sends ([#399](https://github.com/QuantumEntangledAndy/neolink/pull/399) by [@joshkautz](https://github.com/joshkautz), verbatim, plus subscriber depth 1000) | A full channel blocked the message loop, pings went unanswered and the camera dropped the session ("Reaching limit of channel"). | [#143](https://github.com/QuantumEntangledAndy/neolink/issues/143), [#215](https://github.com/QuantumEntangledAndy/neolink/issues/215), [#286](https://github.com/QuantumEntangledAndy/neolink/issues/286), [#315](https://github.com/QuantumEntangledAndy/neolink/issues/315), [#346](https://github.com/QuantumEntangledAndy/neolink/issues/346), [#349](https://github.com/QuantumEntangledAndy/neolink/issues/349), [#366](https://github.com/QuantumEntangledAndy/neolink/issues/366) | Verified |
| 64-bit timestamps ([#398](https://github.com/QuantumEntangledAndy/neolink/pull/398) by [@joshkautz](https://github.com/joshkautz), verbatim) | The 32-bit microsecond counter wrapped every 71.58 minutes and froze the stream. | [#209](https://github.com/QuantumEntangledAndy/neolink/issues/209), [#378](https://github.com/QuantumEntangledAndy/neolink/issues/378) | Verified |
| Never suspend the shared pipeline on client churn | A client leaving set the shared pipeline to NULL and egress stopped for everyone. Diagnosed on gst-rtsp-server 1.22. It does not trigger on gst-rtsp-server 1.26.2, where client churn never calls `gst_rtsp_media_suspend`, and is kept as a no-op safety. | none verified | Unconfirmed |
| Drop oldest frame under back-pressure | The newest frame was dropped instead, so latency grew to minutes. | [#209](https://github.com/QuantumEntangledAndy/neolink/issues/209), [#290](https://github.com/QuantumEntangledAndy/neolink/issues/290), [#310](https://github.com/QuantumEntangledAndy/neolink/issues/310), [#315](https://github.com/QuantumEntangledAndy/neolink/issues/315), [#349](https://github.com/QuantumEntangledAndy/neolink/issues/349), [#360](https://github.com/QuantumEntangledAndy/neolink/issues/360), [#378](https://github.com/QuantumEntangledAndy/neolink/issues/378) | Verified |
| Frame-arrival watchdog | A camera that stopped sending frames was never noticed while its pings still answered. | none verified | Verified |
| Frame pump survives transient errors | One "App source is closed" killed the delivery thread for hours. | [#310](https://github.com/QuantumEntangledAndy/neolink/issues/310), [#315](https://github.com/QuantumEntangledAndy/neolink/issues/315), [#346](https://github.com/QuantumEntangledAndy/neolink/issues/346), [#349](https://github.com/QuantumEntangledAndy/neolink/issues/349), [#360](https://github.com/QuantumEntangledAndy/neolink/issues/360) | Verified |
| Bounded 8 s pipeline build | One camera mid-reconnect froze the RTSP server for every camera. | [#360](https://github.com/QuantumEntangledAndy/neolink/issues/360) | Verified |
| Stale-session reap and client kick | Expired sessions never closed the client's TCP connection, so it blocked on read forever. | [#164](https://github.com/QuantumEntangledAndy/neolink/issues/164), [#315](https://github.com/QuantumEntangledAndy/neolink/issues/315), [#346](https://github.com/QuantumEntangledAndy/neolink/issues/346) | Verified |
| Poller busy-spin fix | After a disconnect the poller re-entered a closed channel and pegged a core at 100%. The spin is reproduced at mechanism level, 644,000 re-entries in 200 ms on the base build against one return on the fixed build. | [#215](https://github.com/QuantumEntangledAndy/neolink/issues/215), [#290](https://github.com/QuantumEntangledAndy/neolink/issues/290), [#320](https://github.com/QuantumEntangledAndy/neolink/issues/320), [#380](https://github.com/QuantumEntangledAndy/neolink/issues/380), [#390](https://github.com/QuantumEntangledAndy/neolink/issues/390) | Verified (mechanism) |
| Frame-starvation exit | After a reconnect the pump waited forever while dead media was served to new clients. | [#209](https://github.com/QuantumEntangledAndy/neolink/issues/209), [#346](https://github.com/QuantumEntangledAndy/neolink/issues/346) | Verified |
| Dead-camera DESCRIBE gate | A powered-off camera was served a DESCRIBE and returned dead media, and the gate serves 0 of 10 DESCRIBEs against 4 of 10 without it. The `get_rates` assert that aborted the process is fixed by gst-rtsp-server 1.26.2 itself (gstreamer MR !7731), so only the DESCRIBE liveness gate is ours. | [#215](https://github.com/QuantumEntangledAndy/neolink/issues/215), [#286](https://github.com/QuantumEntangledAndy/neolink/issues/286), [#370](https://github.com/QuantumEntangledAndy/neolink/issues/370) | Verified |
| Orphan pump reaping | Each timed-out build leaked a thread, reaching 121 threads and 586 FDs in 72 h. | [#380](https://github.com/QuantumEntangledAndy/neolink/issues/380) | Verified |
| Egress-liveness watchdog | The stall watchdog never saw fragmented frames, so it never armed on large streams. | [#346](https://github.com/QuantumEntangledAndy/neolink/issues/346) | Verified |

The pull requests are unchanged from their authors' branches, so upstream can merge them as-is.

## Which mode to use

**`neolink stream`** for one local consumer per camera, such as go2rtc or Frigate:

```yaml
streams:
  driveway:
    - "exec:neolink stream --config /etc/neolink.toml driveway --stream main --format ts"
```

One process, one camera, MPEG-TS on stdout, no RTSP server.

It exits 0 when the consumer closes stdout and non-zero when frames stop, so the supervisor's respawn replaces every in-process watchdog.

Measured side by side with RTSP on the same camera: same cadence, zero dropped frames in a CFR re-encode, 32 MB and 10 FDs versus 73 MB and 37, and 10 s recovery from a 40 s link cut.

**`neolink rtsp`** for several clients per camera, or clients you do not control.

Audio works on both paths. The pipe path carries a camera's AAC track in the MPEG-TS stream. When a camera sends ADPCM instead, it transcodes to AAC through `voaacenc`. That transcode is covered by unit tests over a captured ADPCM block, not by a live ADPCM camera.

Caveat: the pipe path's longest soak is 20 minutes against months for RTSP, and cold start is 4.4 s per respawn.

## Docker

```bash
docker build -t neolink:fork .
```

The base is Debian trixie with gst-rtsp-server 1.26.2, which removed the assert that let a dead camera abort the process (gstreamer !7731).

The build fails on purpose if the runtime library is older than 1.24.9.

`Dockerfile.ab-bookworm-stock` is a test-only control image. Do not deploy it.

## Verification

- `iso-test/sim/verify.sh` is the entrypoint. `--quick` runs the unit tier alone, and no flag runs the container scenarios and the A/B arms as well, ending in a PASS/FAIL table.
- `cargo test --release --workspace`: timestamp wrap and reset, monotonicity, audio re-anchor, MPEG-TS muxer. 119 tests green, 68 in neolink, 49 in neolink_core and 2 doc, up from 54 on the upstream-plus-PRs base.
- `.github/workflows/ci.yml` runs the unit tier and clippy on every push and pull request. It builds the same Debian trixie image the release binary is built in.
- `iso-test/` and `iso-test/sim/` are the A/B harnesses. Each builds a "base" tree with the fix reverted and a "fixed" tree, then asserts the measured difference.
- `iso-test/sim/` drives each wedge mode from the fix table and asserts the matching watchdog fires.
- Fix-table status: 14 rows Verified, 1 row Unconfirmed.

"Verified" in the fix table is a production observation, not a simulation result. The simulation results are a second, independent line of evidence.

## Upstream

Tracks [QuantumEntangledAndy/neolink](https://github.com/QuantumEntangledAndy/neolink), itself a fork of [thirtythreeforty/neolink](https://github.com/thirtythreeforty/neolink). Everything below is upstream documentation, unchanged.

## Installation

Download from the
[release page](https://github.com/QuantumEntangledAndy/neolink/releases)

Extract the zip

Install the latest [gstreamer](https://gstreamer.freedesktop.org/download/)
(1.20.5 as of writing this).

- **Windows**: ensure you install `full` when prompted in the MSI options.
- **Mac**: Install the dpkg version on the official gstreamer website over
  the brew version
- **Ubuntu/Debian**: These packages should work

```bash
sudo apt install \
  libgstrtspserver-1.0-0 \
  libgstreamer1.0-0 \
  libgstreamer-plugins-bad1.0-0 \
  gstreamer1.0-x \
  gstreamer1.0-plugins-base \
  gstreamer1.0-plugins-good \
  gstreamer1.0-plugins-bad \
  libssl
```

- **Windows**: You may also need to
  [install openssl](https://wiki.openssl.org/index.php/Binaries)
- **Macos**: You may also need to
  [install openssl](https://wiki.openssl.org/index.php/Binaries) or
  `brew install openssl@1.1`
- **Ubuntu/Debian**: Install the `libssl` package

Make a config file see below.

## Config/Usage

### RTSP

To use `neolink` you need a config file.

There's a more complete example
[here](https://github.com/QuantumEntangledAndy/neolink/blob/master/sample_config.toml),
but the following should work as a minimal example.

```toml
bind = "0.0.0.0"

[[cameras]]
name = "Camera01"
username = "admin"
password = "password"
uid = "ABCDEF0123456789"

[[cameras]]
name = "Camera02"
username = "admin"
password = "password"
uid = "BCDEF0123456789A"
address = "192.168.1.10"
```

Create a text file called `neolink.toml` in the same folder as the
neolink binary. With your config options.

When ready start `neolink` with the following command
using the terminal in the same folder the neolink binary is in.

```bash
./neolink rtsp --config=neolink.toml
```

### Discovery

To connect to a camera using a UID we need to find the IP address of the camera
with that UID

The IP is discovered with four methods

1. Local discovery: Here we send a broadcast on all visible networks asking
   the local network if there is a camera with this UID. This only works if
   the network supports broadcasts

   If you know the ip address you can put it into the `address` field of the
   config and attempt a direct connection without broadcasts. This requires a
   route from neolink to the camera.

2. Remote discovery: Here we ask the reolink servers what the IP address is.
   This requires that we contact reolink and provide some basic information
   like the UID. Once we have this information we connect directly to the
   local IP address. This requires a route from neolink to the camera and
   for the camera to be able to contact reolink.

3. Map discovery: In this case we register our IP address with reolink and ask
   the camera to connect to us. Once the camera either polls/recieves a connect
   request from the reolink servers the camera will initiate a connection
   to neolink. This requires that our IP and reolink are reachable from
   the camera.

4. Relay: In this case we request that reolink relay our connection. Neolink
   nor the camera need to be able to direcly contact each other. But both
   neolink and the camera need to be able to contact reolink.

This can be controlled with the config

```toml
discovery = "local"
```

In the `[[cameras]]` section of the toml.

Possible values are `local`, `remote`, `map`, `relay` later values implictly
enable prior methods.

#### Cellular

Cellular cameras should select `"cellular"` which only enables `map` and
`relay` since `local` and `remote` will always fail

```toml
discovery = "cellular"
```

See the sample config file for more details.

### MQTT

To use mqtt you will need to adjust your config file as such:

```toml
bind = "0.0.0.0"

[mqtt]
broker_addr = "127.0.0.1" # Address of the mqtt server
port = 1883 # mqtt servers port
credentials = ["username", "password"] # mqtt server login details

[[cameras]]
name = "Camera01"
username = "admin"
password = "password"
uid = "ABCDEF0123456789"
```

Then to start the mqtt+rtsp connection run the following:

```bash
./neolink mqtt-rtsp --config=neolink.toml
```

OR for only mqtt

```bash
./neolink mqtt --config=neolink.toml
```

Neolink will publish these messages:

Messages that are prefixed with `neolink/`

- `/status` Tracks the connection of neolink, `connected` for ready `offline`
  for not ready this is a LastWill message
- `/config` The configuration file used to start neolink, you can publish to
  this to **temporarily** alter the live configuration
- `/config/status` If you publish to `/config` then any errors from your
  publish config will show here, or `Ok(())` if no errors and finished loading

Messages that are prefixed with `neolink/{CAMERANAME}`

Control messages:

- `/control/led [on|off]` Turns status LED on/off
- `/control/ir [on|off|auto]` Turn IR lights on/off or automatically via light
  detection
- `/control/reboot` Reboot the camera
- `/control/ptz [up|down|left|right|in|out] (amount)` Control the PTZ
  movements, amount defaults to 32.0
- `/control/ptz/preset [id]` Move the camera to a PTZ preset
- `/control/ptz/assign [id] [name]` Set the current PTZ position to a preset ID
  and name
- `/control/zoom (amount)` Zoom the camera to the specified amount. Example: 1.0
  for normal and 3.5 for 3.5x zoom factor. This only works on cameras that support
  zoom
- `/control/pir [on|off]`
- `/control/floodlight [on|off]` Turns floodlight (if equipped) on/off
- `/control/floodlight_tasks [on|off]` Turns floodlight (if equipped) tasks on/off
  This is the automatic tasks such as on motion and night triggers
- `/control/wakeup (mins)` For cameras that are using `idle_disconnect` this will
  force a wakeup for at least the given minutes
- `/control/siren on` Signal the siren, the message is always "on" as there is no
  "off" signal for the siren

Status Messages:

- `/status disconnected` Sent when the camera goes offline
- `/status/battery` Sent in reply to a `/query/battery` an XML encoded version
  of the battery status
- `/status/battery_level` A simple % value of current battery level, only
  published when `enable_battery` is true in the config
- `/status/pir` Sent in reply to a `/query/pir` an XML encoded version of the
  pir status
- `/status/motion` Contains the motion detection alarm status. `on` for motion
  and `off` for still, only published when `enable_moton` is true in the config
- `/status/ptz/preset` Sent in reply to a `/query/ptz/preset` an XML encoded
  version of the PTZ presets
- `/status/preview` a base64 encoded camera image updated every 2s. Not
  every camera supports the snapshot command needed for this. In such cases
  there will be no `/status/preview` message. Only published when
  `enable_preview` is true in the config
- `/status/floodlight_tasks` The current status of the floodlight tasks
  used updated every 2s by default

Query Messages:

- `/query/battery` Request that the camera reports its battery level
- `/query/pir` Request that the camera reports its pir status
- `/query/ptz/preset` Request that the camera reports its PTZ presets
- `/query/preview` Request that the camera post a base64 encoded jpeg
  of the stream to `/status/preview` now, ignoring the timer

### Controlling RTSP from MQTT

If neolink is started with `mqtt-rtsp` then the `/neolink/config` can be used
to control the RTSP

Changes made to the config by publishing to `/neolink/config` should be
reflected in the rtsp

These include changing the:

- Available users

```toml
[[users]]
  name = "me"
  pass = "mepass"
```

- Permitted users on a camera

```toml
[[cameras]]
  permitted_users = [ "me" ]
```

- Available streams

```toml
[[cameras]]
  stream = "Main"
```

Setting a value of `None` will disable the stream

- Disable the entire camera (mqtt updates and all)

```toml
[[cameras]]
  enabled = false
```

### MQTT Disable Features

Certain features like preview and motion detection may not be desired
you can disable them with the following config options.
Disabling these may help to conserve battery

```toml
bind = "0.0.0.0"

[mqtt]
broker_addr = "127.0.0.1" # Address of the mqtt server
port = 1883 # mqtt servers port
credentials = ["username", "password"] # mqtt server login details

[[cameras]]
name = "Camera01"
username = "admin"
password = "password"
uid = "ABCDEF0123456789"
[cameras.mqtt]
enable_motion = false        # motion detection
                             # (limited battery drain since it
                             # is a passive listening connection)
                             #
enable_light = false         # flood lights only available on some camera
                             # (limited battery drain since it
                             # is a passive listening connection)
                             #
enable_battery = false       # battery updates in `/status/battery_level`
                             #
enable_preview = false       # preview image in `/status/preview`
                             #
enable_floodlight = false    # preview image in `/status/floodlight_tasks`
                             #
battery_update = 2000        # Number of ms between `/status/battery_level` updates
                             #
preview_update = 2000        # Number of ms between `/status/preview` updates
                             #
floodlight_update = 2000     # Number of ms between `/status/floodlight_tasks` updates
```

#### MQTT Discovery

[MQTT Discovery](https://www.home-assistant.io/integrations/mqtt/#mqtt-discovery)
is partially supported. Currently, discovery is opt-in and camera features
must be manually specified.

```toml
[cameras.mqtt]
  # <see above>
  [cameras.mqtt.discovery]
  topic = "homeassistant"
  features = ["floodlight"]
```

Available features are:

- `floodlight`: This adds a light control to home assistant
- `camera`: This adds a camera preview to home assistant. It is only updated
  every 0.5s and cannot be much more than that since it is updated over mqtt
  not over RTSP. Not every camera supports the snapshot command needed for
  this. In such cases there will be no `/status/preview` message.
- `led`: This adds a switch to chage the LED status light on/off to home
  assistant
- `ir`: This adds a selection switch to chage the IR light on/off/auto to home
  assistant
- `motion`: This adds a motion detection binary sensor to home assistant
- `reboot`: This adds a reboot button to home assistant
- `pt`: This adds a selection of buttons to control the pan and tilt of the
  camera
- `battery`: This adds a battery level sensor to home assistant
- `siren`: Adds a siren button to home assistant

### Extra Camera Settings

Listed below are extra camera settings:

```toml
[[cameras]]
name = "Camera01"
username = "admin"
password = "password"
uid = "ABCDEF0123456789"
debug = false # Displays Debug XML messages from camera
enabled = true # Enable or Disable the camera
update_time = false # When camera connects, force the setting of the camera date/time to now. The default is false
print_format = "None"  # Type of format that logs are displayed in (None, Human, Xml). The default is None
```

- **Debug:** Will dump the various XMLs from the camera as they are recieved
and decrypted. Leave this off unless asked for it to fix an issue.

- **Enabled:** Useful if you want to remove a camera from rtsp without deleting
it from the config

- **update_time:** Used to FORCE an update on the camera time. Usually it checks
if it is needed but this
will force it regardless. (Mostly this was introduced to address a specific
ssue a user had)

- **print_format:** Used for adjusting printing of some values mostly, battery
messages

### Pause

To use the pause feature you will need to adjust your config file as such:

```toml
bind = "0.0.0.0"

[[cameras]]
name = "Camera01"
username = "admin"
password = "password"
uid = "ABCDEF0123456789"
  [cameras.pause]
  on_motion = true # Should pause when no motion
  on_client = true # Should pause when no rtsp client
  timeout = 2.1 # How long to wait after motion stops before pausing
```

Then start the rtsp server as usual:

```bash
./neolink rtsp --config=neolink.toml
```

### Idle Disconnects

To really save battery we need to disconnect the camera when it is idle.

To acheieve this you can add `idle_disconnect = true` to the `[[cameras]]`
section

```toml
bind = "0.0.0.0"

[[cameras]]
name = "Camera01"
username = "admin"
password = "password"
uid = "ABCDEF0123456789"
idle_disconnect = true
[cameras.pause]
  on_client = true # Should pause when no rtsp client
  timeout = 2.1 # How long to wait after motion stops before pausing
```

When `idle_disconnect = true` neolink will disconnect from the camera 30s
after it stops being used.

Neolink considers it as being used if there is an active stream running, or
if there is motion being detected or an mqtt command being run

[Because google remove the api for the push notifications we cannot
reliably use push notifications to wake up, so motion won't wake
up neolink anymore]

You can make neolink stop active streams when there are no rtsp clients using

```toml
[cameras.pause]
  on_client = true # Should pause when no rtsp client
```

Once in the disconnected state. Neolink will stay disconnected until there is a
new requested activation such as a client connecting or an mqtt command

~Neolink will also wake up on push notifications from the camera. These are usually
sent by the camera on motion or PIR alarms. To disable this you can set
`push_notifications = false` in the `[[cameras]]` config~

[Google removed the apis we were using for push notifications]

### Docker

[Docker](https://hub.docker.com/r/quantumentangledandy/neolink) builds are also
provided in multiple architectures. The latest tag tracks master while each
branch gets it's own tag.

```bash
docker pull quantumentangledandy/neolink

# Add `-e "RUST_LOG=debug"` to run with debug logs
#
# --network host is only needed if you require to connect
# via local broadcasts. If you can connect via any other
# method then normal bridge mode should work fine
# and you can ommit this option. Not all OSes support
# network=host, notably macos lacks this option.
docker run --network host --volume=$PWD/config.toml:/etc/neolink.toml quantumentangledandy/neolink
```

#### Environmental Variables

There are currently 2 environmental variables available as part of the container:

- `NEO_LINK_MODE`: defaults to `"rtsp"` if not set, other options are "mqtt" or "mqtt-rtsp".
- `NEO_LINK_PORT`: defaults to `8554`, set this to your required port value.

### Image

You can write an image from the stream to disk using:

```bash
neolink image --config=config.toml --file-path=filepath CameraName
```

Where filepath is the path to save the image to and CameraName is the name of
the camera from the config to save the image from.

File is always jpeg and the extension given in filepath will be added or changed
to reflect this.

Some cameras do not support the SNAP command that is used to generate the image
on the camera. If this is the case with your camera you can try the
`--use-stream` option which will instead create a jpeg by transcoding the video
stream.

### Battery Levels

You can get the battery level and status using

```bash
neolink battery --config=config.toml CameraName
```

This will produce an xml formatted battery status on stdout for processing

### PIR

You can control pir using

```bash
neolink pir --config=config.toml CameraName [on|off]
```

This will turn the PIR on or off

### Reboot

You can reboot a camera using

```bash
neolink reboot --config=config.toml CameraName
```

### Status LED

You can control the status LED using

```bash
neolink status-light --config=config.toml CameraName [on|off]
```

### Talk

You can talk over the camera using

```bash
neolink talk --config=config.toml --adpcm-file=data.adpc\
               --sample-rate=16000 --block-size=512 CameraName
```

Where the sounds is ADPCM encoded

or

```bash
neolink talk --config=config.toml --microphone  CameraName
```

Which uses the default microphone which depends on
[gstreamer](https://gstreamer.freedesktop.org/documentation/autodetect/autoaudiosrc.html?gi-language=c#autoaudiosrc-page)

### PTZ

You can control the PTZ using

```bash
neolink ptz --config=config.toml CameraName control 32 [left|right|up|down|in|out]
```

Where 32 is the speed. Not all cameras support speed

Some cameras also support preset positions

```bash
# Print the list of preset positions
neolink ptz --config=config.toml CameraName preset
# Move the camera to preset ID 0
neolink ptz --config=config.toml CameraName preset 0
# Save the current position as preset ID 0 with name PresetName
neolink ptz --config=config.toml CameraName assign 0 PresetName
```

To change the zoom level use the following:

```bash
# Zoom the camera to 2.5x
neolink ptz --config=config.toml CameraName zoom 2.5
```

With 1.0 being normal and 2.5 being 2.5x zoom

## License

Neolink is free software, released under the GNU Affero General Public License
v3.

This means that if you incorporate it into a piece of software available over
the network, you must offer that software's source code to your users.

## Donations

If you find this code helpful please consider supporting development.

[![ko-fi](https://ko-fi.com/img/githubbutton_sm.svg)](https://ko-fi.com/G2G5HOYIZ)
