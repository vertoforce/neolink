use gstreamer::ClockTime;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::{anyhow, Context, Result};
use gstreamer::{prelude::*, Bin, Caps, Element, ElementFactory, GhostPad};
use gstreamer_app::{AppSrc, AppSrcCallbacks, AppStreamType};
use neolink_core::{
    bc_protocol::StreamKind,
    bcmedia::model::{
        BcMedia, BcMediaIframe, BcMediaInfoV1, BcMediaInfoV2, BcMediaPframe, VideoType,
    },
};
use tokio::{sync::mpsc::channel as mpsc, task::JoinHandle};

use crate::{
    common::{now_epoch_ms, NeoInstance},
    rtsp::gst::{NeoMediaFactory, NeoRtspServer},
    AnyResult,
};

/// EGRESS liveness watchdog tunable (canonical location — reference this const,
/// do not hardcode the literal in the loop logic).
///
/// This is a NEW, independent safety net that sits alongside the three existing
/// load-bearing patches (SuspendMode::Reset + stop_on_disconnect in gst/factory.rs,
/// the RECEIVE-side FRAME_STALENESS_MS watchdog in camthread.rs, and the
/// EOS-at-100-errors / back-pressure-EOS exit below). It catches ONE failure mode
/// the others structurally cannot:
///
///   The "silent wedge" (observed 2026-05-28 14:14→17:06 UTC on `camera A`): the
///   GStreamer appsrc consumer (the RTSP transmit side) stalls, but
///   - frames keep ARRIVING from the camera, so the RECEIVE-side
///     FRAME_STALENESS_MS watchdog stays quiet (camera is healthy);
///   - the appsrc drop-on-near-full guard silently drops frames rather than
///     returning an Err, so `consecutive_errors` never climbs to EOS_THRESHOLD;
///   - the BC subscriber channel fills and drops messages ("Subscriber channel
///     full … dropping message"), again with no send ERROR surfaced here.
///   Net result under the prior code: nothing fired until consecutive_errors
///   eventually tripped ~3h later.
///
/// All the existing signals are RECEIVE-side or internal-data-plane. None of
/// them observe whether RTSP media bytes are actually LEAVING toward the
/// connected client. This watchdog adds exactly that one missing invariant:
/// if no RTP buffers egress from the payloader (`pay0`) for this long while a
/// client is connected (egress has started, i.e. egress_count > 0), tear down
/// via the SAME EOS+exit path the error/back-pressure watchdogs use, so the
/// factory callback rebuilds the pipeline on the next client connect.
///
/// Default 15s. Overridable at runtime via NEOLINK_RTSP_EGRESS_STALENESS_MS so
/// the threshold can be tuned without a rebuild (read once at thread start).
const RTSP_EGRESS_STALENESS_MS: u64 = 15_000;

/// fix 13: frame-pump watchdog tick (ms). The pump loop used to block
/// indefinitely in `media_rx.blocking_recv()`, so EVERY check in the loop body
/// (egress stall, post-EOS grace exit) only executed when a frame
/// arrived — a frame-starved pump detected nothing, exited nothing, and logged
/// nothing (the 2026-07-31 camera C Mode B wedge: camera reconnects, pings
/// pass, pump starves, gst-rtsp-server keeps serving the cached prepared media
/// as dead air with zero log output). Bounding the wait means the watchdog
/// block runs at least this often even with no frames. Tick iterations carry
/// no frame and MUST NOT touch the push-path counters (consecutive_errors /
/// consecutive_backpressure / consecutive_detached / eos_signaled-reset),
/// which assume one iteration == one push attempt — see the `let Some(data)`
/// gate in the loop.
const PUMP_RECV_TICK_MS: u64 = 5_000;

/// fix 15: ORPHAN frame-pump guard — consecutive empty 5s ticks
/// (PUMP_RECV_TICK_MS) on which the appsrc is found DETACHED from its bin
/// before the pump exits.
///
/// Measured leak (camera-d, 72h to 2026-08-28): 49 extra
/// threads all parked in pump_recv_timeout's nanosleep, +~500 MB RSS, +~540
/// socket FDs (BufferPool socketpairs). An orphan is a pump whose pipeline
/// is gone (media unprepared after its clients left, or a bin that was never
/// handed to gst-rtsp-server because its DESCRIBE timed out — fix 6) AND
/// whose camera subscription went quiet: media_rx is EMPTY but OPEN (its
/// media_tx lives in the `stream()` task's run_passive_task loop, which only
/// ends when a send fails — i.e. only after the pump drops media_rx: a
/// circular wait). Every pre-fix15 exit needed a FRAME to run its check
/// (the fix 4 detach fast-exit only tests check_live inside the push path)
/// or egress to have started, so such a pump spun on ticks forever holding
/// pools + subscription.
///
/// Guard: on each empty tick re-run the SAME terminal test fix 4 uses
/// (check_live: appsrc has no bus => it left the bin; never recoverable for
/// THIS pipeline) and fast-exit after this many consecutive detached ticks.
/// Probe-independent by design: a first version keyed on "pay0 egress
/// counter still 0 after 60s" was iso-measured killing a pipeline that was
/// serving 3 consumers at 15 fps — the pay0 BUFFER pad probe never fires
/// when the payloader pushes fragmented NALs as buffer LISTS, so "no egress
/// counted" is not evidence of no egress. Detachment is.
const ORPHAN_DETACHED_TICKS: u32 = 3;

/// fix 6: how long create_element (on gst-rtsp-server's single shared glib
/// main-loop thread) waits for the per-camera task to build the bin before
/// failing the DESCRIBE. Canonical location — see the create_element
/// callback in make_factory for the full rationale.
const BUILD_REPLY_TIMEOUT: Duration = Duration::from_secs(8);

/// fix 15: a pipeline build whose learn phase (draining the first BC frames
/// to discover codec/audio type) has not completed this long after it was
/// requested can no longer be delivered to anyone — its DESCRIBE gave up at
/// BUILD_REPLY_TIMEOUT. Pre-fix15 such builds sat parked in
/// `media_rx.recv()` forever (iso-measured: 7 of 16 timed-out builds never
/// completed), each holding the bin, the buffered frames and a BC
/// start_video subscription. Abandon them instead (2x the requester's wait so
/// the requester is provably gone; a build that completes inside the window
/// is unaffected).
const BUILD_LEARN_ABANDON: Duration = Duration::from_secs(2 * BUILD_REPLY_TIMEOUT.as_secs());

/// fix 14: DESCRIBE liveness gate — grace for the never-connected case.
///
/// `frames_stale` (camthread.rs) deliberately returns false while
/// `last_frame_at == 0` (camera never connected since process start) so the
/// camthread watchdog can't kill a camera that is still logging in. The
/// DESCRIBE gate must NOT inherit that exemption: a camera that has never
/// connected (powered-off `camera A`, 2026-08-14) would otherwise be served the
/// build_unknown splash pipeline — 500 videotestsrc buffers then EOS —
/// feeding placeholder frames into frigate recordings AND leaving behind the
/// post-EOS cached shared media whose complete-but-dataless sender streams
/// trip the gst-rtsp-server get_rates SIGABRT on the next PLAY (the ~30s
/// prod crash cadence: 20s splash + reconnect). Instead: once the factory is
/// older than this grace, a still-never-connected camera fast-fails its
/// DESCRIBEs. 15s is enough for healthy boot-time BC login (measured 3-6s,
/// even on marginal WiFi — and camthread stores last_frame_at=now on connect
/// SUCCESS, which reroutes the gate to frames_stale well before first frame)
/// while sitting BELOW the 20s splash EOS so a dead camera can never
/// complete a splash cycle once the gate is armed.
const NEVER_CONNECTED_GRACE_MS: u64 = 15_000;

/// fix 13: outcome of one bounded wait on the camera frame channel.
enum PumpRecv {
    Frame(BcMedia),
    TimedOut,
    Closed,
}

/// fix 13: bounded blocking receive for the frame-pump std::thread.
///
/// tokio 1.37's mpsc `Receiver` has no `blocking_recv_timeout`, so poll
/// `try_recv` with a short sleep while idle. When frames are flowing,
/// `try_recv` returns immediately and the sleep never runs (no hot-path
/// cost). Idle-poll latency (<= POLL_MS per frame worst case) cannot affect
/// the media clock: the appsrcs carry synthetic PTS (vid_ts / aud_ts,
/// `set_do_timestamp(false)` + `set_is_live(false)`), not arrival times.
fn pump_recv_timeout(
    rx: &mut tokio::sync::mpsc::Receiver<BcMedia>,
    timeout_ms: u64,
) -> PumpRecv {
    use tokio::sync::mpsc::error::TryRecvError;
    const POLL_MS: u64 = 20;
    let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        match rx.try_recv() {
            Ok(v) => return PumpRecv::Frame(v),
            Err(TryRecvError::Disconnected) => return PumpRecv::Closed,
            Err(TryRecvError::Empty) => {
                if std::time::Instant::now() >= deadline {
                    return PumpRecv::TimedOut;
                }
                std::thread::sleep(Duration::from_millis(POLL_MS));
            }
        }
    }
}

fn rtsp_egress_staleness_ms() -> u64 {
    std::env::var("NEOLINK_RTSP_EGRESS_STALENESS_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(RTSP_EGRESS_STALENESS_MS)
}

#[derive(Clone, Debug)]
pub enum AudioType {
    Aac,
    Adpcm(u32),
}

#[derive(Clone, Debug)]
struct StreamConfig {
    #[allow(dead_code)]
    resolution: [u32; 2],
    bitrate: u32,
    fps: u32,
    bitrate_table: Vec<u32>,
    fps_table: Vec<u32>,
    vid_type: Option<VideoType>,
    aud_type: Option<AudioType>,
}
impl StreamConfig {
    async fn new(instance: &NeoInstance, name: StreamKind) -> AnyResult<Self> {
        let (resolution, bitrate, fps, fps_table, bitrate_table) = instance
            .run_passive_task(|cam| {
                Box::pin(async move {
                    let infos = cam
                        .get_stream_info()
                        .await?
                        .stream_infos
                        .iter()
                        .flat_map(|info| info.encode_tables.clone())
                        .collect::<Vec<_>>();
                    if let Some(encode) =
                        infos.iter().find(|encode| encode.name == name.to_string())
                    {
                        let bitrate_table = encode
                            .bitrate_table
                            .split(',')
                            .filter_map(|c| {
                                let i: Result<u32, _> = c.parse();
                                i.ok()
                            })
                            .collect::<Vec<u32>>();
                        let framerate_table = encode
                            .framerate_table
                            .split(',')
                            .filter_map(|c| {
                                let i: Result<u32, _> = c.parse();
                                i.ok()
                            })
                            .collect::<Vec<u32>>();

                        Ok((
                            [encode.resolution.width, encode.resolution.height],
                            bitrate_table
                                .get(encode.default_bitrate as usize)
                                .copied()
                                .unwrap_or(encode.default_bitrate)
                                * 1024,
                            framerate_table
                                .get(encode.default_framerate as usize)
                                .copied()
                                .unwrap_or(encode.default_framerate),
                            framerate_table.clone(),
                            bitrate_table.clone(),
                        ))
                    } else {
                        Ok(([0, 0], 0, 0, vec![], vec![]))
                    }
                })
            })
            .await?;

        Ok(StreamConfig {
            resolution,
            bitrate,
            fps,
            fps_table,
            bitrate_table,
            vid_type: None,
            aud_type: None,
        })
    }

    fn update_fps(&mut self, fps: u32) {
        let new_fps = self.fps_table.get(fps as usize).copied().unwrap_or(fps);
        self.fps = new_fps;
    }
    #[allow(dead_code)]
    fn update_bitrate(&mut self, bitrate: u32) {
        let new_bitrate = self
            .bitrate_table
            .get(bitrate as usize)
            .copied()
            .unwrap_or(bitrate);
        self.bitrate = new_bitrate;
    }

    fn update_from_media(&mut self, media: &BcMedia) {
        match media {
            BcMedia::InfoV1(BcMediaInfoV1 { fps, .. })
            | BcMedia::InfoV2(BcMediaInfoV2 { fps, .. }) => self.update_fps(*fps as u32),
            BcMedia::Aac(_) => {
                self.aud_type = Some(AudioType::Aac);
            }
            BcMedia::Adpcm(adpcm) => {
                self.aud_type = Some(AudioType::Adpcm(adpcm.block_size()));
            }
            BcMedia::Iframe(BcMediaIframe { video_type, .. })
            | BcMedia::Pframe(BcMediaPframe { video_type, .. }) => {
                self.vid_type = Some(*video_type);
            }
        }
    }
}

/// fix 14: staleness threshold for the DESCRIBE gate's connected-then-died
/// branch. Deliberately 2x the camthread FRAME_STALENESS_MS (reference the
/// canonical const, don't restate the literal): an idle-but-healthy camera
/// (zero consumers => zero frames) cycles its BC connection every
/// ~FRAME_STALENESS_MS by design (fix 13 ping-success staleness arm), and
/// last_frame_at is only re-stored ~4s AFTER each reconnect (the two 2s
/// post-login settle sleeps in camthread::run_camera) — so idle-cycle
/// staleness legitimately peaks near FRAME_STALENESS_MS + ~5s. MEASURED
/// (iso test 2026-08-15): a gate that reused FRAME_STALENESS_MS directly
/// false-failed a healthy idle camera C's DESCRIBE in that post-reconnect
/// window — a chicken-and-egg lockout risk (the DESCRIBE it bounces is the
/// consumer whose frames would prove the camera alive). At 2x, that whole
/// healthy regime sits far below the threshold; only a camera whose
/// reconnects are FAILING (truly dead — reconnect success would re-store
/// last_frame_at) lets staleness grow past it.
const GATE_STALENESS_MS: u64 = 2 * crate::common::FRAME_STALENESS_MS;

/// fix 14: shared DESCRIBE liveness predicate for both factories. Two dead
/// states:
///   1. connected-then-died: no frame AND no successful (re)connect for
///      >GATE_STALENESS_MS (see that const for why this is 2x the camthread
///      watchdog's threshold);
///   2. never-connected (last_frame_at == 0, e.g. powered-off camera at
///      process start): dead once the factory is older than
///      NEVER_CONNECTED_GRACE_MS (see that const).
/// Inert for healthy cameras: camthread stores last_frame_at=now on every
/// connect success (fix 5) and the frame-pump refreshes it on every push.
///
/// KNOWN NOISE (assessed safe, deliberate): each gated DESCRIBE returns
/// Ok(None) -> the create_element vfunc returns NULL, and the gstreamer-rs
/// binding glue calls g_object_force_floating on that NULL before handing
/// it to C, printing one `GLib-GObject-CRITICAL ** g_object_force_floating:
/// assertion 'G_IS_OBJECT (object)' failed` per gated attach (~6/min while
/// a camera is down, zero when healthy). Why this is safe and kept:
/// - the C caller explicitly handles the NULL (rtsp-media-factory.c:1844
///   `if (element == NULL) goto no_element` -> NULL media -> the request
///   fails cleanly; measured live: clients get 400 and retry, process
///   RestartCount stays 0);
/// - glib assertion guards just log and return early — inert unless
///   G_DEBUG=fatal-criticals, which is set nowhere in this deployment
///   (compose env, Dockerfile, entrypoint all checked 2026-08-15);
/// - this is the SAME mechanism the fix 6 build-timeout path has used
///   since 2026-06-18 (same CRITICAL signature, hundreds/day during July
///   incidents in Loki, zero associated crashes);
/// - the alternative — returning a streamless element so the pointer is
///   non-NULL — hands gst-rtsp-server a media object its prepare/SDP path
///   was not designed for (0 streams; PREPARED transition depends on
///   receive-only/bus-message details), risking a blocked client watch,
///   i.e. trading bounded log noise for a potential all-client wedge.
fn liveness_gate_dead(
    last_frame_at: &Arc<AtomicU64>,
    armed_at: &std::time::Instant,
) -> bool {
    let last = last_frame_at.load(Ordering::Relaxed);
    if last == 0 {
        armed_at.elapsed() >= Duration::from_millis(NEVER_CONNECTED_GRACE_MS)
    } else {
        crate::common::now_epoch_ms().saturating_sub(last) > GATE_STALENESS_MS
    }
}

pub(super) async fn make_dummy_factory(
    camera: &NeoInstance,
    use_splash: bool,
    pattern: String,
) -> AnyResult<NeoMediaFactory> {
    // fix 14 — the dummy factory is the factory actually mounted for a
    // camera that has NEVER connected (mod.rs mounts it at startup "so the
    // URL will not return 404 while waiting for configuration"; the real
    // make_factory only replaces it once the camera's stream info arrives,
    // which requires a successful BC login). For a powered-off camera the
    // dummy therefore serves EVERY client, and its build_unknown splash
    // (videotestsrc, 500 buffers = 20s, then EOS) is the crash vector: after
    // EOS the cached shared media's streams are complete senders that never
    // pass data again, and a subsequent client PLAY hits the g_assert in
    // gst-rtsp-server rtsp-media.c:2766 get_rates -> SIGABRT of the whole
    // process (2026-08-14 storm, ~130 crashes/hour — cadence ~30s = 20s
    // splash + client reconnect). Gate it exactly like make_factory's
    // callback: once the camera is provably dead, fail the DESCRIBE in
    // microseconds instead of serving the splash. Healthy boot is
    // unaffected: the dummy only splashes inside NEVER_CONNECTED_GRACE_MS,
    // and a camera that connects flips the gate open via last_frame_at.
    let gate_last_frame_at = camera.last_frame_at().await?;
    let gate_name = camera.config().await?.borrow().name.clone();
    let gate_armed_at = std::time::Instant::now();
    NeoMediaFactory::new_with_callback(move |element| {
        if liveness_gate_dead(&gate_last_frame_at, &gate_armed_at) {
            log::info!(
                "create_element: {gate_name} (dummy factory): camera not delivering frames (never-connected or stale >{GATE_STALENESS_MS}ms) — fast-failing DESCRIBE instead of serving splash (fix 14 liveness gate)"
            );
            return Ok(None);
        }
        clear_bin(&element)?;
        if !use_splash {
            Ok(None)
        } else {
            build_unknown(&element, &pattern)?;
            Ok(Some(element))
        }
    })
    .await
}

enum ClientMsg {
    NewClient {
        element: Element,
        reply: std::sync::mpsc::SyncSender<Element>,
    },
}

pub(super) async fn make_factory(
    camera: NeoInstance,
    stream: StreamKind,
    // fix 9: the pump needs a route back to the RTSP server to kick
    // starved-but-connected clients when it terminally exits, and the mount
    // paths of THIS stream to scope the kick. See the zombie-client kick
    // comment at the bottom of the frame-pump thread.
    rtsp: NeoRtspServer,
    paths: Arc<Vec<String>>,
) -> AnyResult<(NeoMediaFactory, JoinHandle<AnyResult<()>>)> {
    let (client_tx, mut client_rx) = mpsc(100);
    // fix 14: clones for the create_element DESCRIBE liveness gate below.
    // Fetched here (async context) because the factory callback runs on the
    // shared glib main-loop thread where we can't await.
    let gate_last_frame_at = camera.last_frame_at().await?;
    let gate_name = camera.config().await?.borrow().name.clone();
    let gate_armed_at = std::time::Instant::now();
    // Create the task that creates the pipelines
    let thread = tokio::task::spawn(async move {
        let name = camera.config().await?.borrow().name.clone();
        // Per-camera frame-arrival cell. The frame-pump thread updates this
        // on every successful push_buffer; the camthread BC ping watchdog
        // reads it to decide whether to honor a ping timeout. See
        // common/camthread.rs for the design rationale.
        let last_frame_at = camera.last_frame_at().await?;

        // fix 9: pipeline GENERATION counter for this stream. Incremented on
        // every NewClient (i.e. every pipeline build); each frame-pump thread
        // remembers the generation it was born under. On terminal exit a pump
        // only kicks clients if it is STILL the newest generation — if a
        // newer pipeline exists, any connected client belongs to that
        // (healthy) pipeline and kicking it would churn a working stream
        // (worst case: an old pump kicking the new pipeline's client, whose
        // reconnect builds another pipeline, whose predecessor's exit kicks
        // again — a ping-pong loop). The guard makes the kick fire at most
        // once per terminal pipeline death, bounded by the pump-exit paths'
        // own thresholds.
        let pipeline_generation = Arc::new(AtomicU64::new(0));

        while let Some(msg) = client_rx.recv().await {
            match msg {
                ClientMsg::NewClient { element, reply } => {
                    log::debug!("New client for {name}::{stream}");
                    let camera = camera.clone();
                    let name = name.clone();
                    let last_frame_at = last_frame_at.clone();
                    let rtsp = rtsp.clone();
                    let paths = paths.clone();
                    let pipeline_generation = pipeline_generation.clone();
                    // fix 16: the generation is bumped INSIDE the build task,
                    // only once the bin has actually been handed to
                    // gst-rtsp-server (reply.send succeeded). Bumping here, at
                    // request time (pre-fix16), meant a build that later
                    // timed out / was abandoned / discarded still incremented
                    // the counter — so the currently SERVING pump was no
                    // longer the newest generation and its terminal exit
                    // silently skipped the fix 9 zombie-client kick.
                    tokio::task::spawn(async move {
                        clear_bin(&element)?;
                        log::trace!("{name}::{stream}: Starting camera");

                        // Start the camera
                        let config = camera.config().await?.borrow().clone();
                        let mut media_rx = camera.stream_while_live(stream).await?;

                        log::trace!("{name}::{stream}: Learning camera stream type");
                        // Learn the camera data type
                        let mut buffer = vec![];
                        let mut frame_count = 0usize;

                        let mut stream_config = StreamConfig::new(&camera, stream).await?;
                        // fix 15: bounded learn phase — see BUILD_LEARN_ABANDON.
                        let learn_deadline = tokio::time::Instant::now() + BUILD_LEARN_ABANDON;
                        loop {
                            let media = match tokio::time::timeout_at(learn_deadline, media_rx.recv()).await {
                                Ok(Some(media)) => media,
                                Ok(None) => break,
                                Err(_elapsed) => {
                                    log::info!(
                                        "{name}::{stream}: pipeline build abandoned — no stream type learned within {:?} (requesting DESCRIBE gave up at {:?}); dropping bin + camera subscription (fix 15)",
                                        BUILD_LEARN_ABANDON, BUILD_REPLY_TIMEOUT
                                    );
                                    return AnyResult::Ok(());
                                }
                            };
                            stream_config.update_from_media(&media);
                            buffer.push(media);
                            if frame_count > 10
                                || (stream_config.vid_type.is_some()
                                    && stream_config.aud_type.is_some())
                            {
                                break;
                            }
                            frame_count += 1;
                        }

                        log::trace!("{name}::{stream}: Building the pipeline");
                        // Build the right video pipeline
                        let vid_src = match stream_config.vid_type.as_ref() {
                            Some(VideoType::H264) => {
                                let src = build_h264(&element, &stream_config)?;
                                AnyResult::Ok(Some(src))
                            }
                            Some(VideoType::H265) => {
                                let src = build_h265(&element, &stream_config)?;
                                AnyResult::Ok(Some(src))
                            }
                            None => {
                                build_unknown(&element, &config.splash_pattern.to_string())?;
                                AnyResult::Ok(None)
                            }
                        }?;

                        // Build the right audio pipeline
                        let aud_src = match stream_config.aud_type.as_ref() {
                            Some(AudioType::Aac) => {
                                let src = build_aac(&element, &stream_config)?;
                                AnyResult::Ok(Some(src))
                            }
                            Some(AudioType::Adpcm(block_size)) => {
                                let src = build_adpcm(&element, *block_size, &stream_config)?;
                                AnyResult::Ok(Some(src))
                            }
                            None => AnyResult::Ok(None),
                        }?;

                        if let Some(app) = vid_src.as_ref() {
                            app.set_callbacks(
                                AppSrcCallbacks::builder()
                                    .seek_data(move |_, _seek_pos| true)
                                    .build(),
                            );
                        }
                        if let Some(app) = aud_src.as_ref() {
                            app.set_callbacks(
                                AppSrcCallbacks::builder()
                                    .seek_data(move |_, _seek_pos| true)
                                    .build(),
                            );
                        }

                        // EGRESS liveness observation point (chosen: option (a),
                        // a GStreamer BUFFER pad probe on the RTP payloader's
                        // src pad). `pay0` (rtph264pay / rtph265pay) is the last
                        // element in the bin WE build before the gst-rtsp-server
                        // takes over and transmits to the client. A buffer
                        // crossing pay0's src pad is the truest "RTP media is
                        // leaving toward the client" signal we can observe
                        // without reaching into the server-owned downstream
                        // (rtpbin / transmit). Crucially this is DOWNSTREAM of
                        // the appsrc: during the silent-wedge mode, push_buffer
                        // into appsrc still "succeeds" (queued / dropped on
                        // near-full) while pay0 output stalls because the RTSP
                        // transmit side isn't pulling — exactly the gap every
                        // existing (receive-side / send-error) signal misses.
                        //
                        // Why a probe and not the server's transmit callback:
                        // gstreamer-rtsp-server 0.23 does not expose the
                        // per-media data-transmit hook in a way we can attach
                        // to the shared media here; the payloader src pad is the
                        // closest observable boundary the crate gives us.
                        let egress_count = Arc::new(AtomicU64::new(0));
                        // fix 16: the payloader element, kept for the pump's
                        // PLAYING gate. NOT the appsrc: send_to_appsrc's
                        // back-pressure logic deliberately toggles the appsrc
                        // element between PAUSED (queue < 1/3) and PLAYING
                        // (queue > 2/3), so appsrc state says nothing about
                        // whether a client is playing; pay0 follows the media
                        // pipeline's state (PAUSED when merely prepared/cached,
                        // PLAYING while >= 1 client PLAYs).
                        let mut pay0_for_pump: Option<Element> = None;
                        {
                            let bin = element
                                .clone()
                                .dynamic_cast::<Bin>()
                                .map_err(|_| anyhow!("pipeline element should be a bin"))?;
                            if let Some(pay0) = bin.by_name("pay0") {
                                pay0_for_pump = Some(pay0.clone());
                                if let Some(srcpad) = pay0.static_pad("src") {
                                    let egress_count_probe = egress_count.clone();
                                    // fix 16: BUFFER | BUFFER_LIST. rtph264pay /
                                    // rtph265pay push a frame that fragments into
                                    // several RTP packets as ONE GstBufferList, and a
                                    // BUFFER-only probe is never called for lists — so
                                    // on every pipeline whose frames exceed the MTU
                                    // (all four of ours, iso-measured 2026-08-28) this
                                    // counter stayed at 0 forever and the egress-stall
                                    // + fix 13 starvation exits could never arm. Count
                                    // the list's packets so the counter is "RTP
                                    // packets egressed" in both cases.
                                    let _ = srcpad.add_probe(
                                        gstreamer::PadProbeType::BUFFER
                                            | gstreamer::PadProbeType::BUFFER_LIST,
                                        move |_pad, info| {
                                            let n = match info.data {
                                                Some(gstreamer::PadProbeData::BufferList(
                                                    ref list,
                                                )) => list.len().max(1) as u64,
                                                _ => 1,
                                            };
                                            egress_count_probe.fetch_add(n, Ordering::Relaxed);
                                            gstreamer::PadProbeReturn::Ok
                                        },
                                    );
                                    log::debug!(
                                        "{name}::{stream}: attached RTSP egress probe on pay0 src pad"
                                    );
                                } else {
                                    log::warn!(
                                        "{name}::{stream}: pay0 has no src pad — egress watchdog disabled for this pipeline"
                                    );
                                }
                            } else {
                                // Unknown/splash pipelines have a different
                                // payloader name; egress watchdog simply stays
                                // disarmed (egress_count never advances, and the
                                // monitor only fires once egress has STARTED).
                                log::debug!(
                                    "{name}::{stream}: no pay0 element — egress watchdog inactive (splash/unknown pipeline)"
                                );
                            }
                        }

                        log::trace!("{name}::{stream}: Sending pipeline to gstreamer");
                        // Send the pipeline back to the factory so it can start.
                        //
                        // fix 15: if the DESCRIBE that asked for this build
                        // already gave up (create_element's BUILD_REPLY_TIMEOUT,
                        // fix 6 — its Receiver is gone, so send() fails), this
                        // bin will never be handed to gst-rtsp-server and no
                        // client can ever attach to it. Spawning the frame-pump
                        // anyway (pre-fix15 behaviour) created an unowned
                        // thread holding media_rx — the orphan leak described at
                        // ORPHAN_DETACHED_TICKS. Discard everything here
                        // instead: dropping `element` (returned inside the
                        // SendError) finalizes the bin, dropping `media_rx` ends
                        // the BC start_video subscription (the stream task's
                        // next send fails and run_passive_task returns), and no
                        // pools/thread are ever created. The factory simply
                        // rebuilds on the client's retry.
                        if let Err(e) = reply.send(element) {
                            drop(e);
                            log::info!(
                                "{name}::{stream}: pipeline built after its DESCRIBE gave up (build-reply timeout) — discarding bin + camera subscription, not spawning a frame-pump (fix 15)"
                            );
                            return AnyResult::Ok(());
                        }
                        // fix 16: this pipeline now exists for gst-rtsp-server;
                        // it supersedes every earlier pump for this stream.
                        let my_generation =
                            pipeline_generation.fetch_add(1, Ordering::SeqCst) + 1;

                        // Run blocking code on a seperate thread
                        // This is not an async thread
                        let frame_pump_last_frame_at = last_frame_at.clone();
                        let egress_count_pump = egress_count.clone();
                        std::thread::spawn(move || {
                            let mut aud_ts: u64 = 0;
                            let mut vid_ts: u64 = 0;
                            let mut pools = Default::default();
                            // Thread lifecycle: drop the frame and keep going
                            // on transient send errors, but EXIT after EOS so
                            // the next client connection rebuilds cleanly via
                            // the factory callback.
                            //
                            // Why this matters for camera B (Reolink Elite WiFi
                            // panorama): the camera's BC connection ping-times-
                            // out every ~50s (WiFi/CPU saturation on the
                            // 5120x1552 stream). During the 5s reconnect, no
                            // frames flow → Frigate's ffmpeg RTSP read times
                            // out → it disconnects → SuspendMode::Reset
                            // unprepares the shared media → all appsrcs are
                            // detached from the bin → bus() returns None →
                            // every push_buffer fails with "App source is
                            // closed" forever. EOS at 100 errors triggers
                            // Frigate to respawn ffmpeg, which DOES create a
                            // new RTSP session and a new factory callback
                            // (confirmed in logs by counter resets to #1).
                            // But without this exit, the OLD thread keeps
                            // looping forever as a zombie, pumping into dead
                            // appsrcs, and N rebuild cycles produce N parallel
                            // zombie threads — all generating "send error"
                            // log spam, all holding stale media_rx + BC
                            // start_video subscriptions on the camera.
                            //
                            // Exit story by version:
                            //   - Exiting on first error (original): dead
                            //     thread, dead pipeline, hours-long wedges.
                            //     Wrong because transient state-transition
                            //     errors are common during pipeline pause.
                            //   - Exiting on N=500 (e3a0ec4): assumed a new
                            //     client would reconnect; but if EOS isn't
                            //     fired, the connected client never sees a
                            //     stream end and never reconnects.
                            //   - Never exit (b020970, 932d682): broke
                            //     camera B because of zombie accumulation
                            //     described above.
                            //   - Exit AFTER EOS + brief grace (this):
                            //     transient errors still tolerated indefinitely
                            //     (no exit unless EOS_THRESHOLD hit AND grace
                            //     elapses), but once EOS is fired and the
                            //     appsrcs are confirmed-detached, the thread
                            //     exits — closing media_rx, ending the
                            //     upstream BC start_video subscription, and
                            //     letting the factory rebuild fresh on
                            //     Frigate's ffmpeg respawn. No more zombies.
                            //
                            // If EOS does not actually cause a client to
                            // reconnect (e.g. eos_shutdown=false on the
                            // factory plus a client that ignores EOS), the
                            // stream stays dead until segment-watchdog
                            // (external) bounces neolink — same fallback as
                            // before, no regression.
                            let mut consecutive_errors: u32 = 0;
                            // Sibling counter for sustained back-pressure
                            // (push_buffer returning Flushing). Unlike
                            // consecutive_errors, the push call itself
                            // SUCCEEDS — but the appsrc queue is full because
                            // the RTSP consumer isn't draining. Without a
                            // counter the EOS-on-errors path never fires for
                            // this failure mode, and the stream silently
                            // wedges until external intervention. Observed
                            // mode: post-restart, frames flow into appsrc,
                            // "Buffer full pausing" loops forever, downstream
                            // ffmpeg sits in poll() forever (especially when
                            // it lacks a socket -timeout), no client churn,
                            // no factory rebuild. See `SendOutcome` plumbing
                            // in send_to_appsrc / send_to_sources.
                            let mut consecutive_backpressure: u32 = 0;
                            let mut eos_signaled = false;

                            // EGRESS liveness state (silent-wedge net). See the
                            // RTSP_EGRESS_STALENESS_MS doc-comment at the top of
                            // this file for the full rationale. We watch the
                            // pay0-src-pad buffer counter incremented by the pad
                            // probe attached above. `egress_last_count` is the
                            // value at the last advance; `egress_last_at` is the
                            // epoch-ms of that advance. The clock only ARMS once
                            // egress has actually started (count > 0) — an idle
                            // pipeline with no connected/consuming client never
                            // advances the counter and must NOT be torn down
                            // (that would fight the Reset/stop_on_disconnect
                            // teardown the existing patch relies on). Mirrors the
                            // last_frame_at==0 guard in camthread.rs::frames_stale.
                            let egress_staleness_ms = rtsp_egress_staleness_ms();
                            let mut egress_last_count: u64 = 0;
                            let mut egress_last_at: u64 = 0;
                            // Epoch-ms at which the egress watchdog fired EOS, or
                            // 0 if it hasn't. We track our OWN exit because the
                            // silent-wedge mode keeps producing Ok(Sent) from
                            // push_buffer (the appsrc accepts/drops frames), which
                            // clears `eos_signaled` and never touches the error /
                            // back-pressure counters — so we cannot lean on their
                            // POST_EOS_GRACE exit. ~2.5s grace mirrors POST_EOS_GRACE
                            // (50 frames @ 20fps) for the same teardown-race reason.
                            let mut egress_eos_at: u64 = 0;
                            const EGRESS_POST_EOS_GRACE_MS: u64 = 2_500;
                            // 100 consecutive errors at 20fps ≈ 5s of sustained
                            // failure. Transient state transitions clear in
                            // well under a second, so this threshold is
                            // comfortably past "transient" territory.
                            const EOS_THRESHOLD: u32 = 100;
                            // Terminal-detach fast-exit (fix 4, part 2).
                            //
                            // Under SuspendMode::None the shared pipeline is
                            // never reset-to-NULL on client churn (that was the
                            // suspend-to-NULL wedge we fixed). But a genuine
                            // FULL unprepare still happens when the LAST client
                            // leaves (prepare_count → 0): gst-rtsp-server takes
                            // the bin to NULL and removes our appsrc. From that
                            // moment `check_live` returns "App source is closed"
                            // (the appsrc's bus() is None — it left the bin) and
                            // it will NEVER recover for THIS pipeline: a new
                            // client triggers a brand-new bin + appsrc + frame-
                            // pump via the factory callback. So this error is
                            // TERMINAL, not transient. The general EOS path waits
                            // EOS_THRESHOLD+POST_EOS_GRACE (150 ≈ 7.5 s @20fps,
                            // and far longer if the camera stalls and the
                            // blocking_recv loop sleeps) before exiting — during
                            // which the doomed thread keeps its per-thread
                            // BufferPool sockets and its media_rx BC subscription
                            // alive. Over repeated full-drop/reconnect cycles
                            // that leaks FDs (measured: ~12 sockets/cycle) and
                            // BC subscriptions. We therefore detect the terminal
                            // "App source is closed" specifically and exit FAST
                            // (after a short confirmation window), dropping
                            // `pools` (freeing the socketpairs) and `media_rx`
                            // (ending the BC start_video subscription) promptly.
                            // The factory rebuilds cleanly on the next connect.
                            const DETACHED_FAST_EXIT_THRESHOLD: u32 = 15; // ~0.75s @20fps
                            let mut consecutive_detached: u32 = 0;
                            // Back-pressure tolerates more before firing —
                            // a slow consumer that briefly falls behind is
                            // normal. ~20s at 20fps. A genuinely stuck
                            // consumer holds Flushing indefinitely, so the
                            // distinction between "slow" and "wedged" is
                            // measured in seconds, not milliseconds.
                            const BACKPRESSURE_EOS_THRESHOLD: u32 = 400;
                            // After EOS, give the pipeline ~2.5s to either
                            // recover (rare) or fully tear down before exiting.
                            // 50 frames at 20fps. If we exit IMMEDIATELY after
                            // EOS, we risk the new client's create_element
                            // callback racing with our thread teardown (the
                            // outer ClientMsg::NewClient handler is async; if
                            // it sees the rx side of media_rx still open
                            // briefly, the new thread spawns first). Empirically
                            // a ~2.5s grace is plenty.
                            const POST_EOS_GRACE: u32 = 50;
                            // fix 15 — the fix 8 AUDIO-STALL exit that used
                            // to live here is REMOVED. It was speculative (one
                            // unconfirmed camera A incident 2026-07-03, never
                            // recurred) and measured as a pure false-positive
                            // generator: 183 fires in 72h on camera_d,
                            // EVERY one preceded ~10-15s earlier by camera BC
                            // ping timeouts and followed within 0-3s by "BC
                            // ping recovered" — i.e. it tore down the shared
                            // pipeline (EOS, pool re-alloc, go2rtc/ffmpeg
                            // reconnect, 139 frigate "Unable to read frames"
                            // in 72h) exactly as a transient camera hiccup was
                            // resolving itself. Audio on these streams is
                            // continuous when healthy (120s ffprobe: 5625 audio
                            // pkts, max gap 0.02s) and isn't even consumed by
                            // the _norm re-encoders. Whole-stream stalls stay
                            // owned by the egress-stall / starvation exits.

                            // fix 15: consecutive empty ticks on which the
                            // appsrc was found detached (orphan guard). Tick-
                            // only — separate from the push-path
                            // consecutive_detached counter.
                            let mut detached_ticks: u32 = 0;

                            log::trace!("{name}::{stream}: Sending buffered frames");
                            for buffered in buffer.drain(..) {
                                let _ = send_to_sources(
                                    buffered,
                                    &mut pools,
                                    &vid_src,
                                    &aud_src,
                                    &mut vid_ts,
                                    &mut aud_ts,
                                    &stream_config,
                                );
                            }

                            log::trace!("{name}::{stream}: Sending new frames");
                            // fix 13: epoch-ms of the last camera media (of ANY
                            // kind) received on media_rx. Initialised at pump
                            // birth = a fresh starvation grace window. Drives
                            // the camera-frame starvation exit in the watchdog
                            // block below.
                            let mut last_camera_frame_ms = now_epoch_ms();
                            loop {
                                // fix 13: bounded wait — a tick iteration
                                // (data == None) runs the watchdog block even
                                // when the camera delivers nothing, which is
                                // exactly when the old blocking_recv loop went
                                // blind.
                                let data = match pump_recv_timeout(
                                    &mut media_rx,
                                    PUMP_RECV_TICK_MS,
                                ) {
                                    PumpRecv::Frame(d) => {
                                        last_camera_frame_ms = now_epoch_ms();
                                        Some(d)
                                    }
                                    PumpRecv::TimedOut => None,
                                    PumpRecv::Closed => {
                                        // Observability (fix 13): this exit —
                                        // the upstream stream task dropped
                                        // media_tx — used to fall out of the
                                        // while-let with NO log line at info,
                                        // leaving a whole class of pump deaths
                                        // invisible at RUST_LOG=info.
                                        log::info!(
                                            "{name}::{stream}: camera frame channel closed — exiting frame-pump thread; factory rebuilds on next connect"
                                        );
                                        break;
                                    }
                                };
                                // EGRESS liveness check. Runs on every arriving
                                // frame AND (fix 13) on every empty tick.
                                // Compare the pay0 egress counter against its
                                // last-advanced value/time.
                                {
                                    let egress_now = egress_count_pump.load(Ordering::Relaxed);
                                    let now_ms = now_epoch_ms();
                                    // fix 16: the egress-stall and starvation
                                    // exits only make sense for a pipeline that
                                    // is actually PLAYING for a client. Once the
                                    // pay0 probe really counts (BUFFER_LIST), a
                                    // cached shared media left PAUSED by a
                                    // DESCRIBE-only client (ffprobe health
                                    // check, consumer that died before PLAY)
                                    // looks like an egress stall — frames arrive,
                                    // the leaky appsrc queue absorbs them, pay0
                                    // stops after preroll — and EOS'ing it with
                                    // no client attached leaves a dead media in
                                    // the factory cache that every later
                                    // DESCRIBE reuses (the fix 14 post-EOS
                                    // corpse class; create_element never runs
                                    // for a cache hit). PAUSED cached media is
                                    // legitimate and reusable; leave it alone.
                                    // State is read from pay0 (see pay0_for_pump),
                                    // never from the appsrc.
                                    let playing = pay0_for_pump.as_ref().is_some_and(|p| {
                                        p.current_state() == gstreamer::State::Playing
                                    });
                                    if egress_eos_at != 0 {
                                        // We already fired the egress EOS; wait
                                        // out the grace window then exit. We own
                                        // this exit (don't reuse the error /
                                        // back-pressure POST_EOS_GRACE) because in
                                        // the silent-wedge mode push_buffer keeps
                                        // returning Ok(Sent), which clears
                                        // `eos_signaled` and never advances those
                                        // counters.
                                        if now_ms.saturating_sub(egress_eos_at)
                                            >= EGRESS_POST_EOS_GRACE_MS
                                        {
                                            log::info!(
                                                "{name}::{stream}: exiting frame-pump thread after egress-stall EOS — factory callback will rebuild on next client connect"
                                            );
                                            break;
                                        }
                                    } else if egress_now > egress_last_count {
                                        // Bytes are leaving toward the client —
                                        // healthy. (Re)arm the clock.
                                        if egress_last_at == 0 {
                                            // fix 16 observability: one line per
                                            // pipeline when the egress watchdogs
                                            // arm (this never printed on
                                            // buffer-list pipelines before).
                                            log::info!(
                                                "{name}::{stream}: RTSP egress started ({egress_now} RTP packets) — egress-stall + starvation watchdogs armed"
                                            );
                                        }
                                        egress_last_count = egress_now;
                                        egress_last_at = now_ms;
                                    } else if data.is_some()
                                        && playing
                                        && egress_last_at != 0
                                        && now_ms.saturating_sub(egress_last_at)
                                            > egress_staleness_ms
                                    {
                                        // Egress HAD started (clock armed) but
                                        // has not advanced for longer than the
                                        // staleness window while frames keep
                                        // arriving: the RTSP consumer is wedged
                                        // and no other signal will catch it.
                                        // Trigger the SAME EOS path the error /
                                        // back-pressure watchdogs use — we do NOT
                                        // invent a parallel teardown.
                                        //
                                        // fix 13: `data.is_some()` gate keeps
                                        // this branch's pre-fix13 semantics
                                        // ("frames keep arriving" was implicit
                                        // when the loop only ran on frames) —
                                        // without it, every >=15s CAMERA-side
                                        // gap would now also fire this consumer-
                                        // wedge signal on a tick and churn
                                        // rebuilds on routine RF blips. Camera-
                                        // side gaps belong to the starvation
                                        // branch below (30s threshold).
                                        log::warn!(
                                            "{name}::{stream}: RTSP egress stalled (no bytes for {}s) — forcing pipeline rebuild",
                                            egress_staleness_ms / 1000
                                        );
                                        if let Some(src) = vid_src.as_ref() {
                                            let _ = src.end_of_stream();
                                        }
                                        if let Some(src) = aud_src.as_ref() {
                                            let _ = src.end_of_stream();
                                        }
                                        eos_signaled = true;
                                        egress_eos_at = now_ms;
                                    } else if egress_last_at != 0
                                        && playing
                                        && now_ms
                                            .saturating_sub(last_camera_frame_ms)
                                            >= crate::common::FRAME_STALENESS_MS
                                    {
                                        // fix 13: CAMERA-FRAME STARVATION exit
                                        // (camera C Mode B, 2026-07-31: 3
                                        // incidents, dead air 10h/11min/6min).
                                        // The camera stopped delivering frames
                                        // to THIS pump (stale BC stream after a
                                        // dead-declare + reconnect) while the
                                        // shared media stays prepared/cached, so
                                        // every new client silently attaches to
                                        // a corpse. No other signal can fire:
                                        // the egress branch above needs frames,
                                        // camthread's pings are healthy, and
                                        // check_live only runs on push. Exit via
                                        // the standard EOS + grace path so the
                                        // fix 9 kick closes consumers, the last
                                        // session releases the media, and the
                                        // next connect rebuilds against a fresh
                                        // camera stream subscription. Guarded on
                                        // egress_last_at != 0 (egress must have
                                        // STARTED) so an idle / slow-preroll
                                        // pipeline is never torn down — same
                                        // arming rule as the egress watchdog.
                                        // Threshold = the canonical camthread
                                        // FRAME_STALENESS_MS, not a new tunable.
                                        log::info!(
                                            "{name}::{stream}: no camera frames for >={}ms while pipeline serving — camera-frame starvation, signaling EOS and exiting so factory rebuilds (fix 13)",
                                            crate::common::FRAME_STALENESS_MS
                                        );
                                        if let Some(src) = vid_src.as_ref() {
                                            let _ = src.end_of_stream();
                                        }
                                        if let Some(src) = aud_src.as_ref() {
                                            let _ = src.end_of_stream();
                                        }
                                        eos_signaled = true;
                                        egress_eos_at = now_ms;
                                    }
                                    // fix 15: ORPHAN guard — see the
                                    // ORPHAN_DETACHED_TICKS doc-comment. Only on
                                    // empty ticks (a frame runs the real push-
                                    // path check_live instead). The appsrc
                                    // having no bus is the fix 4 terminal
                                    // condition; with an EMPTY media_rx nothing
                                    // else could ever observe it.
                                    if data.is_none() {
                                        let detached = vid_src
                                            .as_ref()
                                            .or(aud_src.as_ref())
                                            .is_some_and(|src| check_live(src).is_err());
                                        detached_ticks =
                                            if detached { detached_ticks + 1 } else { 0 };
                                        if detached_ticks >= ORPHAN_DETACHED_TICKS {
                                            log::info!(
                                                "{name}::{stream}: appsrc detached for {} idle ticks with no camera frames — orphan pipeline, fast-exiting frame-pump to free pools + camera subscription (fix 15)",
                                                detached_ticks
                                            );
                                            break;
                                        }
                                    }
                                }
                                // fix 13: tick-only iteration (no frame) — the
                                // watchdog block above has run; nothing to push.
                                // Skipping here keeps every counter below
                                // (consecutive_errors / consecutive_backpressure
                                // / consecutive_detached, and the eos_signaled
                                // reset on Sent) on its original "one iteration
                                // == one push attempt" semantics: ticks can
                                // neither advance nor reset them.
                                let Some(data) = data else { continue };
                                // Stamp arrival BEFORE the push attempt: this
                                // is the per-frame liveness signal the
                                // camthread ping watchdog reads. Even if the
                                // push itself fails (e.g. appsrc detached
                                // mid-rebuild), the fact that BcMedia is
                                // arriving on media_rx proves the BC
                                // connection is alive and the camera is
                                // streaming. We want the watchdog to see
                                // that, NOT to be tied to whether a downstream
                                // gstreamer appsrc is currently consuming.
                                let is_video_frame = matches!(
                                    data,
                                    BcMedia::Iframe(_) | BcMedia::Pframe(_)
                                );
                                if is_video_frame {
                                    frame_pump_last_frame_at
                                        .store(now_epoch_ms(), Ordering::Relaxed);
                                }
                                match send_to_sources(
                                    data,
                                    &mut pools,
                                    &vid_src,
                                    &aud_src,
                                    &mut vid_ts,
                                    &mut aud_ts,
                                    &stream_config,
                                ) {
                                    Ok(SendOutcome::Sent) => {
                                        if consecutive_errors > 0 {
                                            log::info!(
                                                "{name}::{stream}: send recovered after {consecutive_errors} errors"
                                            );
                                        }
                                        if consecutive_backpressure > 0 {
                                            log::info!(
                                                "{name}::{stream}: back-pressure recovered after {consecutive_backpressure} blocked pushes"
                                            );
                                        }
                                        consecutive_errors = 0;
                                        consecutive_backpressure = 0;
                                        consecutive_detached = 0;
                                        eos_signaled = false;
                                    }
                                    Ok(SendOutcome::BackPressured) => {
                                        consecutive_backpressure += 1;
                                        // Same Layer-2 recovery as the error
                                        // path: if back-pressure persists past
                                        // BACKPRESSURE_EOS_THRESHOLD, the
                                        // consumer is wedged. Signal EOS to
                                        // force client disconnect → factory
                                        // rebuild on next connect.
                                        if consecutive_backpressure == BACKPRESSURE_EOS_THRESHOLD
                                            && !eos_signaled
                                        {
                                            log::warn!(
                                                "{name}::{stream}: {BACKPRESSURE_EOS_THRESHOLD} consecutive back-pressured pushes — consumer stuck, signaling EOS and exiting thread to force rebuild"
                                            );
                                            if let Some(src) = vid_src.as_ref() {
                                                let _ = src.end_of_stream();
                                            }
                                            if let Some(src) = aud_src.as_ref() {
                                                let _ = src.end_of_stream();
                                            }
                                            eos_signaled = true;
                                        }
                                        if eos_signaled
                                            && consecutive_backpressure
                                                >= BACKPRESSURE_EOS_THRESHOLD + POST_EOS_GRACE
                                        {
                                            log::info!(
                                                "{name}::{stream}: exiting frame-pump thread after back-pressure EOS — factory callback will rebuild on next client connect"
                                            );
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        consecutive_errors += 1;
                                        // Terminal-detach fast path (fix 4):
                                        // "App source is closed" means the appsrc
                                        // left the bin (full unprepare) and will
                                        // never recover for THIS pipeline. Count
                                        // these specifically; after a short
                                        // confirmation window, exit immediately
                                        // so `pools` (socketpairs) and `media_rx`
                                        // (BC subscription) are dropped promptly
                                        // instead of leaking across the long
                                        // EOS_THRESHOLD+grace window. Matched on
                                        // the message check_live emits (bus None).
                                        let detached = e
                                            .to_string()
                                            .contains("App source is closed");
                                        if detached {
                                            consecutive_detached += 1;
                                            if consecutive_detached
                                                >= DETACHED_FAST_EXIT_THRESHOLD
                                            {
                                                log::info!(
                                                    "{name}::{stream}: appsrc detached (full unprepare) — fast-exiting frame-pump thread, freeing pools + BC subscription; factory rebuilds on next connect"
                                                );
                                                break;
                                            }
                                        } else {
                                            consecutive_detached = 0;
                                        }
                                        // Log sparsely (powers of 2) to avoid
                                        // spamming tens of thousands of lines
                                        // during a sustained outage.
                                        if consecutive_errors.is_power_of_two() {
                                            log::info!(
                                                "{name}::{stream}: send error #{consecutive_errors} (dropping frame): {e:?}"
                                            );
                                        }
                                        // Layer 2 self-recovery: at sustained
                                        // failure, proactively EOS the appsrcs
                                        // to force connected clients to
                                        // disconnect+reconnect, which triggers
                                        // a fresh factory callback → fresh
                                        // pipeline → fresh thread.
                                        if consecutive_errors == EOS_THRESHOLD
                                            && !eos_signaled
                                        {
                                            log::warn!(
                                                "{name}::{stream}: {EOS_THRESHOLD} consecutive errors — signaling EOS and exiting thread to force rebuild"
                                            );
                                            if let Some(src) = vid_src.as_ref() {
                                                let _ = src.end_of_stream();
                                            }
                                            if let Some(src) = aud_src.as_ref() {
                                                let _ = src.end_of_stream();
                                            }
                                            eos_signaled = true;
                                        }
                                        // Exit after EOS+grace so this
                                        // (now-doomed) thread doesn't
                                        // accumulate as a zombie alongside
                                        // the rebuilt thread. Dropping
                                        // media_rx here closes the upstream
                                        // BC subscription chain, freeing the
                                        // camera-side start_video resources.
                                        if eos_signaled
                                            && consecutive_errors
                                                >= EOS_THRESHOLD + POST_EOS_GRACE
                                        {
                                            log::info!(
                                                "{name}::{stream}: exiting frame-pump thread after EOS — factory callback will rebuild on next client connect"
                                            );
                                            break;
                                        }
                                    }
                                }
                            }
                            // fix 9 — ZOMBIE-CLIENT KICK (the camera C storm
                            // root-cause fix, 6 bursts on 2026-07-06).
                            //
                            // Every exit from the loop above means THIS
                            // pipeline can never deliver another frame: the
                            // appsrc is detached (terminal fast-exit, fix 4),
                            // or we EOS'd and gave up (error / back-pressure /
                            // egress-stall / starvation / orphan paths), or
                            // media_rx closed. All of those paths end with the same
                            // contract: "the factory rebuilds on the next
                            // client connect". The camera C storm proved that
                            // contract can never complete on its own: during a
                            // >30s camera outage the consumer's RTSP session
                            // expires SERVER-side (silent cleanup() / stale
                            // sweep) which unprepares the shared media, but
                            // go2rtc's TCP connection is never closed — so the
                            // only consumer sits alive-but-starved, never
                            // reconnects, and "next client connect" never
                            // happens. Frames flowed camera→neolink for 10–18
                            // minutes per burst while zero bytes egressed,
                            // until segment-watchdog restarted the container.
                            //
                            // Fix: on terminal pump exit, force-close the RTSP
                            // client connections still attached to this
                            // stream's paths (exact-path match) so the
                            // consumer SEES the death and reconnects into a
                            // fresh factory callback within seconds. If no
                            // client is attached (the normal fix 4 churn
                            // case: everyone already left), the kick is a
                            // no-op. Generation-guarded (see
                            // pipeline_generation above) so a stale pump can
                            // never kick a newer, healthy pipeline's clients.
                            if pipeline_generation.load(Ordering::SeqCst) == my_generation {
                                rtsp.kick_clients_of_paths(
                                    paths,
                                    format!("{name}::{stream}"),
                                    "frame-pump exited; this pipeline can never stream again"
                                        .to_string(),
                                );
                            } else {
                                log::debug!(
                                    "{name}::{stream}: frame-pump exiting without client kick — a newer pipeline generation exists"
                                );
                            }
                            log::trace!("{name}::{stream}: frame-pump thread done");
                            AnyResult::Ok(())
                        });
                        AnyResult::Ok(())
                    });
                }
            }
        }
        AnyResult::Ok(())
    });

    // Now setup the factory
    let factory = NeoMediaFactory::new_with_callback(move |element| {
        // fix 14 — DESCRIBE liveness gate. A dead camera must fail the
        // DESCRIBE in microseconds, not occupy the SHARED glib main-loop
        // thread building a pipeline that can never stream — and, worse, can
        // leave behind the post-EOS splash media whose complete-but-dataless
        // sender streams tripped the gst-rtsp-server get_rates g_assert
        // SIGABRT on the next PLAY (2026-08-14 storm, ~130 whole-process
        // crashes/hour; the C-side de-assert in
        // docker/gst-rtsp-media-deassert.patch is the backstop, this gate
        // closes the window that reaches it). Dead-state predicate shared
        // with make_dummy_factory — see liveness_gate_dead. Ok(None) -> gst
        // fails this DESCRIBE cleanly; go2rtc just retries until the camera
        // is back.
        if liveness_gate_dead(&gate_last_frame_at, &gate_armed_at) {
            log::info!(
                "create_element: {gate_name}::{stream}: camera not delivering frames (never-connected or stale >{GATE_STALENESS_MS}ms) — fast-failing DESCRIBE (fix 14 liveness gate)"
            );
            return Ok(None);
        }
        let (reply, new_element) = std::sync::mpsc::sync_channel(1);
        client_tx.blocking_send(ClientMsg::NewClient { element, reply })?;

        // BOUNDED wait — the load-bearing fix for the all-camera CLOSE_WAIT
        // wedge (2026-06-18). This closure runs on gst-rtsp-server's single
        // shared glib main-loop thread (create_element). The reply only comes
        // after the per-camera tokio task drains ~10 BC frames to learn the
        // codec and builds the bin; if that camera's stream is mid-reconnect
        // or wedged, an unbounded recv blocks this thread FOREVER, freezing
        // RTSP for EVERY camera → 200+ unanswered connections pile up in
        // CLOSE_WAIT → camera_fps=0 everywhere; only `docker restart neolink`
        // clears it. With a bounded recv the main loop blocks at most
        // BUILD_REPLY_TIMEOUT per stuck connect, keeps serving the other
        // cameras, and on timeout returns Err → build_pipeline maps it to
        // "media restarting" (Ok(None)) → gst fails this DESCRIBE cleanly and
        // closes the socket; go2rtc just retries until the stream is ready.
        // (fix 15: the build task itself abandons a learn phase that outlives
        // this wait — BUILD_LEARN_ABANDON — and discards a bin whose reply
        // finds this receiver gone, so a timed-out build leaks nothing.)
        let element = new_element.recv_timeout(BUILD_REPLY_TIMEOUT).map_err(|e| {
            log::warn!(
                "create_element: pipeline build did not reply within {:?} ({e:?}) — failing this DESCRIBE so the shared glib main loop stays free for the other cameras",
                BUILD_REPLY_TIMEOUT
            );
            e
        })?;
        Ok(Some(element))
    })
    .await?;
    Ok((factory, thread))
}

/// Outcome of a push into a single appsrc, or the aggregate result of a
/// send_to_sources call. Used by the frame-pump thread to distinguish a
/// healthy push from sustained back-pressure (push_buffer returning
/// Flushing). The Sent/BackPressured distinction drives the back-pressure
/// EOS watchdog — without it, a stuck downstream consumer wedges silently
/// because push_buffer→Flushing is not an Err.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SendOutcome {
    Sent,
    BackPressured,
}

/// fix 11: rate-limited "video appsrc near capacity" log. With
/// leaky-type=downstream the appsrc silently drops the oldest buffered frame
/// when full; this keeps those drops observable without spamming (the old
/// drop-newest code logged per dropped frame). Rate limit is process-global
/// (at most one line / 5s across all cameras) — enough signal, no flood.
fn log_video_near_full(appsrc: &AppSrc) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static LAST_LOG_MS: AtomicU64 = AtomicU64::new(0);
    const MIN_INTERVAL_MS: u64 = 5000;
    let now = now_epoch_ms();
    let prev = LAST_LOG_MS.load(Ordering::Relaxed);
    if now.saturating_sub(prev) >= MIN_INTERVAL_MS
        && LAST_LOG_MS
            .compare_exchange(prev, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    {
        log::debug!(
            "{}: video appsrc near capacity ({} / {} bytes) — leaky-downstream dropping oldest frames",
            appsrc.name(),
            appsrc.current_level_bytes(),
            appsrc.max_bytes()
        );
    }
}

fn send_to_sources(
    data: BcMedia,
    pools: &mut HashMap<usize, gstreamer::BufferPool>,
    vid_src: &Option<AppSrc>,
    aud_src: &Option<AppSrc>,
    vid_ts: &mut u64,
    aud_ts: &mut u64,
    stream_config: &StreamConfig,
) -> AnyResult<SendOutcome> {
    // Track whether ANY push in this call hit Flushing. The video path is
    // the only one that drives back-pressure recovery — audio is dropped
    // on near-full upstream so it can't generate sustained BackPressured.
    let mut outcome = SendOutcome::Sent;
    // Update TS
    match data {
        BcMedia::Aac(aac) => {
            let duration = aac.duration().expect("Could not calculate AAC duration");
            if let Some(aud_src) = aud_src.as_ref() {
                // Drop audio frames when buffer is nearly full to prevent
                // cascading backpressure that can stall the video pipeline
                let max = aud_src.max_bytes();
                if max > 0 && aud_src.current_level_bytes() >= max * 9 / 10 {
                    log::debug!("Audio buffer near capacity, dropping AAC frame");
                } else {
                    log::debug!("Sending AAC: {:?}", Duration::from_micros(*aud_ts));
                    if let SendOutcome::BackPressured = send_to_appsrc(
                        aud_src,
                        aac.data,
                        Duration::from_micros(*aud_ts),
                        pools,
                    )? {
                        outcome = SendOutcome::BackPressured;
                    }
                }
            }
            *aud_ts += duration as u64;
        }
        BcMedia::Adpcm(adpcm) => {
            let duration = adpcm
                .duration()
                .expect("Could not calculate ADPCM duration");
            if let Some(aud_src) = aud_src.as_ref() {
                let max = aud_src.max_bytes();
                if max > 0 && aud_src.current_level_bytes() >= max * 9 / 10 {
                    log::debug!("Audio buffer near capacity, dropping ADPCM frame");
                } else {
                    log::trace!("Sending ADPCM: {:?}", Duration::from_micros(*aud_ts));
                    if let SendOutcome::BackPressured = send_to_appsrc(
                        aud_src,
                        adpcm.data,
                        Duration::from_micros(*aud_ts),
                        pools,
                    )? {
                        outcome = SendOutcome::BackPressured;
                    }
                }
            }
            *aud_ts += duration as u64;
        }
        BcMedia::Iframe(BcMediaIframe { data, .. })
        | BcMedia::Pframe(BcMediaPframe { data, .. }) => {
            if let Some(vid_src) = vid_src.as_ref() {
                // fix 11: drop-OLDEST-under-pressure (was drop-NEWEST via
                // a34c36c). The video appsrc now carries leaky-type=downstream
                // (set in pipe_h264/pipe_h265), so when its internal queue hits
                // max-bytes it discards the OLDEST buffered frame and enqueues
                // this fresh one — bounding egress latency. The old a34c36c
                // guard skipped pushing the NEW frame when >=90% full, which
                // left stale frames queued and let latency grow under sustained
                // back-pressure (lesson from PR #400 discussion / LinuxMainframe
                // 16-camera fix). We therefore always push here; the appsrc
                // self-bounds by dropping oldest. `send_to_appsrc` still guards
                // the terminal "App source is closed" detach path (check_live)
                // and back-pressure Flushing — those protections are unchanged.
                // A rate-limited near-full log keeps the leaky drops observable.
                let max = vid_src.max_bytes();
                if max > 0 && vid_src.current_level_bytes() >= max * 9 / 10 {
                    log_video_near_full(vid_src);
                }
                log::trace!("Sending VID: {:?}", Duration::from_micros(*vid_ts));
                if let SendOutcome::BackPressured =
                    send_to_appsrc(vid_src, data, Duration::from_micros(*vid_ts), pools)?
                {
                    outcome = SendOutcome::BackPressured;
                }
            }
            const MICROSECONDS: u64 = 1000000;
            *vid_ts += MICROSECONDS / stream_config.fps as u64;
        }
        _ => {}
    }
    Ok(outcome)
}

fn bucket_size_for(n: usize) -> Option<usize> {
    const MIN_BUCKET: usize = 256;
    const MAX_BUCKET: usize = 1024 * 1024;
    if n == 0 {
        return Some(MIN_BUCKET);
    }
    if n > MAX_BUCKET {
        return None;
    }
    let mut b = n.next_power_of_two();
    if b < MIN_BUCKET {
        b = MIN_BUCKET;
    }
    Some(b)
}

fn acquire_pooled_buffer(
    pools: &mut std::collections::HashMap<usize, gstreamer::BufferPool>,
    data: &[u8],
    timestamp: gstreamer::ClockTime,
) -> AnyResult<gstreamer::Buffer> {
    let needed = data.len();
    if let Some(bucket) = bucket_size_for(needed) {
        let pool = pools.entry(bucket).or_insert_with(|| {
            let pool = gstreamer::BufferPool::new();
            let mut cfg = pool.config();
            // caps=None, size=bucket, min=8, max=64
            cfg.set_params(None, bucket as u32, 8, 64);
            pool.set_config(cfg).expect("pool config failed");
            pool.set_active(true).expect("activate pool");
            log::info!("New BufferPool (Bucket) allocated: size={bucket}");
            pool
        });

        let mut buf = pool.acquire_buffer(None)?;
        {
            let buf_ref = buf.get_mut().unwrap();
            buf_ref.set_dts(timestamp);
            buf_ref.set_pts(timestamp);
            {
                let mut map = buf_ref.map_writable().unwrap();
                map[..needed].copy_from_slice(data);
            }
            if bucket > needed {
                let _ = buf_ref.set_size(needed);
            }
        }
        Ok(buf)
    } else {
        // Fallback without pooling
        let mut buf = gstreamer::Buffer::with_size(needed)
            .context("allocate large non-pooled buffer")?;
        {
            let buf_ref = buf.get_mut().unwrap();
            buf_ref.set_dts(timestamp);
            buf_ref.set_pts(timestamp);
            let mut map = buf_ref.map_writable().unwrap();
            map.copy_from_slice(data);
        }
        Ok(buf)
    }
}


fn send_to_appsrc(
    appsrc: &gstreamer_app::AppSrc,
    data: Vec<u8>,
    mut ts: std::time::Duration,
    pools: &mut std::collections::HashMap<usize, gstreamer::BufferPool>,
) -> AnyResult<SendOutcome> {
    check_live(appsrc)?; // Stop if appsrc is dropped

    // In live mode we follow the advice in
    // https://gstreamer.freedesktop.org/documentation/additional/design/element-source.html?gi-language=c#live-sources
    // Only push buffers when in play state and have a clock
    // we also timestamp at the current time
    if appsrc.is_live() {
        if let Some(time) = appsrc
            .current_clock_time()
            .and_then(|t| appsrc.base_time().map(|bt| t - bt))
        {
            if matches!(appsrc.current_state(), gstreamer::State::Playing) {
                ts = Duration::from_micros(time.useconds());
            } else {
		// Not playing — treat as a no-op, not back-pressure.
                return Ok(SendOutcome::Sent);
            }
        } else {
	    // Clock not up yet — same.
            return Ok(SendOutcome::Sent);
        }
    }

    let timestamp = ClockTime::from_useconds(ts.as_micros() as u64);
    let buf = acquire_pooled_buffer(pools, &data, timestamp)?;

    match appsrc.push_buffer(buf) {
        Ok(_) => {}
        Err(gstreamer::FlowError::Flushing) => {
            log::info!(
                "Buffer full on {} pausing stream until client consumes frames",
                appsrc.name()
            );
            return Ok(SendOutcome::BackPressured);
        }
        Err(e) => return Err(anyhow::anyhow!("Error in streaming: {e:?}")),
    }

    // Backpressure-Logic
    let level = appsrc.current_level_bytes();
    let max = appsrc.max_bytes();
    if level >= max * 2 / 3 && matches!(appsrc.current_state(), gstreamer::State::Paused) {
        let _ = appsrc.set_state(gstreamer::State::Playing);
    } else if level <= max / 3 && matches!(appsrc.current_state(), gstreamer::State::Playing) {
        let _ = appsrc.set_state(gstreamer::State::Paused);
    }

    Ok(SendOutcome::Sent)
}

fn check_live(app: &AppSrc) -> Result<()> {
    app.bus().ok_or(anyhow!("App source is closed"))?;
    app.pads()
        .iter()
        .all(|pad| pad.is_linked())
        .then_some(())
        .ok_or(anyhow!("App source is not linked"))
}

fn clear_bin(bin: &Element) -> Result<()> {
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    // Clear the autogenerated ones
    for element in bin.iterate_elements().into_iter().flatten() {
        bin.remove(&element)?;
    }

    Ok(())
}

fn build_unknown(bin: &Element, pattern: &str) -> Result<()> {
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Unknown Pipeline");
    let source = make_element("videotestsrc", "testvidsrc")?;
    source.set_property_from_str("pattern", pattern);
    source.set_property("num-buffers", 500i32); // Send buffers then EOS
    let queue = make_queue("queue0", 1024 * 1024 * 4)?;

    let overlay = make_element("textoverlay", "overlay")?;
    overlay.set_property("text", "Stream not Ready");
    overlay.set_property_from_str("valignment", "top");
    overlay.set_property_from_str("halignment", "left");
    overlay.set_property("font-desc", "Sans, 16");
    let encoder = make_element("jpegenc", "encoder")?;
    let payload = make_element("rtpjpegpay", "pay0")?;

    bin.add_many([&source, &queue, &overlay, &encoder, &payload])?;
    source.link_filtered(
        &queue,
        &Caps::builder("video/x-raw")
            .field("format", "YUY2")
            .field("width", 896i32)
            .field("height", 512i32)
            .field("framerate", gstreamer::Fraction::new(25, 1))
            .build(),
    )?;
    Element::link_many([&queue, &overlay, &encoder, &payload])?;

    Ok(())
}

struct Linked {
    appsrc: AppSrc,
    output: Element,
}

fn pipe_h264(bin: &Element, stream_config: &StreamConfig) -> Result<Linked> {
    let buffer_size = buffer_size(stream_config.bitrate);
    log::debug!(
        "buffer_size: {buffer_size}, bitrate: {}",
        stream_config.bitrate
    );
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building H264 Pipeline");
    let source = make_element("appsrc", "vidsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;

    source.set_is_live(false);
    source.set_block(false);
    // fix 11: leaky-type=downstream — when the internal queue reaches
    // max-bytes, drop the OLDEST buffered frame and enqueue the new one,
    // rather than accumulating stale frames (growing latency) or wedging.
    // Lesson from PR #400's discussion (LinuxMainframe's 16-camera fix):
    // under sustained downstream back-pressure you want to shed stale data,
    // not the fresh frame. Property added in GStreamer 1.20; our runtime is
    // bookworm's 1.22. Set via property-string (same style as the wave/
    // emit-signals props here) so it is a no-op-safe runtime property set.
    // Video only — audio keeps its simpler drop-newest skip (buffers tiny).
    source.set_property_from_str("leaky-type", "downstream");
    source.set_min_latency(1000 / (stream_config.fps as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;
    let queue = make_queue("source_queue", buffer_size)?;
    let parser = make_element("h264parse", "parser")?;
    // let stamper = make_element("h264timestamper", "stamper")?;

    bin.add_many([&source, &queue, &parser])?;
    Element::link_many([&source, &queue, &parser])?;

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: parser,
    })
}

fn build_h264(bin: &Element, stream_config: &StreamConfig) -> Result<AppSrc> {
    let linked = pipe_h264(bin, stream_config)?;

    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;

    let payload = make_element("rtph264pay", "pay0")?;
    bin.add_many([&payload])?;
    Element::link_many([&linked.output, &payload])?;
    Ok(linked.appsrc)
}

fn pipe_h265(bin: &Element, stream_config: &StreamConfig) -> Result<Linked> {
    let buffer_size = buffer_size(stream_config.bitrate);
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building H265 Pipeline");
    let source = make_element("appsrc", "vidsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;
    source.set_is_live(false);
    source.set_block(false);
    // fix 11: leaky-type=downstream — drop OLDEST buffered frame under
    // pressure instead of accumulating stale frames. See pipe_h264 for the
    // full rationale (PR #400 discussion, GStreamer 1.20+ property).
    source.set_property_from_str("leaky-type", "downstream");
    source.set_min_latency(1000 / (stream_config.fps as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;
    let queue = make_queue("source_queue", buffer_size)?;
    let parser = make_element("h265parse", "parser")?;
    // let stamper = make_element("h265timestamper", "stamper")?;

    bin.add_many([&source, &queue, &parser])?;
    Element::link_many([&source, &queue, &parser])?;

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: parser,
    })
}

fn build_h265(bin: &Element, stream_config: &StreamConfig) -> Result<AppSrc> {
    let linked = pipe_h265(bin, stream_config)?;

    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;

    let payload = make_element("rtph265pay", "pay0")?;
    bin.add_many([&payload])?;
    Element::link_many([&linked.output, &payload])?;
    Ok(linked.appsrc)
}

fn pipe_aac(bin: &Element, stream_config: &StreamConfig) -> Result<Linked> {
    // Audio seems to run at about 800kbs
    let buffer_size = 512 * 1416;
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Aac pipeline");
    let source = make_element("appsrc", "audsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;

    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(1000 / (stream_config.fps as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;

    let queue = make_queue("audqueue", buffer_size)?;
    let parser = make_element("aacparse", "audparser")?;
    let decoder = match make_element("faad", "auddecoder_faad") {
        Ok(ele) => Ok(ele),
        Err(_) => make_element("avdec_aac", "auddecoder_avdec_aac"),
    }?;

    // The fallback
    let silence = make_element("audiotestsrc", "audsilence")?;
    silence.set_property_from_str("wave", "silence");
    let fallback_switch = make_element("fallbackswitch", "audfallbackswitch");
    if let Ok(fallback_switch) = fallback_switch.as_ref() {
        fallback_switch.set_property("timeout", 3u64 * 1_000_000_000u64);
        fallback_switch.set_property("immediate-fallback", true);
    }

    let encoder = make_element("audioconvert", "audencoder")?;

    bin.add_many([&source, &queue, &parser, &decoder, &encoder])?;
    if let Ok(fallback_switch) = fallback_switch.as_ref() {
        bin.add_many([&silence, fallback_switch])?;
        Element::link_many([
            &source,
            &queue,
            &parser,
            &decoder,
            fallback_switch,
            &encoder,
        ])?;
        Element::link_many([&silence, fallback_switch])?;
    } else {
        Element::link_many([&source, &queue, &parser, &decoder, &encoder])?;
    }

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: encoder,
    })
}

fn build_aac(bin: &Element, stream_config: &StreamConfig) -> Result<AppSrc> {
    let linked = pipe_aac(bin, stream_config)?;

    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;

    let payload = make_element("rtpL16pay", "pay1")?;
    bin.add_many([&payload])?;
    Element::link_many([&linked.output, &payload])?;
    Ok(linked.appsrc)
}

fn pipe_adpcm(bin: &Element, block_size: u32, stream_config: &StreamConfig) -> Result<Linked> {
    let buffer_size = 512 * 1416;
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Adpcm pipeline");
    // Original command line
    // caps=audio/x-adpcm,layout=dvi,block_align={},channels=1,rate=8000
    // ! queue silent=true max-size-bytes=10485760 min-threshold-bytes=1024
    // ! adpcmdec
    // ! audioconvert
    // ! rtpL16pay name=pay1

    let source = make_element("appsrc", "audsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;
    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(1000 / (stream_config.fps as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);

    source.set_caps(Some(
        &Caps::builder("audio/x-adpcm")
            .field("layout", "div")
            .field("block_align", block_size as i32)
            .field("channels", 1i32)
            .field("rate", 8000i32)
            .build(),
    ));

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;

    let queue = make_queue("audqueue", buffer_size)?;
    let decoder = make_element("decodebin", "auddecoder")?;
    let encoder = make_element("audioconvert", "audencoder")?;
    let encoder_out = encoder.clone();

    bin.add_many([&source, &queue, &decoder, &encoder])?;
    Element::link_many([&source, &queue, &decoder])?;
    decoder.connect_pad_added(move |_element, pad| {
        let sink_pad = encoder
            .static_pad("sink")
            .expect("Encoder is missing its pad");
        pad.link(&sink_pad)
            .expect("Failed to link ADPCM decoder to encoder");
    });

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: encoder_out,
    })
}

fn build_adpcm(bin: &Element, block_size: u32, stream_config: &StreamConfig) -> Result<AppSrc> {
    let linked = pipe_adpcm(bin, block_size, stream_config)?;

    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;

    let payload = make_element("rtpL16pay", "pay1")?;
    bin.add_many([&payload])?;
    Element::link_many([&linked.output, &payload])?;
    Ok(linked.appsrc)
}

#[allow(dead_code)]
fn pipe_silence(bin: &Element, stream_config: &StreamConfig) -> Result<Linked> {
    // Audio seems to run at about 800kbs
    let buffer_size = 512 * 1416;
    let bin = bin
        .clone()
        .dynamic_cast::<Bin>()
        .map_err(|_| anyhow!("Media source's element should be a bin"))?;
    log::debug!("Building Silence pipeline");
    let source = make_element("appsrc", "audsrc")?
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot cast to appsrc."))?;

    source.set_is_live(false);
    source.set_block(false);
    source.set_min_latency(1000 / (stream_config.fps as i64));
    source.set_property("emit-signals", false);
    source.set_max_bytes(buffer_size as u64);
    source.set_do_timestamp(false);
    source.set_stream_type(AppStreamType::Stream);

    let source = source
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot cast back"))?;

    let sink_queue = make_queue("audsinkqueue", buffer_size)?;
    let sink = make_element("fakesink", "silence_sink")?;

    let silence = make_element("audiotestsrc", "audsilence")?;
    silence.set_property_from_str("wave", "silence");
    let src_queue = make_queue("audsinkqueue", buffer_size)?;
    let encoder = make_element("audioconvert", "audencoder")?;

    bin.add_many([&source, &sink_queue, &sink, &silence, &src_queue, &encoder])?;

    Element::link_many([&source, &sink_queue, &sink])?;

    Element::link_many([&silence, &src_queue, &encoder])?;

    let source = source
        .dynamic_cast::<AppSrc>()
        .map_err(|_| anyhow!("Cannot convert appsrc"))?;
    Ok(Linked {
        appsrc: source,
        output: encoder,
    })
}

#[allow(dead_code)]
struct AppSrcPair {
    vid: AppSrc,
    aud: Option<AppSrc>,
}

// #[allow(dead_code)]
// /// Experimental build a stream of MPEGTS
// fn build_mpegts(bin: &Element, stream_config: &StreamConfig) -> Result<AppSrcPair> {
//     let buffer_size = buffer_size(stream_config.bitrate);
//     log::debug!(
//         "buffer_size: {buffer_size}, bitrate: {}",
//         stream_config.bitrate
//     );

//     // VID
//     let vid_link = match stream_config.vid_format {
//         VidFormat::H264 => pipe_h264(bin, stream_config)?,
//         VidFormat::H265 => pipe_h265(bin, stream_config)?,
//         VidFormat::None => unreachable!(),
//     };

//     // AUD
//     let aud_link = match stream_config.aud_format {
//         AudFormat::Aac => pipe_aac(bin, stream_config)?,
//         AudFormat::Adpcm(block) => pipe_adpcm(bin, block, stream_config)?,
//         AudFormat::None => pipe_silence(bin, stream_config)?,
//     };

//     let bin = bin
//         .clone()
//         .dynamic_cast::<Bin>()
//         .map_err(|_| anyhow!("Media source's element should be a bin"))?;

//     // MUX
//     let muxer = make_element("mpegtsmux", "mpeg_muxer")?;
//     let rtp = make_element("rtpmp2tpay", "pay0")?;

//     bin.add_many([&muxer, &rtp])?;
//     Element::link_many([&vid_link.output, &muxer, &rtp])?;
//     Element::link_many([&aud_link.output, &muxer])?;

//     Ok(AppSrcPair {
//         vid: vid_link.appsrc,
//         aud: Some(aud_link.appsrc),
//     })
// }

// Convenice funcion to make an element or provide a message
// about what plugin is missing
fn make_element(kind: &str, name: &str) -> AnyResult<Element> {
    ElementFactory::make_with_name(kind, Some(name)).with_context(|| {
        let plugin = match kind {
            "appsrc" => "app (gst-plugins-base)",
            "audioconvert" => "audioconvert (gst-plugins-base)",
            "adpcmdec" => "Required for audio",
            "h264parse" => "videoparsersbad (gst-plugins-bad)",
            "h265parse" => "videoparsersbad (gst-plugins-bad)",
            "rtph264pay" => "rtp (gst-plugins-good)",
            "rtph265pay" => "rtp (gst-plugins-good)",
            "rtpjitterbuffer" => "rtp (gst-plugins-good)",
            "aacparse" => "audioparsers (gst-plugins-good)",
            "rtpL16pay" => "rtp (gst-plugins-good)",
            "x264enc" => "x264 (gst-plugins-ugly)",
            "x265enc" => "x265 (gst-plugins-bad)",
            "avdec_h264" => "libav (gst-libav)",
            "avdec_h265" => "libav (gst-libav)",
            "videotestsrc" => "videotestsrc (gst-plugins-base)",
            "imagefreeze" => "imagefreeze (gst-plugins-good)",
            "audiotestsrc" => "audiotestsrc (gst-plugins-base)",
            "decodebin" => "playback (gst-plugins-good)",
            _ => "Unknown",
        };
        format!(
            "Missing required gstreamer plugin `{}` for `{}` element",
            plugin, kind
        )
    })
}

#[allow(dead_code)]
fn make_dbl_queue(name: &str, buffer_size: u32) -> AnyResult<Element> {
    let queue = make_element("queue", &format!("queue1_{}", name))?;
    queue.set_property("max-size-bytes", buffer_size);
    queue.set_property("max-size-buffers", 0u32);
    queue.set_property("max-size-time", 0u64);
    // queue.set_property(
    //     "max-size-time",
    //     std::convert::TryInto::<u64>::try_into(tokio::time::Duration::from_secs(5).as_nanos())
    //         .unwrap_or(0),
    // );

    let queue2 = make_element("queue2", &format!("queue2_{}", name))?;
    queue2.set_property("max-size-bytes", buffer_size * 2u32 / 3u32);
    queue.set_property("max-size-buffers", 0u32);
    queue.set_property("max-size-time", 0u64);
    queue2.set_property(
        "max-size-time",
        std::convert::TryInto::<u64>::try_into(tokio::time::Duration::from_secs(5).as_nanos())
            .unwrap_or(0),
    );
    queue2.set_property("use-buffering", false);

    let bin = gstreamer::Bin::builder().name(name).build();
    bin.add_many([&queue, &queue2])?;
    Element::link_many([&queue, &queue2])?;

    let pad = queue
        .static_pad("sink")
        .expect("Failed to get a static pad from queue.");
    let ghost_pad = GhostPad::builder_with_target(&pad).unwrap().build();
    ghost_pad.set_active(true)?;
    bin.add_pad(&ghost_pad)?;

    let pad = queue2
        .static_pad("src")
        .expect("Failed to get a static pad from queue2.");
    let ghost_pad = GhostPad::builder_with_target(&pad).unwrap().build();
    ghost_pad.set_active(true)?;
    bin.add_pad(&ghost_pad)?;

    let bin = bin
        .dynamic_cast::<Element>()
        .map_err(|_| anyhow!("Cannot convert bin"))?;
    Ok(bin)
}

fn make_queue(name: &str, buffer_size: u32) -> AnyResult<Element> {
    let queue = make_element("queue", &format!("queue1_{}", name))?;
    queue.set_property("max-size-bytes", buffer_size);
    queue.set_property("max-size-buffers", 0u32);
    queue.set_property("max-size-time", 0u64);
    queue.set_property(
        "max-size-time",
        std::convert::TryInto::<u64>::try_into(tokio::time::Duration::from_secs(5).as_nanos())
            .unwrap_or(0),
    );
    Ok(queue)
}

fn buffer_size(_bitrate: u32) -> u32 {
    // Fixed 10 MB video buffer. Original formula (bitrate*2/8) gave ~1.5 MB
    // for our 6 Mbps 4K H.265 camera A stream — tight enough that a single
    // oversize I-frame plus a brief consumer stall would fill to the 90%
    // drop threshold. 10 MB is ~13 s of average bitrate / ~3 s of
    // sustained peak, comfortably above any realistic go2rtc/Frigate
    // hiccup. RAM cost is trivial (per-stream, one stream).
    10 * 1024 * 1024
}
