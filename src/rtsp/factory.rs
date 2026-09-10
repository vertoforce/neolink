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

use crate::{common::{now_epoch_ms, NeoInstance}, rtsp::gst::NeoMediaFactory, AnyResult};

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

pub(super) async fn make_dummy_factory(
    use_splash: bool,
    pattern: String,
) -> AnyResult<NeoMediaFactory> {
    NeoMediaFactory::new_with_callback(move |element| {
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
) -> AnyResult<(NeoMediaFactory, JoinHandle<AnyResult<()>>)> {
    let (client_tx, mut client_rx) = mpsc(100);
    // Create the task that creates the pipelines
    let thread = tokio::task::spawn(async move {
        let name = camera.config().await?.borrow().name.clone();
        // Per-camera frame-arrival cell. The frame-pump thread updates this
        // on every successful push_buffer; the camthread BC ping watchdog
        // reads it to decide whether to honor a ping timeout. See
        // common/camthread.rs for the design rationale.
        let last_frame_at = camera.last_frame_at().await?;

        while let Some(msg) = client_rx.recv().await {
            match msg {
                ClientMsg::NewClient { element, reply } => {
                    log::debug!("New client for {name}::{stream}");
                    let camera = camera.clone();
                    let name = name.clone();
                    let last_frame_at = last_frame_at.clone();
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
                        while let Some(media) = media_rx.recv().await {
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
                        {
                            let bin = element
                                .clone()
                                .dynamic_cast::<Bin>()
                                .map_err(|_| anyhow!("pipeline element should be a bin"))?;
                            if let Some(pay0) = bin.by_name("pay0") {
                                if let Some(srcpad) = pay0.static_pad("src") {
                                    let egress_count_probe = egress_count.clone();
                                    let _ = srcpad.add_probe(
                                        gstreamer::PadProbeType::BUFFER,
                                        move |_pad, _info| {
                                            egress_count_probe
                                                .fetch_add(1, Ordering::Relaxed);
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
                        // Send the pipeline back to the factory so it can start
                        let _ = reply.send(element);

                        // Run blocking code on a seperate thread
                        // This is not an async thread
                        let frame_pump_last_frame_at = last_frame_at.clone();
                        let egress_count_pump = egress_count.clone();
                        std::thread::spawn(move || {
                            let mut aud_ts: u64 = 0;
                            let mut vid_ts: u64 = 0;
                            let mut pools = Default::default();
                            // fix 8: per-track last-successful-push clocks for
                            // the audio-stall exit (see check below).
                            let mut push_times = PushTimes::default();
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
                            // AUDIO-STALL exit (fix 8). Motivating incident
                            // 2026-07-03: camera A's long-lived RTSP session went
                            // AUDIO-ONLY wedged — video kept flowing but the
                            // audio track delivered ~60 packets then went
                            // permanently silent (a FRESH session to the same
                            // camera had working audio). Browser MSE players
                            // spin forever on the empty audio track. No
                            // existing watchdog catches this: egress (pay0 =
                            // video) advances, sends succeed, no errors, no
                            // back-pressure. If the stall is neolink-side,
                            // exiting the pump so the factory rebuilds the
                            // session self-heals it. Guards (ALL required):
                            //   (a) this stream advertises audio (aud_src set),
                            //   (b) audio pushed successfully at least once
                            //       this session (audio flowed, THEN stopped —
                            //       video-only / audio-disabled sessions can
                            //       never trip),
                            //   (c) no successful audio push for >=
                            //       AUDIO_STALL_EXIT_SECS,
                            //   (d) video pushed successfully within the last
                            //       AUDIO_STALL_VIDEO_FRESH_MS (whole-stream
                            //       outages/reconnects stay owned by the
                            //       existing watchdogs and never trip this).
                            // Exit reuses the egress-stall EOS machinery
                            // verbatim (EOS both srcs → EGRESS_POST_EOS_GRACE_MS
                            // → the same terminal break fix 4 established) —
                            // no new teardown mechanics.
                            const AUDIO_STALL_EXIT_SECS: u64 = 10;
                            const AUDIO_STALL_VIDEO_FRESH_MS: u64 = 2_000;

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
                                    &mut push_times,
                                );
                            }

                            log::trace!("{name}::{stream}: Sending new frames");
                            while let Some(data) = media_rx.blocking_recv() {
                                // EGRESS liveness check. Runs on every arriving
                                // frame (the loop keeps spinning during the
                                // silent wedge precisely because frames are
                                // still ARRIVING — that's the failure mode).
                                // Compare the pay0 egress counter against its
                                // last-advanced value/time.
                                {
                                    let egress_now = egress_count_pump.load(Ordering::Relaxed);
                                    let now_ms = now_epoch_ms();
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
                                        egress_last_count = egress_now;
                                        egress_last_at = now_ms;
                                    } else if egress_last_at != 0
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
                                    }
                                    // Independent of the egress if/else chain
                                    // above (which takes the "counter advanced"
                                    // branch on virtually every frame while
                                    // video flows — exactly when THIS check
                                    // must run).
                                    if egress_eos_at == 0
                                        && aud_src.is_some()
                                        && push_times.audio_ms != 0
                                        && now_ms.saturating_sub(push_times.audio_ms)
                                            >= AUDIO_STALL_EXIT_SECS * 1000
                                        && push_times.video_ms != 0
                                        && now_ms.saturating_sub(push_times.video_ms)
                                            <= AUDIO_STALL_VIDEO_FRESH_MS
                                    {
                                        // Audio-stall exit (fix 8) — see the
                                        // AUDIO_STALL_EXIT_SECS doc-comment.
                                        log::warn!(
                                            "{name}::{stream}: audio stalled {}s while video flows — exiting frame-pump so factory rebuilds session (fix 8)",
                                            now_ms.saturating_sub(push_times.audio_ms) / 1000
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
                                }
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
                                    &mut push_times,
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
        const BUILD_REPLY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(8);
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

/// fix 8: epoch-ms of the last SUCCESSFUL (Sent, not back-pressured, not
/// dropped-on-near-full) push into each appsrc. Written by send_to_sources,
/// read by the frame-pump's audio-stall check. 0 = never pushed this session.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct PushTimes {
    pub(crate) video_ms: u64,
    pub(crate) audio_ms: u64,
}

fn send_to_sources(
    data: BcMedia,
    pools: &mut HashMap<usize, gstreamer::BufferPool>,
    vid_src: &Option<AppSrc>,
    aud_src: &Option<AppSrc>,
    vid_ts: &mut u64,
    aud_ts: &mut u64,
    stream_config: &StreamConfig,
    push_times: &mut PushTimes,
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
                    match send_to_appsrc(
                        aud_src,
                        aac.data,
                        Duration::from_micros(*aud_ts),
                        pools,
                    )? {
                        SendOutcome::BackPressured => outcome = SendOutcome::BackPressured,
                        SendOutcome::Sent => push_times.audio_ms = now_epoch_ms(),
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
                    match send_to_appsrc(
                        aud_src,
                        adpcm.data,
                        Duration::from_micros(*aud_ts),
                        pools,
                    )? {
                        SendOutcome::BackPressured => outcome = SendOutcome::BackPressured,
                        SendOutcome::Sent => push_times.audio_ms = now_epoch_ms(),
                    }
                }
            }
            *aud_ts += duration as u64;
        }
        BcMedia::Iframe(BcMediaIframe { data, .. })
        | BcMedia::Pframe(BcMediaPframe { data, .. }) => {
            if let Some(vid_src) = vid_src.as_ref() {
                // Mirror the audio drop-on-near-full (upstream PR #399)
                // for the video path too. Without this, a backpressure
                // spike on the video appsrc caused `send_to_appsrc` to
                // fail → the frame-pump thread exited via `r?` → shared
                // pipeline orphaned → 1h+ DESCRIBE wedge (observed
                // 2026-04-21 06:28 "Buffer full on vidsrc" → silent
                // starve until segment-watchdog bounced neolink 1h 13m
                // later). Dropping a frame leaves a brief visual glitch
                // until the next keyframe; a wedged pipeline leaves a
                // black stream for minutes.
                let max = vid_src.max_bytes();
                if max > 0 && vid_src.current_level_bytes() >= max * 9 / 10 {
                    log::debug!("Video buffer near capacity, dropping video frame");
                } else {
                    log::trace!("Sending VID: {:?}", Duration::from_micros(*vid_ts));
                    match send_to_appsrc(vid_src, data, Duration::from_micros(*vid_ts), pools)? {
                        SendOutcome::BackPressured => outcome = SendOutcome::BackPressured,
                        SendOutcome::Sent => push_times.video_ms = now_epoch_ms(),
                    }
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
