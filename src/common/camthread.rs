use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Weak,
};
use tokio::{
    sync::watch::{Receiver as WatchReceiver, Sender as WatchSender},
    time::{interval, sleep, timeout, Duration, Instant},
};
use tokio_util::sync::CancellationToken;

use crate::{config::CameraConfig, utils::connect_and_login, AnyResult};
use neolink_core::bc_protocol::BcCamera;

/// Liveness is measured by frame-arrival, not BC pings.
///
/// Why: the Reolink Elite WiFi panorama (camera B, 5120x1552 HEVC) saturates
/// its CPU on the main-stream encoder and reliably misses BC `Ping` replies
/// every ~50-60s while frames keep flowing the entire time. Under the prior
/// implementation, 5 missed pings (~25s) returned an Err from run_camera and
/// triggered a full BC reconnect, which dropped the camera_watch and tore down
/// every appsrc ("App source is closed" cascade). The pipeline rebuilt cleanly
/// thanks to the EOS-and-exit work in factory.rs, but the user-visible result
/// was 3-5s recording segments instead of the configured 10s.
///
/// The fix: frames-flowing is the authoritative liveness signal. If we've
/// received a frame in the last `FRAME_STALENESS_MS`, the camera is alive and
/// any ping miss is treated as informational. BC pings are still sent (cheap
/// keepalive, useful as a probe) and logged on miss, but they no longer drive
/// the watchdog state machine. Other BC errors (login failure, real protocol
/// errors) still propagate as before — only the *timeout* path is demoted.
const FRAME_STALENESS_MS: u64 = 30_000;

#[derive(Eq, PartialEq, Copy, Clone)]
pub(crate) enum NeoCamThreadState {
    Connected,
    Disconnected,
}

pub(crate) struct NeoCamThread {
    state: WatchReceiver<NeoCamThreadState>,
    config: WatchReceiver<CameraConfig>,
    cancel: CancellationToken,
    camera_watch: WatchSender<Weak<BcCamera>>,
    /// Epoch millis of the last successful frame push from the rtsp factory.
    /// Updated by `factory.rs::send_to_sources` on every push. Read here in
    /// the ping watchdog to decide whether to honor a ping timeout.
    last_frame_at: Arc<AtomicU64>,
}

impl NeoCamThread {
    pub(crate) async fn new(
        watch_state_rx: WatchReceiver<NeoCamThreadState>,
        watch_config_rx: WatchReceiver<CameraConfig>,
        camera_watch_tx: WatchSender<Weak<BcCamera>>,
        cancel: CancellationToken,
        last_frame_at: Arc<AtomicU64>,
    ) -> Self {
        Self {
            state: watch_state_rx,
            config: watch_config_rx,
            cancel,
            camera_watch: camera_watch_tx,
            last_frame_at,
        }
    }
    async fn run_camera(&mut self, config: &CameraConfig) -> AnyResult<()> {
        let name = config.name.clone();
        log::trace!("Attempting connection with config: {config:?}");
        let camera = Arc::new(connect_and_login(config).await?);
        log::trace!("  - Connected");

        sleep(Duration::from_secs(2)).await; // Delay a little since some calls will error if camera is waking up
        if let Err(e) = update_camera_time(&camera, &name, config.update_time).await {
            log::warn!("Could not set camera time, (perhaps missing on this camera of your login in not an admin): {e:?}");
        }
        sleep(Duration::from_secs(2)).await; // Delay a little since some calls will error if camera is waking up

        self.camera_watch.send_replace(Arc::downgrade(&camera));

        let cancel_check = self.cancel.clone();
        let last_frame_at = self.last_frame_at.clone();
        let watchdog_name = name.clone();
        // Now we wait for a disconnect
        tokio::select! {
            _ = cancel_check.cancelled() => {
                AnyResult::Ok(())
            }
            v = camera.join() => {
                v?;
                Ok(())
            },
            v = async {
                let mut interval = interval(Duration::from_secs(5));
                let mut missed_pings: u32 = 0;
                loop {
                    interval.tick().await;
                    log::trace!("Sending ping");
                    match timeout(Duration::from_secs(5), camera.get_linktype()).await {
                        Ok(Ok(_)) => {
                            log::trace!("Ping reply");
                            if missed_pings > 0 {
                                log::info!(
                                    "{watchdog_name}: BC ping recovered after {missed_pings} miss(es)"
                                );
                            }
                            missed_pings = 0;
                            continue
                        },
                        Ok(Err(neolink_core::Error::UnintelligibleReply { reply, why })) => {
                            // Camera does not support pings just wait forever.
                            // Frame-staleness watchdog (below) is still active.
                            log::trace!("Pings not supported: {reply:?}: {why}");
                            // Fall through into a frames-only watchdog loop.
                            loop {
                                interval.tick().await;
                                if frames_stale(&last_frame_at) {
                                    log::error!(
                                        "{watchdog_name}: no frames for >{FRAME_STALENESS_MS}ms (pings unsupported), declaring camera dead"
                                    );
                                    break;
                                }
                            }
                            break Err(anyhow::anyhow!(
                                "Frame-staleness watchdog: no frames for >{FRAME_STALENESS_MS}ms"
                            ));
                        },
                        Ok(Err(e)) => {
                            // Real BC error (not a timeout) — these still
                            // tear down. Examples: connection reset, protocol
                            // error, login revoked. Frames couldn't be
                            // flowing if the BC connection itself errored,
                            // so no need to consult last_frame_at here.
                            break Err(e.into());
                        },
                        Err(_) => {
                            // Ping reply timeout. Under the old logic this
                            // counted toward 5 strikes and then forced a BC
                            // reconnect. That's wrong for camera B, which
                            // misses pings under encode load even while
                            // pumping frames at full rate.
                            //
                            // New logic: a ping timeout is just a log line.
                            // Tear-down only happens if frames have ALSO
                            // stopped (frames_stale returns true). Other BC
                            // errors above still tear down immediately.
                            missed_pings = missed_pings.saturating_add(1);
                            let stale = frames_stale(&last_frame_at);
                            if stale {
                                log::error!(
                                    "{watchdog_name}: ping timeout #{missed_pings} AND no frames for >{FRAME_STALENESS_MS}ms — declaring camera dead"
                                );
                                break Err(anyhow::anyhow!(
                                    "Frame-staleness watchdog: no frames for >{FRAME_STALENESS_MS}ms"
                                ));
                            } else {
                                // Log every miss at INFO so the issue is
                                // visible, but don't escalate.
                                log::info!(
                                    "{watchdog_name}: BC ping timeout #{missed_pings} (frames still arriving — not declaring dead)"
                                );
                                continue;
                            }
                        }
                    }
                }
            } => v,
        }?;

        let _ = camera.logout().await;
        let _ = camera.shutdown().await;

        Ok(())
    }

    // Will run and attempt to maintain the connection
    //
    // A watch sender is used to send the new camera
    // whenever it changes
    pub(crate) async fn run(&mut self) -> AnyResult<()> {
        const MAX_BACKOFF: Duration = Duration::from_secs(5);
        const MIN_BACKOFF: Duration = Duration::from_millis(50);

        let mut backoff = MIN_BACKOFF;

        loop {
            self.state
                .clone()
                .wait_for(|state| matches!(state, NeoCamThreadState::Connected))
                .await?;
            let mut config_rec = self.config.clone();

            let config = config_rec.borrow_and_update().clone();
            let now = Instant::now();
            let name = config.name.clone();

            let mut state = self.state.clone();

            let res = tokio::select! {
                Ok(_) = config_rec.changed() => {
                    None
                }
                Ok(_) = state.wait_for(|state| matches!(state, NeoCamThreadState::Disconnected)) => {
                    log::trace!("State changed to disconnect");
                    None
                }
                v = self.run_camera(&config) => {
                    Some(v)
                }
            };
            self.camera_watch.send_replace(Weak::new());

            if res.is_none() {
                // If None go back and reload NOW
                //
                // This occurs if there was a config change
                log::trace!("Config change or Manual disconnect");
                continue;
            }

            // Else we see what the result actually was
            let result = res.unwrap();

            if now.elapsed() > Duration::from_secs(60) {
                // Command ran long enough to be considered a success
                backoff = MIN_BACKOFF;
            }
            if backoff > MAX_BACKOFF {
                backoff = MAX_BACKOFF;
            }

            match result {
                Ok(()) => {
                    // Normal shutdown
                    log::trace!("Normal camera shutdown");
                    self.cancel.cancel();
                    return Ok(());
                }
                Err(e) => {
                    // An error
                    // Check if it is non-retry
                    let e_inner = e.downcast_ref::<neolink_core::Error>();
                    match e_inner {
                        Some(neolink_core::Error::CameraLoginFail) => {
                            // Fatal
                            log::error!("{name}: Login credentials were not accepted");
                            self.cancel.cancel();
                            return Err(e);
                        }
                        _ => {
                            // Non fatal
                            log::warn!("{name}: Connection Lost: {:?}", e);
                            log::info!("{name}: Attempt reconnect in {:?}", backoff);
                            sleep(backoff).await;
                            backoff *= 2;
                        }
                    }
                }
            }
        }
    }
}

impl Drop for NeoCamThread {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Current epoch milliseconds. Saturating; returns 0 if SystemTime is before
/// UNIX_EPOCH (which would only happen with a wildly misconfigured clock).
pub(crate) fn now_epoch_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// True if the last frame arrived more than `FRAME_STALENESS_MS` ago.
///
/// Returns false when `last_frame_at == 0`, i.e. before any frame has arrived.
/// This means the watchdog won't trigger on a brand-new camera that hasn't
/// produced its first frame yet — that case is handled by the BC connect
/// path (`connect_and_login` errors during `run_camera`'s start). Once we've
/// seen at least one frame, staleness is enforced.
fn frames_stale(last_frame_at: &Arc<AtomicU64>) -> bool {
    let last = last_frame_at.load(Ordering::Relaxed);
    if last == 0 {
        return false;
    }
    let now = now_epoch_ms();
    now.saturating_sub(last) > FRAME_STALENESS_MS
}

async fn update_camera_time(camera: &BcCamera, name: &str, update_time: bool) -> AnyResult<()> {
    let cam_time = camera.get_time().await?;
    let mut update = false;
    if let Some(time) = cam_time {
        log::info!("{}: Camera time is already set: {}", name, time);
        if update_time {
            update = true;
        }
    } else {
        update = true;
        log::warn!("{}: Camera has no time set, Updating", name);
    }
    if update {
        use std::time::SystemTime;
        let new_time = SystemTime::now();

        log::info!("{}: Setting time to {:?}", name, new_time);
        match camera.set_time(new_time.into()).await {
            Ok(_) => {
                let cam_time = camera.get_time().await?;
                if let Some(time) = cam_time {
                    log::info!("{}: Camera time is now set: {}", name, time);
                }
            }
            Err(e) => {
                log::error!(
                    "{}: Camera did not accept new time (is user an admin?): Error: {:?}",
                    name,
                    e
                );
            }
        }
    }
    Ok(())
}
