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
// pub(crate): fix 13 — the RTSP frame-pump's camera-frame starvation exit
// (rtsp/factory.rs) references this same canonical threshold instead of
// hardcoding its own literal.
pub(crate) const FRAME_STALENESS_MS: u64 = 30_000;

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

        // Fresh grace window on every (re)connect: reset the staleness clock to
        // connection time so a slow-to-start camera (marginal WiFi link) gets a
        // full FRAME_STALENESS_MS to deliver its first frame. Without this the
        // watchdog inherits the pre-disconnect timestamp and re-trips within one
        // ~5s tick, producing a tight connect→declare-dead→reconnect loop that
        // never lets the camera re-establish its video stream.
        self.last_frame_at.store(now_epoch_ms(), Ordering::Relaxed);

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
                            // fix 13: enforce frame staleness on the ping-SUCCESS
                            // path too. Mode B of the 2026-07-31 camera C wedge:
                            // after a dead-declare + reconnect the camera answered
                            // pings but never delivered frames — frames_stale was
                            // only consulted in the ping-timeout arm, so a
                            // frames-dead/pings-alive camera was never re-declared
                            // dead and the starved RTSP pipeline wedged forever
                            // (zero neolink log lines for the camera during hours
                            // of dead air). Same threshold, same teardown path as
                            // the timeout arm. Accepted caveat: a camera with NO
                            // consumers also has no frames and will now cycle its
                            // BC connection every ~FRAME_STALENESS_MS; in this
                            // deployment every camera has a permanent preload /
                            // exec consumer, so frames always flow when healthy.
                            if tick_declares_dead(
                                PingTick::Answered,
                                frames_stale(&last_frame_at),
                            ) {
                                log::error!(
                                    "{watchdog_name}: pings OK but no frames for >{FRAME_STALENESS_MS}ms — declaring camera dead (fix 13)"
                                );
                                break Err(anyhow::anyhow!(
                                    "Frame-staleness watchdog: no frames for >{FRAME_STALENESS_MS}ms (pings healthy)"
                                ));
                            }
                            continue
                        },
                        Ok(Err(neolink_core::Error::UnintelligibleReply { reply, why })) => {
                            // Camera does not support pings just wait forever.
                            // Frame-staleness watchdog (below) is still active.
                            log::trace!("Pings not supported: {reply:?}: {why}");
                            // Fall through into a frames-only watchdog loop.
                            loop {
                                interval.tick().await;
                                if tick_declares_dead(
                                    PingTick::Unsupported,
                                    frames_stale(&last_frame_at),
                                ) {
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
                            if tick_declares_dead(
                                PingTick::Failed,
                                frames_stale(&last_frame_at),
                            ) {
                                break Err(e.into());
                            }
                            continue;
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
                            let stale = tick_declares_dead(
                                PingTick::TimedOut,
                                frames_stale(&last_frame_at),
                            );
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

/// What one watchdog tick learned from the BC keepalive ping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PingTick {
    /// The camera answered.
    Answered,
    /// The ping reply timed out.
    TimedOut,
    /// This camera does not implement BC pings at all.
    Unsupported,
    /// A real BC error: connection reset, protocol error, login revoked.
    Failed,
}

/// Whether this tick must declare the camera dead and tear the BC connection
/// down. Pure, so the policy can be table-tested; `run_camera`'s select arms
/// are the only callers and each passes its own `frames_stale` reading.
///
/// The whole point of the frame-arrival watchdog (`681cc41`, `fix 13`) lives
/// in this table: liveness is frames, not pings.
pub(crate) fn tick_declares_dead(tick: PingTick, frames_stale: bool) -> bool {
    match tick {
        // fix 13. A camera that answers pings but has stopped delivering
        // frames is dead — the camera C Mode-B wedge. Before fix 13 this arm
        // ignored frames entirely and such a camera was never re-declared.
        PingTick::Answered => frames_stale,
        // 681cc41. A ping timeout on its own is informational: camera B
        // saturates its encoder and misses replies every ~50-60s while frames
        // keep flowing. Only a timeout WITH stale frames is a death; the old
        // logic counted five misses and reconnected regardless.
        PingTick::TimedOut => frames_stale,
        // Pings unsupported: frames are the only signal there is.
        PingTick::Unsupported => frames_stale,
        // Real protocol errors still tear down immediately — frames cannot be
        // flowing if the BC connection itself errored.
        PingTick::Failed => true,
    }
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

#[cfg(test)]
mod tests {
    //! Regression tests for the frame-arrival watchdog: `681cc41` (BC-ping
    //! liveness replaced by frame arrival), `f2385e8`/fix 5 (reconnect grace
    //! window) and `60546ff`/fix 13 (staleness enforced on the ping-SUCCESS
    //! arm too).
    //!
    //! The base tree is commit `8708608` — upstream master plus PRs
    //! #373/#400/#399/#398 — whose watchdog has no concept of frames at all:
    //! it counts five missed pings and forces a BC reconnect. That policy
    //! cannot be linked against, so where a comparison is needed it is
    //! reconstructed here as `base_tick_declares_dead` and labelled as such.

    use super::*;

    /// The pre-`681cc41` policy, reconstructed from the base tree: five missed
    /// pings in a row is a death, frames are never consulted.
    const BASE_MISSED_PING_LIMIT: u32 = 5;
    fn base_tick_declares_dead(tick: PingTick, missed_pings: u32) -> bool {
        match tick {
            PingTick::Answered | PingTick::Unsupported => false,
            PingTick::TimedOut => missed_pings >= BASE_MISSED_PING_LIMIT,
            PingTick::Failed => true,
        }
    }

    fn frame_at(ms_ago: u64) -> Arc<AtomicU64> {
        Arc::new(AtomicU64::new(now_epoch_ms() - ms_ago))
    }

    #[test]
    fn no_frame_yet_is_not_stale() {
        // Zero means "nothing has arrived"; the connect path owns that case.
        assert!(!frames_stale(&Arc::new(AtomicU64::new(0))));
    }

    #[test]
    fn frames_go_stale_only_past_the_threshold() {
        assert!(!frames_stale(&frame_at(0)));
        assert!(!frames_stale(&frame_at(FRAME_STALENESS_MS - 1_000)));
        assert!(frames_stale(&frame_at(FRAME_STALENESS_MS + 1_000)));
    }

    /// fix 5. Before the fix, `run_camera` left `last_frame_at` holding the
    /// pre-disconnect timestamp, so a camera that had been down for a while was
    /// judged against a clock that was already stale and got declared dead on
    /// the first 5 s tick after logging back in — a connect/kill/reconnect loop
    /// that never let a marginal-WiFi camera push its first frame.
    #[test]
    fn a_reconnect_restarts_the_grace_window() {
        // The watchdog ticks every 5 s. Advancing the wall clock by t ticks is
        // the same as pulling `last_frame_at` back by t * 5 s.
        fn ticks_survived(stored: u64) -> usize {
            (0..12)
                .take_while(|t| {
                    let probe = Arc::new(AtomicU64::new(stored.saturating_sub(t * 5_000)));
                    !frames_stale(&probe)
                })
                .count()
        }

        // Two minutes of downtime, then a successful login.
        let last_frame_at = frame_at(120_000);

        // Base: no reset on connect, so the clock is already stale and the
        // camera is declared dead on the very first tick after logging in.
        assert!(frames_stale(&last_frame_at), "base: stale the moment it connects");
        assert_eq!(
            ticks_survived(last_frame_at.load(Ordering::Relaxed)),
            0,
            "base: dies on tick 0"
        );

        // Fixed (fix 5): run_camera stores now_epoch_ms() right after connect.
        last_frame_at.store(now_epoch_ms(), Ordering::Relaxed);
        assert!(!frames_stale(&last_frame_at), "fixed: fresh grace window");
        // Survives ticks 0..=6 (0-30 s) and dies at tick 7 (35 s) — the whole
        // of FRAME_STALENESS_MS is available for the first frame.
        assert_eq!(
            ticks_survived(last_frame_at.load(Ordering::Relaxed)),
            (FRAME_STALENESS_MS / 5_000) as usize + 1,
            "fixed: a full FRAME_STALENESS_MS of grace"
        );
    }

    /// fix 13, the camera C Mode-B wedge: pings answered, frames stopped.
    /// The base policy never looks at frames on a successful ping, so such a
    /// camera is never re-declared dead and its RTSP pipeline wedges.
    #[test]
    fn pings_answered_with_dead_frames_is_a_death() {
        assert!(tick_declares_dead(PingTick::Answered, true));
        assert!(!base_tick_declares_dead(PingTick::Answered, 0));
        // Healthy camera: unaffected.
        assert!(!tick_declares_dead(PingTick::Answered, false));
    }

    /// `681cc41`, the camera B scenario: the panorama saturates its encoder
    /// and misses ping replies while pumping frames at full rate. Count the
    /// tear-downs each policy produces over a minute of that.
    #[test]
    fn missed_pings_with_frames_flowing_no_longer_tear_the_camera_down() {
        let ticks = 12; // 12 x 5 s = 60 s
        let mut base_deaths = 0;
        let mut fixed_deaths = 0;
        let mut missed = 0;
        for _ in 0..ticks {
            missed += 1;
            if base_tick_declares_dead(PingTick::TimedOut, missed) {
                base_deaths += 1;
                missed = 0; // the base reconnected, resetting its counter
            }
            // Frames are arriving the whole time.
            if tick_declares_dead(PingTick::TimedOut, false) {
                fixed_deaths += 1;
            }
        }
        eprintln!(
            "60 s of missed pings with frames flowing: base tore the camera down \
             {base_deaths} time(s), fixed {fixed_deaths}"
        );
        assert_eq!(base_deaths, 2);
        assert_eq!(fixed_deaths, 0);
    }

    /// A ping timeout that coincides with stale frames is still a death, and a
    /// real BC error is a death whatever the frames say.
    #[test]
    fn genuine_failures_still_tear_the_camera_down() {
        assert!(tick_declares_dead(PingTick::TimedOut, true));
        assert!(tick_declares_dead(PingTick::Unsupported, true));
        assert!(!tick_declares_dead(PingTick::Unsupported, false));
        assert!(tick_declares_dead(PingTick::Failed, false));
        assert!(tick_declares_dead(PingTick::Failed, true));
    }
}
