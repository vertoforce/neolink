//! Attempts to subclass RtspServer
//!
//! We are now messing with gstreamer glib objects
//! expect issues

use super::AnyResult;
use crate::config::*;

use anyhow::Context;
use gstreamer::glib::{self, object_subclass, MainLoop, Object};
use gstreamer_rtsp::RTSPAuthMethod;
use gstreamer_rtsp_server::{
    gio::{TlsAuthenticationMode, TlsCertificate},
    prelude::*,
    subclass::prelude::*,
    RTSPAuth, RTSPFilterResult, RTSPServer, RTSPToken, RTSP_TOKEN_MEDIA_FACTORY_ROLE,
};
use log::*;
use std::{
    collections::{HashMap, HashSet},
    fs,
    sync::Arc,
};
use tokio::{
    sync::RwLock,
    task::JoinSet,
    time::{timeout, Duration},
};
use tokio_util::sync::CancellationToken;

glib::wrapper! {
    /// The wrapped RTSPServer
    pub(crate) struct NeoRtspServer(ObjectSubclass<NeoRtspServerImpl>) @extends RTSPServer;
}

impl Default for NeoRtspServer {
    fn default() -> Self {
        Self::new().unwrap()
    }
}

/// Per-session keepalive timeout handed to gst-rtsp-server (seconds).
///
/// Canonical location for the "30" that was previously a magic literal in the
/// `connect_new_session` handler. A healthy RTSP client (go2rtc preload,
/// Frigate ffmpeg) refreshes its session well inside this window via RTCP /
/// keepalive, so the value only governs how long an ABANDONED session lingers
/// before `RTSPSessionPool::cleanup()` reaps it. Kept at 30s: smaller risks
/// dropping a briefly-slow ffmpeg client; the stale-reap sweep below collapses
/// the half-dead (CLOSE_WAIT) window without touching this floor.
const SESSION_TIMEOUT_SECS: u32 = 30;

/// Stale-session reap margin for the periodic sweep (milliseconds).
///
/// WHY THIS EXISTS (root cause, measured 2026-06-01): camera B repeatedly
/// entered a non-self-healing `App source is closed` rebuild loop that only an
/// EXTERNAL container restart cleared (incidents 074224Z / 165009Z). The
/// frame-pump EOS+exit path fired and `create_element` DID rebuild the
/// pipeline, but the rebuild never stuck. At the wedge there were 9 RTSP
/// sessions stuck in CLOSE_WAIT on :8554, and the `connect_closed` reap
/// (commit d4b516b) had logged `reaped` ZERO times — because CLOSE_WAIT means
/// the peer half-closed and gst-rtsp-server's `closed` signal never fires.
/// With `set_shared(true)` + `SuspendMode::Reset`, the shared media stays
/// PREPARED while ANY session references it, so those zombie sessions blocked
/// a clean unprepare → the rebuilt appsrc inherited a torn-down media and
/// re-hit `App source is closed`, storming until segment-watchdog bounced the
/// process. camera B (5120 panorama) churns connections hardest (~6× the
/// wedge rate of camera A), so it lost this race first.
///
/// THE GAP: `connect_closed` covers clean TCP close; `cleanup()` only reaps at
/// the full 30s `SESSION_TIMEOUT_SECS`. A CLOSE_WAIT session sits in between
/// for up to ~30s — long enough to sustain the storm. This sweep additively
/// reaps a session that has gone quiet (stopped being touched by RTCP /
/// keepalive — exactly what happens when the peer half-closes into CLOSE_WAIT)
/// SOONER than the full timeout, WITHOUT lowering the 30s floor that protects
/// legitimately-slow clients on the clean path.
///
/// HOW WE DETECT "quiet" — correctly this time. `next_timeout_usec(now)`
/// returns, despite its name, the **milliseconds remaining until expiry**
/// (upstream `rtsp-session.c`: `res = GST_TIME_AS_MSECONDS(last_access - now)`,
/// clamped at 0; the `usec` refers to the `now` arg, which is monotonic µs).
/// A freshly-touched session reports `remaining ≈ timeout*1000` ms; as the
/// client goes quiet `remaining` decays toward 0. We therefore compute, ALL IN
/// MILLISECONDS:
///   since_touch_ms = timeout_ms - remaining_ms        (how long since touched)
/// and reap when `since_touch_ms >= timeout_ms - SESSION_STALE_REAP_MARGIN_MS`,
/// i.e. once the session is within MARGIN of its own expiry. Equivalently:
/// reap when `0 < remaining_ms <= SESSION_STALE_REAP_MARGIN_MS`.
///
/// Attempt #1 (neolink:local fix, reverted) got this WRONG two ways, proven
/// 2026-06-01: (1) it compared `remaining` (ms) against `timeout_us`
/// (microseconds), so `since_touch` was ALWAYS ≥ ~29.97e6 and EVERY session —
/// including freshly-touched active ones — exceeded the threshold and was
/// reaped, instantly storming `App source is closed` and zeroing all cams;
/// (2) it guarded its log with `if !reaped.is_empty()`, but `filter()` returns
/// only the sessions for which the closure returned `Ref` (per the C API:
/// "a GList with all sessions for which func returned GST_RTSP_FILTER_REF"),
/// never the Removed ones — so `reaped` was always empty and the damage was
/// silent. This version keeps everything in ms and counts removals itself.
///
/// 5s margin: `remaining` only drops this low when the client hasn't been
/// touched for ~25s of a 30s timeout — far past any healthy keepalive interval
/// (go2rtc preload / Frigate ffmpeg refresh every few seconds), so a live
/// session is never in this band; yet it fires ~5s before the 30s `cleanup()`
/// would, and the 2s sweep cadence means a genuinely dead session is gone
/// within ~5-7s of crossing the line instead of lingering the full 30s.
/// Overridable via NEOLINK_RTSP_SESSION_STALE_REAP_MARGIN_MS for tuning
/// against observed values without a rebuild.
const SESSION_STALE_REAP_MARGIN_MS: i64 = 5_000;

fn session_stale_reap_margin_ms() -> i64 {
    std::env::var("NEOLINK_RTSP_SESSION_STALE_REAP_MARGIN_MS")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(SESSION_STALE_REAP_MARGIN_MS)
}

/// Pure staleness predicate for the sweep — extracted so the ms/seconds
/// arithmetic that destroyed attempt #1 (`neolink:local fix`, reverted 2026-06-01)
/// is unit-testable without a live RTSP server. See
/// `SESSION_STALE_REAP_MARGIN_MS` for the full root cause.
///
/// `remaining_ms` is what `RTSPSession::next_timeout_usec(now)` actually
/// returns: MILLISECONDS to expiry, clamped at 0 (upstream `rtsp-session.c`
/// does `GST_TIME_AS_MSECONDS(...)`; the `usec` in the name refers to the
/// monotonic `now` ARGUMENT, not the return value). Everything here is
/// therefore in milliseconds.
///
/// A session is stale iff it has a real (non-zero) timeout AND its own
/// remaining-to-expiry has decayed into the narrow pre-expiry margin
/// (`remaining_ms <= margin_ms`, equivalently
/// `since_touch_ms >= timeout_secs*1000 - margin_ms`). `remaining_ms == 0`
/// (already expired, client may still hold it) counts as stale — fix 9 wants
/// those owners kicked, not silently left behind.
pub(crate) fn session_is_stale_ms(timeout_secs: u32, remaining_ms: i64, margin_ms: i64) -> bool {
    let timeout_ms = (timeout_secs as i64).saturating_mul(1000);
    timeout_ms > 0 && remaining_ms <= margin_ms
}

/// One ordered action of the fix 9 close-then-reap sweep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReapStep {
    /// Close the TCP connection of the Nth collected owning client.
    CloseOwner(usize),
    /// Pool-remove every stale session (single `filter()` pass).
    PoolReapStaleSessions,
}

/// The fix 9 ORDERING CONTRACT, expressed as data so it can be asserted in a
/// test instead of only being argued in a comment: every owning client is
/// closed BEFORE the pool reap.
///
/// Why the order matters (iso GATE Z, 2026-07-09): pool-removal cascades —
/// gst-rtsp-server's client watches the pool's `session-removed` signal and
/// detaches the session from the client — so a sweep that reaps first can no
/// longer find "the client owning this session" and the starved consumer stays
/// a zombie. The pre-fix9 sweep had no close step at all (see
/// `git show d90ce64^:src/rtsp/gst/server.rs`).
pub(crate) fn stale_reap_plan(n_owners: usize) -> Vec<ReapStep> {
    let mut plan: Vec<ReapStep> = (0..n_owners).map(ReapStep::CloseOwner).collect();
    plan.push(ReapStep::PoolReapStaleSessions);
    plan
}

/// Exact-path predicate for the zombie-client kick (fix 9).
///
/// `RTSPSessionMedia::matches(path)` (`gst_rtsp_session_media_matches`) returns
/// `Some(n)` when `path` STARTS WITH the session-media's own mount path, where
/// `n` is the length of that mount path — i.e. a bare prefix hit also returns
/// `Some`. Accepting any `Some` would let a `/<cam>/main` kick collateral-kill
/// a client attached to the bare `/<cam>` alias (and vice versa). Requiring
/// `n == candidate.len()` makes the match EXACT.
pub(crate) fn media_path_is_exact_match(matched: Option<i32>, candidate: &str) -> bool {
    matched.map(|m| m as usize == candidate.len()).unwrap_or(false)
}

/// Generation guard for the frame-pump terminal-exit kick (fix 9).
///
/// A frame-pump remembers the pipeline generation it was born under and may
/// only kick clients while it is STILL the newest generation; otherwise a slow
/// dying pump would kick the clients of the healthy pipeline that replaced it,
/// which is a kick loop. Extracted here as a pure predicate for testing.
///
/// NOTE: the live call site is the inline
/// The live guard on the frame-pump's terminal kick in `src/rtsp/factory.rs`
/// calls this, so the table test below is testing the shipped comparison.
pub(crate) fn kick_generation_is_current(my_generation: u64, current_generation: u64) -> bool {
    my_generation == current_generation
}

impl NeoRtspServer {
    pub(crate) fn new() -> AnyResult<Self> {
        gstreamer::init().context("Gstreamer failed to initialise")?;
        let factory = Object::new::<NeoRtspServer>();

        // Setup auth
        let auth = factory.auth().unwrap_or_default();
        auth.set_supported_methods(RTSPAuthMethod::Basic);
        let mut un_authtoken = RTSPToken::builder()
            .field(
                //RTSP_TOKEN_MEDIA_FACTORY_ROLE: Means look inside the media factory settings and use the same permissions this user (`"anonymous"`) has
                RTSP_TOKEN_MEDIA_FACTORY_ROLE,
                "anonymous",
            )
            .build();
        auth.set_default_token(Some(&mut un_authtoken));
        factory.set_auth(Some(&auth));

        factory.connect_client_connected(|_, client| {
            client.connect_new_session(|_, session| {
                log::debug!("New Session");
                // Session timeout too small causes us to drop
                // some ffmpeg clients too soon
                // Too long causes too many open connections with
                // clients like frigate (that seem to open multiple
                //   connections without shutting down old ones)
                session.set_timeout(SESSION_TIMEOUT_SECS);
            });
            // Proactive session reap on TCP close. Without this, when a
            // client's TCP connection drops without a TEARDOWN (common with
            // ffmpeg processes that exit on rw_timeout, with kill, or with
            // socket reset), the session lingers in the pool until its
            // 30s timeout expires. During that window the gst-rtsp-server
            // still counts the session as an active consumer of the shared
            // media — observed symptom is CLOSE_WAIT accumulation on :8554
            // (measured 7 stuck sessions / 7min uptime on 2026-05-20) plus
            // shared-pipeline back-pressure even when no real client is
            // draining. Forcing Remove here on `closed` collapses the
            // zombie window to ~0.
            client.connect_closed(|client| {
                // Count the removals OURSELVES. `session_filter()` returns
                // only the sessions the closure Ref'd — never the Removed
                // ones — so the old `if !removed.is_empty()` guard could
                // never fire and this reap was silent. MEASURED on a live
                // client 2026-09-10 (`claim2_live_client_session_filter_
                // returns_refd_not_removed`): filter returned 0 while 1
                // session was really detached. Same class of bug as the one
                // 7fe3ef3 fixed in the sweep.
                let mut removed_count: usize = 0;
                client.session_filter(Some(&mut |_client, _session| {
                    removed_count += 1;
                    RTSPFilterResult::Remove
                }));
                if removed_count > 0 {
                    log::info!(
                        "RTSP client closed — reaped {removed_count} session(s) immediately"
                    );
                }
            });
        });

        Ok(factory)
    }

    pub(crate) async fn run(&self, bind_addr: &str, bind_port: u16) -> AnyResult<()> {
        let server = self;
        server.set_address(bind_addr);
        server.set_service(&format!("{}", bind_port));
        // Attach server to default Glib context
        let _ = server.attach(None);
        let main_loop = Arc::new(MainLoop::new(None, false));

        // Run the Glib main loop.
        let main_loop_thread = main_loop.clone();
        let main_loop_cancel = CancellationToken::new();
        let main_loop_gaurd = main_loop_cancel.clone().drop_guard();
        let handle = tokio::task::spawn_blocking(move || {
            main_loop_thread.run();
            drop(main_loop_gaurd);
            AnyResult::Ok(())
        });
        timeout(Duration::from_secs(5), self.imp().threads.write())
            .await
            .with_context(|| "Timeout waiting to lock Server threads")?
            .spawn(async move { handle.await? });

        let clean_up_server = server.clone();
        let handle = tokio::task::spawn_blocking(move || {
            while !main_loop_cancel.is_cancelled() {
                if let Some(sessions) = clean_up_server.session_pool() {
                    let cleanups = sessions.cleanup();
                    if cleanups > 0 {
                        log::debug!("Cleaned up {cleanups} sessions");
                    }
                    // Stale-session reap (additive net for the CLOSE_WAIT case;
                    // see SESSION_STALE_REAP_MARGIN_MS doc-comment for the full
                    // root cause). `cleanup()` above only removes sessions past
                    // the full SESSION_TIMEOUT_SECS; the `connect_closed` hook
                    // only covers clean TCP close. Neither catches a half-closed
                    // (CLOSE_WAIT) client that has stopped being touched but is
                    // not yet expired — and those zombie sessions hold the
                    // SHARED media PREPARED, blocking the EOS-driven rebuild
                    // from ever sticking.
                    //
                    // ALL ARITHMETIC IN MILLISECONDS. `next_timeout_usec(now)`
                    // returns ms-remaining-to-expiry (clamped at 0); `now` is
                    // monotonic µs (the `usec` in the name is the arg, not the
                    // return). A live session reports remaining ≈ timeout_ms; a
                    // quiet/half-dead one decays toward 0. We reap only the
                    // narrow band just before expiry, so a session that is
                    // still being keepalive-touched (any healthy client) is
                    // NEVER in range. We count removals ourselves because
                    // `filter()` returns only the Ref'd sessions, not the
                    // Removed ones.
                    let margin_ms = session_stale_reap_margin_ms();
                    let now = glib::monotonic_time();
                    // fix 9 restructure: the sweep used to pool-Remove the
                    // stale session directly. That is invisible at the TCP
                    // level AND it cascades immediately — gst-rtsp-server's
                    // client watches the pool's session-removed signal and
                    // detaches the session from the client — so by the time
                    // anything later tries to find "the client owning this
                    // session", the client is session-less and unmatchable
                    // (proven in the iso GATE Z run 2026-07-09: neither a
                    // sessionid- nor a path-based lookup found the frozen
                    // consumer after the pool-Remove; it stayed a zombie).
                    // If the peer is genuinely CLOSE_WAIT (the case this
                    // sweep was built for), a client close() is a no-op
                    // cleanup; but if the peer is ALIVE and merely starved
                    // (camera C 2026-07-06: camera stopped delivering frames
                    // >30s, so RTP stopped, so the session went quiet — while
                    // go2rtc's TCP connection sat healthy and blocked on
                    // read), a silent reap turns it into a zombie that never
                    // reconnects. So the sweep now runs in three passes:
                    //   1) READ-ONLY pool filter: collect the stale band
                    //      (remaining <= margin, INCLUDING already-expired 0
                    //      — the client may still hold a session cleanup()
                    //      beat us to).
                    //   2) On the glib main context (owner of the client
                    //      watches): close() every client owning a stale
                    //      session — the TCP FIN/RST makes the expiry VISIBLE
                    //      so an alive-but-starved consumer reconnects
                    //      immediately; while its camera is still down the
                    //      DESCRIBEs fail fast (fix 6) and retry, recovering
                    //      the instant the camera returns instead of waiting
                    //      for segment-watchdog.
                    //   3) Then pool-Remove the stale sessions (same reap the
                    //      sweep always did, just after the kick instead of
                    //      before it).
                    let mut stale: Vec<(Option<glib::GString>, i64)> = Vec::new();
                    sessions.filter(Some(&mut |_, session| {
                        let timeout_secs = session.timeout();
                        let timeout_ms = (timeout_secs as i64).saturating_mul(1000);
                        let remaining_ms = session.next_timeout_usec(now) as i64;
                        let since_touch_ms = timeout_ms.saturating_sub(remaining_ms);
                        log::debug!(
                            "{:?}: remaining_ms={} timeout_ms={} since_touch_ms={}",
                            session.sessionid(),
                            remaining_ms,
                            timeout_ms,
                            since_touch_ms,
                        );
                        // Stale iff the session has a real (non-zero) timeout
                        // AND its remaining-to-expiry has decayed into the
                        // narrow pre-expiry margin. A freshly-/recently-
                        // touched live session has remaining far above the
                        // margin, so it is never selected here.
                        if session_is_stale_ms(timeout_secs, remaining_ms, margin_ms) {
                            stale.push((session.sessionid(), remaining_ms));
                        }
                        RTSPFilterResult::Keep
                    }));
                    if !stale.is_empty() {
                        log::info!(
                            "RTSP stale-session sweep — {} stale session(s) (remaining<={}ms of {}s timeout): {:?}; closing owners then reaping",
                            stale.len(),
                            margin_ms,
                            SESSION_TIMEOUT_SECS,
                            stale,
                        );
                        let stale_ids: Vec<glib::GString> =
                            stale.iter().filter_map(|(id, _)| id.clone()).collect();
                        let kick_server = clean_up_server.clone();
                        glib::MainContext::default().invoke(move || {
                            // Pass 2 — collect the owning clients first, close
                            // AFTER the filter returns (no client mutation
                            // while the server iterates its client list).
                            let mut owners: Vec<gstreamer_rtsp_server::RTSPClient> = Vec::new();
                            kick_server.client_filter(Some(&mut |_, client| {
                                let owns = std::cell::Cell::new(false);
                                client.session_filter(Some(&mut |_, session| {
                                    if session
                                        .sessionid()
                                        .map(|sid| stale_ids.contains(&sid))
                                        .unwrap_or(false)
                                    {
                                        owns.set(true);
                                    }
                                    RTSPFilterResult::Keep
                                }));
                                if owns.get() {
                                    owners.push(client.clone());
                                }
                                RTSPFilterResult::Keep
                            }));
                            // The close-before-reap ORDER is the whole point
                            // of the fix 9 restructure, so it is expressed as
                            // data (`stale_reap_plan`) and merely executed
                            // here — see that function for why reaping first
                            // makes the owner unmatchable, and for the test
                            // that pins the ordering.
                            let mut kicked: usize = 0;
                            let mut reaped_count: usize = 0;
                            for step in stale_reap_plan(owners.len()) {
                                match step {
                                    ReapStep::CloseOwner(i) => {
                                        owners[i].close();
                                        kicked += 1;
                                    }
                                    ReapStep::PoolReapStaleSessions => {
                                        if kicked > 0 {
                                            log::info!(
                                                "RTSP stale-session sweep — closed {kicked} client connection(s) owning stale session(s) (alive-but-starved peers now reconnect instead of zombieing)"
                                            );
                                        }
                                        // Pass 3 — the reap the sweep always
                                        // did. close() above already cascades
                                        // session removal for owned sessions
                                        // via the closed hook; this pass
                                        // catches the ownerless leftovers
                                        // (true CLOSE_WAIT with the client
                                        // object already gone). We count the
                                        // removals OURSELVES: `filter()`
                                        // returns only the sessions the
                                        // closure Ref'd, never the Removed
                                        // ones (the silent-damage half of the
                                        // attempt-#1 bug).
                                        if let Some(sessions) = kick_server.session_pool() {
                                            sessions.filter(Some(&mut |_, session| {
                                                if session
                                                    .sessionid()
                                                    .map(|sid| stale_ids.contains(&sid))
                                                    .unwrap_or(false)
                                                {
                                                    reaped_count += 1;
                                                    RTSPFilterResult::Remove
                                                } else {
                                                    RTSPFilterResult::Keep
                                                }
                                            }));
                                        }
                                        if reaped_count > 0 {
                                            log::debug!(
                                                "RTSP stale-session sweep — pool-reaped {reaped_count} stale session(s)"
                                            );
                                        }
                                    }
                                }
                            }
                        });
                    }
                }
                // 2s sweep handles the residual case where `closed` didn't
                // fire (e.g. client TCP RST without protocol close); the
                // closed-signal hook covers the common path. The stale-reap
                // filter above additionally collapses the CLOSE_WAIT window
                // (peer half-closed, `closed` signal never fires) that fed the
                // camera B rebuild storm — reaping such a session ~5s before
                // the 30s `cleanup()` would, without touching the 30s floor for
                // legitimately-slow clients on the clean path.
                std::thread::sleep(Duration::from_secs(2));
            }
            AnyResult::Ok(())
        });
        timeout(Duration::from_secs(5), self.imp().threads.write())
            .await
            .with_context(|| "Timeout waiting to lock Server threads")?
            .spawn(async move { handle.await? });

        // Put copy of main loop inside the rtsp server
        timeout(Duration::from_secs(5), self.imp().main_loop.write())
            .await
            .with_context(|| "Timeout waiting to lock Server main_loop")?
            .replace(main_loop);
        Ok(())
    }

    pub(crate) async fn quit(&self) -> AnyResult<()> {
        if let Some(main_loop) = self.imp().main_loop.read().await.as_ref() {
            main_loop.quit();
        }
        Ok(())
    }

    pub(crate) async fn join(&self) -> AnyResult<()> {
        let mut threads = self.imp().threads.write().await;
        while let Some(thread) = threads.join_next().await {
            thread??;
        }
        Ok(())
    }

    pub(crate) fn set_up_tls(&self, config: &Config) -> AnyResult<()> {
        self.imp().set_up_tls(config)
    }

    pub(crate) async fn add_user(&self, username: &str, password: &str) -> AnyResult<()> {
        self.imp().add_user(username, password).await
    }

    pub(crate) async fn remove_user(&self, username: &str) -> AnyResult<()> {
        self.imp().remove_user(username).await
    }

    pub(crate) async fn get_users(&self) -> AnyResult<HashSet<String>> {
        self.imp().get_users().await
    }

    /// fix 9 — zombie-client kick: force-close the TCP connection of every
    /// RTSP client whose session is attached to one of `paths`.
    ///
    /// WHY (the camera C storm, 6 bursts on 2026-07-06): when a marginal-WiFi
    /// camera stops delivering frames for >SESSION_TIMEOUT_SECS, RTP egress
    /// stops, so the consumer's RTSP session stops being touched and is
    /// silently expired server-side (the 30s `cleanup()` — which logs only at
    /// debug — or the stale-session sweep). Removing a session from the pool
    /// does NOT close the owning client's TCP connection: a TCP-interleaved
    /// consumer like go2rtc keeps its socket open and blocks on read forever.
    /// When the session release unprepares the shared media, the frame-pump
    /// correctly fast-exits on the terminal "App source is closed" (fix 4) —
    /// but its exit contract, "the factory rebuilds on the next client
    /// connect", never completes because the ONLY consumer still believes its
    /// existing connection is fine and never reconnects. Result: frames flow
    /// camera→neolink while the consumer starves for 10–18 min until
    /// segment-watchdog escalates to a container restart.
    ///
    /// FIX: make the breakage VISIBLE at the TCP level. `RTSPClient::close()`
    /// (gst_rtsp_client_close: "Close the connection of client and remove all
    /// media it was managing") sends the peer a FIN/RST; go2rtc's reconnect
    /// loop then re-DESCRIBEs within seconds, driving a fresh factory
    /// callback → fresh pipeline → fresh appsrc. If the peer really was gone
    /// (genuine CLOSE_WAIT), close() is a no-op cleanup.
    ///
    /// Path matching is EXACT (matches() reports the matched byte count; we
    /// require it to equal the candidate path's length) so a mainStream kick
    /// can never collateral-kick a subStream client via the bare "/<cam>"
    /// alias prefix.
    ///
    /// Runs on the glib main context (the thread that owns the client watches)
    /// rather than the caller's thread — same discipline as every other
    /// client-list mutation in gst-rtsp-server. The closure is non-blocking
    /// and O(clients × sessions), so it cannot re-create the fix 6
    /// frozen-main-loop hazard. Fire-and-forget by design: the caller (a
    /// dying frame-pump thread or the sweep) must not block on the glib loop.
    pub(crate) fn kick_clients_of_paths(
        &self,
        paths: Arc<Vec<String>>,
        label: String,
        reason: String,
    ) {
        let server = self.clone();
        glib::MainContext::default().invoke(move || {
            // Collect matches first, close AFTER the filter returns — no
            // client mutation while the server iterates its client list.
            let mut matched: Vec<gstreamer_rtsp_server::RTSPClient> = Vec::new();
            server.client_filter(Some(&mut |_, client| {
                // Cell instead of `mut bool`: the nested FnMut filter
                // closures would otherwise need overlapping &mut borrows.
                let attached = std::cell::Cell::new(false);
                client.session_filter(Some(&mut |_, session| {
                    session.filter(Some(&mut |_, media| {
                        if !attached.get()
                            && paths
                                .iter()
                                .any(|p| media_path_is_exact_match(media.matches(p), p))
                        {
                            attached.set(true);
                        }
                        RTSPFilterResult::Keep
                    }));
                    RTSPFilterResult::Keep
                }));
                if attached.get() {
                    matched.push(client.clone());
                }
                RTSPFilterResult::Keep
            }));
            let kicked = matched.len();
            for client in matched {
                client.close();
            }
            if kicked > 0 {
                log::info!(
                    "{label}: kicked {kicked} RTSP client connection(s) — {reason}; consumers will reconnect into a fresh pipeline"
                );
            } else {
                // fix 13 observability: promoted from debug. A kick that
                // matches zero clients while a consumer believes it is
                // connected is exactly the invisible signature of the
                // 2026-07-31 camera C dead-air incidents — it must be
                // visible at the default RUST_LOG=info.
                log::info!(
                    "{label}: zombie-client kick found no attached clients ({reason})"
                );
            }
        });
    }
}

unsafe impl Send for NeoRtspServer {}
unsafe impl Sync for NeoRtspServer {}

#[derive(Default)]
pub(crate) struct NeoRtspServerImpl {
    threads: RwLock<JoinSet<AnyResult<()>>>,
    users: RwLock<HashMap<String, String>>,
    main_loop: RwLock<Option<Arc<MainLoop>>>,
}

impl ObjectImpl for NeoRtspServerImpl {}
impl RTSPServerImpl for NeoRtspServerImpl {}

#[object_subclass]
impl ObjectSubclass for NeoRtspServerImpl {
    const NAME: &'static str = "NeoRtspServer";
    type Type = NeoRtspServer;
    type ParentType = RTSPServer;
}

impl NeoRtspServerImpl {
    pub(crate) fn set_tls(
        &self,
        cert_file: &str,
        client_auth: TlsAuthenticationMode,
    ) -> AnyResult<()> {
        debug!("Setting up TLS using {}", cert_file);
        let auth = self.obj().auth().unwrap_or_default();

        // We seperate reading the file and changing to a PEM so that we get different error messages.
        let cert_contents = fs::read_to_string(cert_file).with_context(|| "TLS file not found")?;
        let cert = TlsCertificate::from_pem(&cert_contents)
            .with_context(|| "Not a valid TLS certificate")?;
        auth.set_tls_certificate(Some(&cert));
        auth.set_tls_authentication_mode(client_auth);

        self.obj().set_auth(Some(&auth));
        Ok(())
    }

    pub(crate) fn set_up_tls(&self, config: &Config) -> AnyResult<()> {
        let tls_client_auth = match &config.tls_client_auth as &str {
            "request" => TlsAuthenticationMode::Requested,
            "require" => TlsAuthenticationMode::Required,
            "none" => TlsAuthenticationMode::None,
            _ => unreachable!(),
        };
        if let Some(cert_path) = &config.certificate {
            self.set_tls(cert_path, tls_client_auth)
                .with_context(|| "Failed to set up TLS")?;
        }
        Ok(())
    }

    pub(crate) async fn add_user(&self, username: &str, password: &str) -> AnyResult<()> {
        let mut locked_users = self.users.write().await;
        let auth = self.obj().auth().unwrap();

        let token = RTSPToken::builder()
            .field(RTSP_TOKEN_MEDIA_FACTORY_ROLE, username)
            .build();
        let basic = RTSPAuth::make_basic(username, password);

        if let Some(old_basic) = locked_users.get(username) {
            if basic.as_str() == old_basic {
                // Password is the same
                return Ok(());
            } else {
                // Different password
                auth.remove_basic(old_basic);
            }
        }

        auth.add_basic(basic.as_str(), &token);

        locked_users.insert(username.to_string(), basic.to_string());
        Ok(())
    }

    pub(crate) async fn remove_user(&self, username: &str) -> AnyResult<()> {
        let mut locked_users = self.users.write().await;
        let auth = self.obj().auth().unwrap();

        if let Some(old_basic) = locked_users.get(username) {
            auth.remove_basic(old_basic);
        }

        locked_users.remove(username);
        Ok(())
    }

    pub(crate) async fn get_users(&self) -> AnyResult<HashSet<String>> {
        let locked_users = self.users.read().await;
        Ok(locked_users.keys().cloned().collect())
    }
}
// ===========================================================================
// Regression tests for fix E — stale-session reap / zombie-client kick
// (fix 9 d90ce64, its predecessor 7fe3ef3, and cd4b78b).
//
// `neolink` is a binary crate with no lib target, so the tests live inside the
// source file (same pattern as `src/stream/mod.rs`).
//
// Two kinds of test here:
//   * PURE   — table tests over the extracted predicates. No GStreamer.
//   * LIVE   — measurements against the REAL gst-rtsp-server library objects
//              (RTSPSessionPool / RTSPSession / RTSPSessionMedia). These pin
//              the upstream API semantics the fix depends on. They SKIP (early
//              return + eprintln) when `gstreamer::init()` fails, so the suite
//              stays green on a machine with no GStreamer runtime.
// ===========================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use gstreamer_rtsp_server::{RTSPMediaFactory, RTSPSessionPool};
    use std::sync::Mutex;
    use std::time::{Duration as StdDuration, Instant};

    /// `gstreamer::init()` exactly once; `false` ⇒ LIVE tests skip.
    fn gst_ready() -> bool {
        use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
        use std::sync::Once;
        static INIT: Once = Once::new();
        static OK: AtomicBool = AtomicBool::new(false);
        INIT.call_once(|| {
            OK.store(gstreamer::init().is_ok(), AtomicOrdering::SeqCst);
        });
        OK.load(AtomicOrdering::SeqCst)
    }

    /// The end-to-end rig binds a real port and drives the process-wide default
    /// glib main context, so only one may run at a time.
    static LIVE_RIG_LOCK: Mutex<()> = Mutex::new(());

    /// MEASURED (`claim1_live_next_timeout_usec_is_milliseconds`): gst-rtsp-server
    /// adds `extra-timeout` (default 5s) on top of `timeout` before computing
    /// time-to-expiry, so a `set_timeout(30)` session reports ~35_000ms
    /// remaining when freshly touched — NOT 30_000. Every pure table below is
    /// built on this measured window.
    const MEASURED_EXTRA_TIMEOUT_SECS: u32 = 5;

    /// Full expiry window in ms for a session with `timeout_secs`.
    fn expiry_window_ms(timeout_secs: u32) -> i64 {
        (timeout_secs + MEASURED_EXTRA_TIMEOUT_SECS) as i64 * 1000
    }

    // -------------------------------------------------------------------
    // THE PRE-FIX EXPRESSIONS, reconstructed so both arms run in the same
    // process against the same inputs.
    //   * `base_attempt1_would_reap` — attempt #1 (`neolink:local fix`, reverted
    //     2026-06-01, never committed to this tree); reconstructed from the
    //     root-cause writeup in `git show 7fe3ef3`: it compared the
    //     MILLISECOND return of `next_timeout_usec()` against a MICROSECOND
    //     timeout.
    //   * `base_8708608_would_reap` — the real base tree: its sweep body is
    //     `let remaining = ...; log::debug!(...); RTSPFilterResult::Keep`, i.e.
    //     it never reaps anything.
    //   * `base_naive_path_match` — the "did it match at all?" predicate that
    //     fix 9 deliberately did NOT use.
    // -------------------------------------------------------------------

    /// Attempt #1's predicate: `since_touch = timeout_us - remaining_ms`,
    /// reaped when that exceeded a 10s threshold. `remaining_ms` is in ms and
    /// `timeout_us` in µs, so `since_touch` is ~1000× too large for every
    /// session — including one touched microseconds ago.
    fn base_attempt1_would_reap(timeout_secs: u32, remaining_ms: i64, threshold: i64) -> bool {
        let timeout_us = (timeout_secs as i64) * 1_000_000;
        let since_touch = timeout_us - remaining_ms;
        since_touch > threshold
    }

    /// Base tree 8708608: the sweep only logged.
    fn base_8708608_would_reap(_timeout_secs: u32, _remaining_ms: i64) -> bool {
        false
    }

    /// The naive path predicate: any match at all.
    fn base_naive_path_match(matched: Option<i32>, _candidate: &str) -> bool {
        matched.is_some()
    }

    // ===================================================================
    // CLAIM 1 — staleness arithmetic is in MILLISECONDS
    // ===================================================================

    /// PURE. The fixed predicate over the whole idleness range, scored against
    /// both base arms. Window = (30 + 5) * 1000 = 35_000ms (measured), reap
    /// band = `remaining_ms <= 5_000`, i.e. idle >= 30_000ms.
    #[test]
    fn claim1_stale_predicate_ms_table() {
        let timeout_secs = SESSION_TIMEOUT_SECS; // 30
        let margin_ms = SESSION_STALE_REAP_MARGIN_MS; // 5_000
        let window_ms = expiry_window_ms(timeout_secs); // 35_000

        // (idle_ms, expected_stale)
        let table: &[(i64, bool)] = &[
            (0, false),      // freshly touched -> remaining 35_000
            (10_000, false), // healthy keepalive -> 25_000
            (25_000, false), // -> 10_000
            (29_999, false), // -> 5_001, one ms outside the band
            (30_000, true),  // -> 5_000, band edge
            (31_000, true),  // -> 4_000
            (34_000, true),  // -> 1_000
            (35_000, true),  // expired, remaining clamps to 0
            (36_000, true),
        ];

        let mut base_a1_reaps = 0usize;
        let mut base_8708608_reaps = 0usize;
        let mut fixed_reaps = 0usize;
        let mut healthy_rows = 0usize;
        let mut base_a1_reaps_healthy = 0usize;

        eprintln!(
            "idle_ms  remaining_ms  fixed  base#1(thr=10s_us)  base#1(thr=10s_ms)  base8708608  expect"
        );
        for (idle_ms, expect) in table {
            // next_timeout_usec() clamps at 0.
            let remaining_ms = (window_ms - idle_ms).max(0);
            let fixed = session_is_stale_ms(timeout_secs, remaining_ms, margin_ms);
            let b1_us = base_attempt1_would_reap(timeout_secs, remaining_ms, 10_000_000);
            let b1_ms = base_attempt1_would_reap(timeout_secs, remaining_ms, 10_000);
            let b0 = base_8708608_would_reap(timeout_secs, remaining_ms);
            eprintln!(
                "{idle_ms:7}  {remaining_ms:12}  {fixed:5}  {b1_us:18}  {b1_ms:18}  {b0:11}  {expect:6}"
            );
            assert_eq!(
                fixed, *expect,
                "idle_ms={idle_ms} remaining_ms={remaining_ms}: fixed predicate disagrees"
            );
            if fixed {
                fixed_reaps += 1;
            }
            if b1_us {
                base_a1_reaps += 1;
            }
            if b0 {
                base_8708608_reaps += 1;
            }
            if !*expect {
                healthy_rows += 1;
                if b1_us {
                    base_a1_reaps_healthy += 1;
                }
            }
        }

        eprintln!(
            "CLAIM1 totals over {} rows: fixed reaped {fixed_reaps}, attempt#1 reaped {base_a1_reaps}, base 8708608 reaped {base_8708608_reaps}",
            table.len()
        );
        eprintln!(
            "CLAIM1 healthy rows: {healthy_rows}; attempt#1 wrongly reaped {base_a1_reaps_healthy}/{healthy_rows}, fixed wrongly reaped 0/{healthy_rows}"
        );

        // BASE ARM (attempt #1): reaps EVERY row, healthy ones included.
        assert_eq!(
            base_a1_reaps,
            table.len(),
            "the ms/µs confusion reaps every session"
        );
        assert_eq!(base_a1_reaps_healthy, healthy_rows);
        // BASE ARM (real base tree): never reaps -> the zombie lingers.
        assert_eq!(base_8708608_reaps, 0);
        assert_eq!(fixed_reaps, 5);
    }

    /// PURE. Band edges, in remaining-ms space (independent of the window).
    #[test]
    fn claim1_reap_band_edges() {
        let t = SESSION_TIMEOUT_SECS;
        let m = SESSION_STALE_REAP_MARGIN_MS;
        assert!(!session_is_stale_ms(t, 5_001, m), "remaining 5001ms healthy");
        assert!(session_is_stale_ms(t, 5_000, m), "remaining 5000ms stale");
        assert!(session_is_stale_ms(t, 0, m), "already expired is stale");
        // A session with no timeout is never reaped by this sweep.
        assert!(!session_is_stale_ms(0, 0, m));
        // The env override moves the band and nothing else.
        assert!(!session_is_stale_ms(t, 6_000, 5_000));
        assert!(session_is_stale_ms(t, 6_000, 8_000));
    }

    /// LIVE. Measure what `RTSPSession::next_timeout_usec()` actually returns:
    /// milliseconds (and with `extra_timeout` folded in), not microseconds.
    /// This is the fact attempt #1 got wrong.
    #[test]
    fn claim1_live_next_timeout_usec_is_milliseconds() {
        if !gst_ready() {
            eprintln!("SKIP claim1_live_next_timeout_usec_is_milliseconds: gstreamer::init() failed");
            return;
        }
        let pool = RTSPSessionPool::new();
        let session = pool.create().expect("create session");
        session.set_timeout(SESSION_TIMEOUT_SECS);
        session.touch();
        let extra = session.extra_timeout();
        let now = glib::monotonic_time();
        let remaining = session.next_timeout_usec(now) as i64;
        eprintln!(
            "MEASURED: timeout={}s extra_timeout={}s -> next_timeout_usec(now)={remaining}",
            SESSION_TIMEOUT_SECS, extra
        );
        assert_eq!(
            extra, MEASURED_EXTRA_TIMEOUT_SECS,
            "the pure tables are calibrated on extra_timeout={MEASURED_EXTRA_TIMEOUT_SECS}"
        );
        let window = expiry_window_ms(SESSION_TIMEOUT_SECS);
        let us = SESSION_TIMEOUT_SECS as i64 * 1_000_000;
        assert!(
            (window - 1_000..=window).contains(&remaining),
            "expected ~{} (MILLISECONDS incl. extra_timeout), got {}",
            window,
            remaining
        );
        assert!(
            remaining < us / 100,
            "value is nowhere near the {}µs the `usec` name implies",
            us
        );
        eprintln!(
            "=> unit is MILLISECONDS ({remaining} ≈ {window}), NOT microseconds ({us}); attempt #1 computed since_touch = {} for a session touched microseconds ago",
            us - remaining
        );
    }

    /// LIVE. The same real session probed at simulated idle times by advancing
    /// the `now` argument. Both arms scored on real library output.
    #[test]
    fn claim1_live_staleness_band_on_real_session() {
        if !gst_ready() {
            eprintln!("SKIP claim1_live_staleness_band_on_real_session: gstreamer::init() failed");
            return;
        }
        let pool = RTSPSessionPool::new();
        let session = pool.create().expect("create session");
        session.set_timeout(SESSION_TIMEOUT_SECS);
        session.touch();
        let base_now = glib::monotonic_time();
        let margin_ms = SESSION_STALE_REAP_MARGIN_MS;

        // (idle_seconds, expect_stale)
        let idles: &[(i64, bool)] = &[
            (0, false),
            (10, false),
            (25, false),
            (29, false),
            (31, true),
            (34, true),
            (36, true),
        ];
        let mut base_wrong = 0usize;
        let mut fixed_wrong = 0usize;
        eprintln!("idle_s  measured_remaining_ms  fixed_stale  attempt#1_reap  expect");
        for (idle_s, expect) in idles {
            // `now` is monotonic MICROseconds; advancing it simulates idleness.
            let now = base_now + idle_s * 1_000_000;
            let remaining_ms = session.next_timeout_usec(now) as i64;
            let fixed = session_is_stale_ms(session.timeout(), remaining_ms, margin_ms);
            let b1 = base_attempt1_would_reap(session.timeout(), remaining_ms, 10_000_000);
            eprintln!("{idle_s:6}  {remaining_ms:21}  {fixed:11}  {b1:14}  {expect:6}");
            if fixed != *expect {
                fixed_wrong += 1;
            }
            if b1 != *expect {
                base_wrong += 1;
            }
        }
        eprintln!(
            "CLAIM1 LIVE: wrong verdicts — attempt#1 {base_wrong}/{}, fixed {fixed_wrong}/{}",
            idles.len(),
            idles.len()
        );
        assert_eq!(fixed_wrong, 0, "fixed predicate must match every row");
        assert_eq!(
            base_wrong, 4,
            "attempt #1 must be wrong on exactly the 4 healthy rows"
        );
    }

    // ===================================================================
    // CLAIM 2 — the reap must count sessions REMOVED, not sessions Ref'd
    // ===================================================================

    /// LIVE. `RTSPSessionPool::filter()` returns the Ref'd sessions only.
    /// Measured: remove 3 of 3 sessions and the returned Vec has length 0 —
    /// which is exactly why attempt #1's `if !reaped.is_empty()` log never
    /// fired while it was deleting every session in the pool.
    #[test]
    fn claim2_live_filter_returns_refd_not_removed() {
        if !gst_ready() {
            eprintln!("SKIP claim2_live_filter_returns_refd_not_removed: gstreamer::init() failed");
            return;
        }
        // --- arm A: closure returns Remove (what the reap does) ---
        let pool = RTSPSessionPool::new();
        for _ in 0..3 {
            pool.create().expect("create session");
        }
        let before = pool.n_sessions();
        let mut counted_ourselves = 0usize; // the FIXED metric
        let returned = pool.filter(Some(&mut |_, _session| {
            counted_ourselves += 1;
            RTSPFilterResult::Remove
        }));
        let after = pool.n_sessions();
        eprintln!(
            "CLAIM2 arm A (Remove): n_sessions {before} -> {after}; filter() returned {} session(s) [BASE metric]; counted ourselves {counted_ourselves} [FIXED metric]",
            returned.len()
        );
        assert_eq!(before, 3);
        assert_eq!(after, 0, "all 3 really were removed");
        assert_eq!(
            returned.len(),
            0,
            "BASE metric reports 0 removals while 3 sessions were destroyed"
        );
        assert_eq!(counted_ourselves, 3, "FIXED metric reports the real count");

        // --- arm B: control, closure returns Ref ---
        let pool2 = RTSPSessionPool::new();
        for _ in 0..3 {
            pool2.create().expect("create session");
        }
        let returned2 = pool2.filter(Some(&mut |_, _s| RTSPFilterResult::Ref));
        eprintln!(
            "CLAIM2 arm B (Ref): filter() returned {} session(s), n_sessions still {}",
            returned2.len(),
            pool2.n_sessions()
        );
        assert_eq!(returned2.len(), 3, "the returned Vec IS the Ref set");
        assert_eq!(pool2.n_sessions(), 3);
    }

    /// LIVE. The whole reap decision path (predicate + filter) on a real pool:
    /// only the stale-band sessions are removed, the healthy ones survive.
    #[test]
    fn claim2_live_only_stale_sessions_are_removed() {
        if !gst_ready() {
            eprintln!("SKIP claim2_live_only_stale_sessions_are_removed: gstreamer::init() failed");
            return;
        }
        let pool = RTSPSessionPool::new();
        // 2 "healthy" sessions (30s timeout + 5s extra, freshly touched) and 2
        // "zombies" (1s timeout, extra 0 -> already deep in the reap band).
        for _ in 0..2 {
            let s = pool.create().expect("create");
            s.set_timeout(SESSION_TIMEOUT_SECS);
            s.touch();
        }
        for _ in 0..2 {
            let s = pool.create().expect("create");
            s.set_timeout(1);
            s.set_extra_timeout(0);
            s.touch();
        }
        let now = glib::monotonic_time();
        let margin_ms = SESSION_STALE_REAP_MARGIN_MS;
        let before = pool.n_sessions();

        let mut fixed_removed_healthy = 0usize;
        let mut fixed_removed_zombie = 0usize;
        let mut base_a1_healthy = 0usize;
        let mut base_a1_zombie = 0usize;
        pool.filter(Some(&mut |_, session| {
            let timeout_secs = session.timeout();
            let healthy = timeout_secs == SESSION_TIMEOUT_SECS;
            let remaining_ms = session.next_timeout_usec(now) as i64;
            if base_attempt1_would_reap(timeout_secs, remaining_ms, 10_000_000) {
                if healthy {
                    base_a1_healthy += 1;
                } else {
                    base_a1_zombie += 1;
                }
            }
            if session_is_stale_ms(timeout_secs, remaining_ms, margin_ms) {
                if healthy {
                    fixed_removed_healthy += 1;
                } else {
                    fixed_removed_zombie += 1;
                }
                RTSPFilterResult::Remove
            } else {
                RTSPFilterResult::Keep
            }
        }));
        let after = pool.n_sessions();
        eprintln!(
            "CLAIM2 live pool: {before} sessions (2 healthy + 2 zombie) -> FIXED removed healthy={fixed_removed_healthy} zombie={fixed_removed_zombie}, survivors {after}"
        );
        eprintln!(
            "CLAIM2 live pool: attempt#1 would have removed healthy={base_a1_healthy} zombie={base_a1_zombie} — i.e. EXACTLY INVERTED: it kills the live consumers and spares the zombies (its `timeout_us - remaining_ms` grows with the timeout, so a short-timeout dead session scores LOW)"
        );
        assert_eq!(before, 4);
        assert_eq!(fixed_removed_zombie, 2, "fixed removes both zombies");
        assert_eq!(fixed_removed_healthy, 0, "fixed removes no healthy session");
        assert_eq!(after, 2, "both healthy sessions survive");
        assert_eq!(base_a1_healthy, 2, "attempt #1 kills both HEALTHY sessions");
        assert_eq!(base_a1_zombie, 0, "attempt #1 spares both zombies");
    }

    // ===================================================================
    // CLAIM 3 — close the owning client BEFORE pool-reaping
    // ===================================================================

    /// PURE. The ordering contract as the sweep executes it.
    #[test]
    fn claim3_plan_closes_every_owner_before_the_reap() {
        let plan = stale_reap_plan(3);
        eprintln!("CLAIM3 fixed plan (3 owners): {plan:?}");
        assert_eq!(
            plan,
            vec![
                ReapStep::CloseOwner(0),
                ReapStep::CloseOwner(1),
                ReapStep::CloseOwner(2),
                ReapStep::PoolReapStaleSessions,
            ]
        );
        let reap_at = plan
            .iter()
            .position(|s| *s == ReapStep::PoolReapStaleSessions)
            .expect("plan must reap");
        let last_close = plan
            .iter()
            .rposition(|s| matches!(s, ReapStep::CloseOwner(_)))
            .expect("plan must close owners");
        assert!(last_close < reap_at, "every close must precede the pool reap");
        // No owners (true CLOSE_WAIT, client object already gone): still reap.
        assert_eq!(stale_reap_plan(0), vec![ReapStep::PoolReapStaleSessions]);
    }

    // ===================================================================
    // CLAIM 4 — exact-path match + generation guard (pure parts)
    // ===================================================================

    /// PURE. Table over both predicates, using the `matched` values MEASURED in
    /// `claim4_live_kick_is_exact_path_matched` plus the hypothetical
    /// prefix-hit shape the guard exists to reject.
    #[test]
    fn claim4_exact_path_match_table() {
        // (label, matched_from_gst, candidate, expect_kick)
        let table: &[(&str, Option<i32>, &str, bool)] = &[
            // measured on gst-rtsp-server 1.26.2 (see the live test)
            ("exact /testcam/main", Some(13), "/testcam/main", true),
            ("exact /testcam", Some(8), "/testcam", true),
            ("no match (/testcam/main2)", None, "/testcam/main2", false),
            ("no match (/othercam/main)", None, "/othercam/main", false),
            // a prefix hit: `matched` reports the MOUNT path length, shorter
            // than the candidate. Only the exact predicate rejects it.
            ("alias prefix hit", Some(8), "/testcam/main", false),
        ];
        let mut base_kicks = 0usize;
        let mut fixed_kicks = 0usize;
        let mut base_false_kicks = 0usize;
        eprintln!("case                       matched    candidate        fixed  base(naive)  expect");
        for (label, matched, candidate, expect) in table {
            let fixed = media_path_is_exact_match(*matched, candidate);
            let base = base_naive_path_match(*matched, candidate);
            eprintln!("{label:26} {matched:?}  {candidate:15}  {fixed:5}  {base:11}  {expect:6}");
            assert_eq!(fixed, *expect, "case {label}");
            if fixed {
                fixed_kicks += 1;
            }
            if base {
                base_kicks += 1;
                if !*expect {
                    base_false_kicks += 1;
                }
            }
        }
        eprintln!(
            "CLAIM4 totals: fixed kicked {fixed_kicks}/{} (0 false), naive base kicked {base_kicks}/{} of which {base_false_kicks} FALSE",
            table.len(),
            table.len()
        );
        assert_eq!(fixed_kicks, 2);
        assert_eq!(base_false_kicks, 1, "the naive predicate collateral-kicks");
    }

    /// PURE. Generation guard: only the newest pipeline generation may kick.
    #[test]
    fn claim4_generation_guard_table() {
        // (my_generation, current_generation, may_kick)
        let table: &[(u64, u64, bool)] = &[
            (1, 1, true),  // fresh pump, still newest
            (5, 5, true),  // ditto after 4 rebuilds
            (4, 5, false), // stale pump, a newer pipeline exists -> no kick
            (1, 9, false), // very stale
            (6, 5, false), // impossible-but-guarded
            (0, 0, true),  // pre-first-build
        ];
        let mut kicks = 0usize;
        for (mine, current, expect) in table {
            let may = kick_generation_is_current(*mine, *current);
            eprintln!(
                "CLAIM4 gen guard: my={mine} current={current} -> may_kick={may} (expect {expect})"
            );
            assert_eq!(may, *expect);
            if may {
                kicks += 1;
            }
        }
        assert_eq!(kicks, 3);
        // BASE ARM: no guard at all — a stale pump kicks unconditionally.
        let unguarded_kicks = table.len();
        eprintln!(
            "CLAIM4 gen guard totals: guarded {kicks}/{}, unguarded (pre-fix) {unguarded_kicks}/{} -> {} kicks would land on a NEWER pipeline's clients",
            table.len(),
            table.len(),
            unguarded_kicks - kicks
        );
    }

    /// The constants the whole fix is calibrated against.
    #[test]
    fn claim1_constants_are_what_the_writeup_says() {
        assert_eq!(SESSION_TIMEOUT_SECS, 30);
        assert_eq!(SESSION_STALE_REAP_MARGIN_MS, 5_000);
    }

    // ===================================================================
    // LIVE END-TO-END RIG — real NeoRtspServer + real rtspsrc client(s)
    // over loopback. Everything is 127.0.0.1 / "testcam"; no camera, no LAN.
    // ===================================================================

    struct LiveRig {
        server: NeoRtspServer,
        main_loop: glib::MainLoop,
        thread: Option<std::thread::JoinHandle<()>>,
        port: i32,
    }

    impl LiveRig {
        fn sessions(&self) -> u32 {
            self.server
                .session_pool()
                .map(|p| p.n_sessions())
                .unwrap_or(0)
        }
        fn wait_sessions(&self, want: u32, max: StdDuration) -> u32 {
            let t0 = Instant::now();
            loop {
                let n = self.sessions();
                if n >= want || t0.elapsed() > max {
                    return n;
                }
                std::thread::sleep(StdDuration::from_millis(50));
            }
        }
        /// Start a consumer and WAIT until it is really PLAYING. Kicking a
        /// consumer that is still mid-SETUP is not the scenario under test
        /// (and measured flaky: rtspsrc mid-handshake does not always post an
        /// error when the peer closes), so every arm starts from a fully
        /// established stream.
        fn client(&self, path: &str) -> gstreamer::Element {
            let p = gstreamer::parse::launch(&format!(
                "rtspsrc location=rtsp://127.0.0.1:{}{path} protocols=tcp latency=0 ! fakesink sync=false",
                self.port
            ))
            .expect("client pipeline");
            p.set_state(gstreamer::State::Playing).expect("client play");
            let t0 = Instant::now();
            loop {
                let (_, cur, _) = p.state(Some(gstreamer::ClockTime::from_mseconds(200)));
                if cur == gstreamer::State::Playing {
                    eprintln!("   rig: consumer on {path} reached PLAYING in {:?}", t0.elapsed());
                    break;
                }
                if t0.elapsed() > StdDuration::from_secs(15) {
                    eprintln!("   rig: consumer on {path} NEVER reached PLAYING (state={cur:?})");
                    break;
                }
            }
            p
        }
    }

    impl Drop for LiveRig {
        fn drop(&mut self) {
            self.main_loop.quit();
            if let Some(t) = self.thread.take() {
                let _ = t.join();
            }
        }
    }

    /// Every element the rig needs; missing ⇒ the LIVE tests skip.
    fn live_elements_present() -> bool {
        ["videotestsrc", "rtpvrawpay", "rtspsrc", "fakesink"]
            .iter()
            .all(|e| gstreamer::ElementFactory::find(e).is_some())
    }

    fn start_rig(paths: &[&str]) -> Option<LiveRig> {
        if !gst_ready() || !live_elements_present() {
            return None;
        }
        let server = NeoRtspServer::new().ok()?;
        server.set_address("127.0.0.1");
        server.set_service("0"); // ephemeral port
        let mounts = server.mount_points()?;
        for path in paths {
            let f = RTSPMediaFactory::new();
            // Raw payload: no encoder needed, so this rig only depends on
            // gstreamer1.0-plugins-base/good.
            f.set_launch("( videotestsrc is-live=true ! video/x-raw,format=I420,width=64,height=48,framerate=10/1 ! rtpvrawpay name=pay0 pt=96 )");
            f.set_shared(true);
            f.set_transport_mode(gstreamer_rtsp_server::RTSPTransportMode::PLAY);
            f.set_suspend_mode(gstreamer_rtsp_server::RTSPSuspendMode::None);
            f.add_role_from_structure(
                &gstreamer::Structure::builder("anonymous")
                    .field(gstreamer_rtsp_server::RTSP_PERM_MEDIA_FACTORY_ACCESS, true)
                    .field(gstreamer_rtsp_server::RTSP_PERM_MEDIA_FACTORY_CONSTRUCT, true)
                    .build(),
            );
            mounts.add_factory(path, f);
        }
        server.attach(None).ok()?;
        let port = server.bound_port();
        let main_loop = glib::MainLoop::new(None, false);
        let ml = main_loop.clone();
        let thread = std::thread::spawn(move || ml.run());
        std::thread::sleep(StdDuration::from_millis(200));
        Some(LiveRig {
            server,
            main_loop,
            thread: Some(thread),
            port,
        })
    }

    /// How long until the client pipeline notices its connection died
    /// (Error or Eos on its bus). `None` ⇒ it is still happily connected —
    /// i.e. a ZOMBIE.
    fn wait_client_end(pipeline: &gstreamer::Element, max: StdDuration) -> Option<StdDuration> {
        let t0 = Instant::now();
        let bus = pipeline.bus()?;
        while t0.elapsed() < max {
            while let Some(msg) = bus.pop() {
                match msg.view() {
                    gstreamer::MessageView::Error(_) | gstreamer::MessageView::Eos(_) => {
                        return Some(t0.elapsed())
                    }
                    _ => {}
                }
            }
            std::thread::sleep(StdDuration::from_millis(25));
        }
        None
    }

    /// All clients currently attached to the server (the Ref set).
    fn all_clients(server: &NeoRtspServer) -> Vec<gstreamer_rtsp_server::RTSPClient> {
        server.client_filter(Some(&mut |_, _| RTSPFilterResult::Ref))
    }

    /// LIVE E2E. CLAIM 3: reap-only (pre-fix9) leaves a live consumer as a
    /// zombie AND makes its owner unmatchable; close-then-reap (fix 9) kills
    /// the connection so the consumer reconnects. Both arms measured against
    /// the same rig with a real rtspsrc consumer.
    #[test]
    fn claim3_live_reap_only_zombies_the_consumer() {
        let _guard = LIVE_RIG_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let Some(rig) = start_rig(&["/testcam/main"]) else {
            eprintln!("SKIP claim3_live_reap_only_zombies_the_consumer: no GStreamer runtime");
            return;
        };
        let observe = StdDuration::from_secs(4);

        // ---------------- BASE ARM: pool-reap only ----------------
        let client_a = rig.client("/testcam/main");
        let n = rig.wait_sessions(1, StdDuration::from_secs(10));
        assert_eq!(n, 1, "consumer A must own a session");
        let sess_a = rig
            .server
            .session_pool()
            .unwrap()
            .filter(Some(&mut |_, _s| RTSPFilterResult::Ref))
            .first()
            .and_then(|s| s.sessionid())
            .expect("session id");
        eprintln!("CLAIM3 LIVE base arm: consumer A attached, session {sess_a}, n_sessions={n}");

        // The pre-fix9 sweep: pool-Remove the stale session, nothing else.
        let mut base_reaped = 0usize;
        rig.server.session_pool().unwrap().filter(Some(&mut |_, _s| {
            base_reaped += 1;
            RTSPFilterResult::Remove
        }));
        eprintln!(
            "CLAIM3 LIVE base arm: pool-reaped {base_reaped} session(s), n_sessions now {}",
            rig.sessions()
        );

        // MEASURE the cascade: can anything still find the owning client?
        let owners_after_reap = all_clients(&rig.server)
            .iter()
            .filter(|c| {
                !c.session_filter(Some(&mut |_, _| RTSPFilterResult::Ref))
                    .is_empty()
            })
            .count();
        let clients_after_reap = all_clients(&rig.server).len();
        eprintln!(
            "CLAIM3 LIVE base arm: {clients_after_reap} client object(s) still attached, of which {owners_after_reap} still own a session (this is the cascade: pool removal detaches the session from its client)"
        );

        let base_end = wait_client_end(&client_a, observe);
        eprintln!(
            "CLAIM3 LIVE base arm: consumer A after {observe:?} -> ended={base_end:?} (None = still connected = ZOMBIE)"
        );
        let _ = client_a.set_state(gstreamer::State::Null);

        assert_eq!(base_reaped, 1);
        assert_eq!(
            owners_after_reap, 0,
            "after the pool reap the owning client is unmatchable — proven cascade"
        );
        assert!(
            base_end.is_none(),
            "pre-fix9: the consumer must survive the silent reap as a zombie"
        );

        // ---------------- FIXED ARM: close-then-reap ----------------
        std::thread::sleep(StdDuration::from_millis(500));
        let client_b = rig.client("/testcam/main");
        let n = rig.wait_sessions(1, StdDuration::from_secs(10));
        assert_eq!(n, 1, "consumer B must own a session");
        eprintln!("CLAIM3 LIVE fixed arm: consumer B attached, n_sessions={n}");

        // Exactly the production sweep: collect the stale ids, find the owning
        // clients, then execute stale_reap_plan() — closes before reaping.
        let stale_ids: Vec<glib::GString> = rig
            .server
            .session_pool()
            .unwrap()
            .filter(Some(&mut |_, _s| RTSPFilterResult::Ref))
            .iter()
            .filter_map(|s| s.sessionid())
            .collect();
        let owners: Vec<_> = all_clients(&rig.server)
            .into_iter()
            .filter(|c| {
                let owns = std::cell::Cell::new(false);
                c.session_filter(Some(&mut |_, session| {
                    if session
                        .sessionid()
                        .map(|sid| stale_ids.contains(&sid))
                        .unwrap_or(false)
                    {
                        owns.set(true);
                    }
                    RTSPFilterResult::Keep
                }));
                owns.get()
            })
            .collect();
        eprintln!(
            "CLAIM3 LIVE fixed arm: {} stale session(s), {} owning client(s) found BEFORE the reap",
            stale_ids.len(),
            owners.len()
        );
        assert_eq!(
            owners.len(),
            1,
            "the owner IS matchable while the session is still in the pool"
        );

        // FIXED ARM: execute stale_reap_plan() on the glib main
        // context, exactly as the sweep does.
        let (tx, rx) = std::sync::mpsc::channel::<(usize, usize)>();
        let server = rig.server.clone();
        let plan_ids = stale_ids.clone();
        let plan_owners = owners.clone();
        let t_kick = Instant::now();
        glib::MainContext::default().invoke(move || {
            let mut kicked = 0usize;
            let mut reaped = 0usize;
            for step in stale_reap_plan(plan_owners.len()) {
                match step {
                    ReapStep::CloseOwner(i) => {
                        plan_owners[i].close();
                        kicked += 1;
                    }
                    ReapStep::PoolReapStaleSessions => {
                        if let Some(pool) = server.session_pool() {
                            pool.filter(Some(&mut |_, s| {
                                if s.sessionid()
                                    .map(|sid| plan_ids.contains(&sid))
                                    .unwrap_or(false)
                                {
                                    reaped += 1;
                                    RTSPFilterResult::Remove
                                } else {
                                    RTSPFilterResult::Keep
                                }
                            }));
                        }
                    }
                }
            }
            let _ = tx.send((kicked, reaped));
        });
        let (kicked, reaped) = rx
            .recv_timeout(StdDuration::from_secs(5))
            .expect("sweep plan must run on the main context");
        let fixed_end = wait_client_end(&client_b, observe);
        eprintln!(
            "CLAIM3 LIVE fixed arm: closed {kicked} owner(s), pool-reaped {reaped}; consumer B ended after {fixed_end:?} (kick issued at t+0, budget {observe:?}, elapsed {:?})",
            t_kick.elapsed()
        );
        let _ = client_b.set_state(gstreamer::State::Null);
        eprintln!(
            "CLAIM3 LIVE summary: base(reap-only) consumer alive after {}ms; fixed(close-then-reap on the main context) consumer dead after {}ms",
            observe.as_millis(),
            fixed_end.map(|d| d.as_millis() as i64).unwrap_or(-1)
        );
        assert_eq!(kicked, 1);
        assert_eq!(reaped, 1);
        assert!(
            fixed_end.is_some(),
            "fix 9: closing the owner must break the consumer's connection"
        );
    }

    /// LIVE E2E. CLAIM 4: `kick_clients_of_paths` is exact-path matched — it
    /// kills the consumer on the kicked path and nobody else. Also MEASURES
    /// `gst_rtsp_session_media_matches()` on real session medias.
    #[test]
    fn claim4_live_kick_is_exact_path_matched() {
        let _guard = LIVE_RIG_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let Some(rig) = start_rig(&["/testcam/main", "/testcam"]) else {
            eprintln!("SKIP claim4_live_kick_is_exact_path_matched: no GStreamer runtime");
            return;
        };
        let client_main = rig.client("/testcam/main");
        let client_alias = rig.client("/testcam");
        let n = rig.wait_sessions(2, StdDuration::from_secs(10));
        assert_eq!(n, 2, "both consumers must own a session");

        // ---- measure matches() on the real session medias ----
        let candidates = [
            "/testcam/main",
            "/testcam/main2",
            "/testcam",
            "/testcam/sub",
            "/othercam/main",
        ];
        let mut exact_hits = 0usize;
        let mut naive_hits = 0usize;
        eprintln!("MEASURED gst_rtsp_session_media_matches() on live session medias:");
        rig.server.session_pool().unwrap().filter(Some(&mut |_, session| {
            session.filter(Some(&mut |_, media| {
                for c in candidates {
                    let m = media.matches(c);
                    let ex = media_path_is_exact_match(m, c);
                    let na = base_naive_path_match(m, c);
                    if ex {
                        exact_hits += 1;
                    }
                    if na {
                        naive_hits += 1;
                    }
                    eprintln!("   candidate {c:15} matched={m:?} exact={ex} naive={na}");
                }
                RTSPFilterResult::Keep
            }));
            RTSPFilterResult::Keep
        }));
        eprintln!(
            "CLAIM4 LIVE: over 2 session medias × {} candidates -> exact hits {exact_hits}, naive hits {naive_hits}",
            candidates.len()
        );

        // ---- negative kick: a path nobody is attached to ----
        rig.server.kick_clients_of_paths(
            Arc::new(vec!["/testcam/main2".to_string()]),
            "testcam::main".to_string(),
            "negative control".to_string(),
        );
        let neg_main = wait_client_end(&client_main, StdDuration::from_secs(2));
        let neg_alias = wait_client_end(&client_alias, StdDuration::from_millis(500));
        eprintln!(
            "CLAIM4 LIVE negative kick(/testcam/main2): main_ended={neg_main:?} alias_ended={neg_alias:?} n_sessions={}",
            rig.sessions()
        );
        assert!(neg_main.is_none() && neg_alias.is_none(), "no false kick");
        assert_eq!(rig.sessions(), 2, "both sessions survive a non-matching kick");

        // ---- positive kick: exactly one consumer ----
        rig.server.kick_clients_of_paths(
            Arc::new(vec!["/testcam/main".to_string()]),
            "testcam::main".to_string(),
            "terminal pipeline death".to_string(),
        );
        let pos_main = wait_client_end(&client_main, StdDuration::from_secs(4));
        let pos_alias = wait_client_end(&client_alias, StdDuration::from_millis(500));
        eprintln!(
            "CLAIM4 LIVE positive kick(/testcam/main): main_ended={pos_main:?} alias_ended={pos_alias:?} n_sessions={}",
            rig.sessions()
        );
        let _ = client_main.set_state(gstreamer::State::Null);
        let _ = client_alias.set_state(gstreamer::State::Null);
        assert!(
            pos_main.is_some(),
            "the consumer on the kicked path must be disconnected"
        );
        assert!(
            pos_alias.is_none(),
            "the consumer on /testcam must NOT be collateral-kicked"
        );
    }

    /// LIVE E2E. CLAIM 2 (client side): `RTSPClient::session_filter()` has the
    /// same Ref-not-Removed contract as the pool filter — measured on a real
    /// client, because `connect_closed` in `new()` logs off that return value.
    #[test]
    fn claim2_live_client_session_filter_returns_refd_not_removed() {
        let _guard = LIVE_RIG_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let Some(rig) = start_rig(&["/testcam/main"]) else {
            eprintln!("SKIP claim2_live_client_session_filter_returns_refd_not_removed: no GStreamer runtime");
            return;
        };
        let client = rig.client("/testcam/main");
        let n = rig.wait_sessions(1, StdDuration::from_secs(10));
        assert_eq!(n, 1);
        let clients = all_clients(&rig.server);
        assert_eq!(clients.len(), 1, "one attached client");

        let refd = clients[0].session_filter(Some(&mut |_, _| RTSPFilterResult::Ref));
        eprintln!("CLAIM2 LIVE client.session_filter(Ref) -> {} session(s)", refd.len());
        assert_eq!(refd.len(), 1);

        let mut counted = 0usize;
        let returned = clients[0].session_filter(Some(&mut |_, _| {
            counted += 1;
            RTSPFilterResult::Remove
        }));
        std::thread::sleep(StdDuration::from_millis(300));
        let still_owned = clients[0]
            .session_filter(Some(&mut |_, _| RTSPFilterResult::Ref))
            .len();
        let pool_after = rig.sessions();
        eprintln!(
            "CLAIM2 LIVE client.session_filter(Remove) -> returned {} [BASE metric, the value `connect_closed` logs off], counted ourselves {counted} [FIXED metric]; client still owns {still_owned} session(s); pool n_sessions {n} -> {pool_after}",
            returned.len()
        );
        assert_eq!(
            returned.len(),
            0,
            "the connect_closed log's `!removed.is_empty()` guard can never be true"
        );
        assert_eq!(counted, 1, "one session really was passed to the filter");
        assert_eq!(still_owned, 0, "Remove detaches the session from the CLIENT");
        // MEASURED: client-side Remove does NOT take the session out of the
        // pool — it only detaches it from the client. The pool still holds it
        // until cleanup()/the stale sweep. Recorded, not asserted as a design
        // claim, so a future gst version changing this shows up as a diff.
        eprintln!(
            "CLAIM2 LIVE note: pool retained {pool_after} session(s) after the client-side Remove"
        );
        let _ = client.set_state(gstreamer::State::Null);
    }
}
