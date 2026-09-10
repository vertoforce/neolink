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
                let removed = client.session_filter(Some(&mut |_client, _session| {
                    RTSPFilterResult::Remove
                }));
                if !removed.is_empty() {
                    log::info!(
                        "RTSP client closed — reaped {} session(s) immediately",
                        removed.len()
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
                        let timeout_ms = (session.timeout() as i64).saturating_mul(1000);
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
                        if timeout_ms > 0 && remaining_ms <= margin_ms {
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
                            let kicked = owners.len();
                            for client in owners {
                                client.close();
                            }
                            if kicked > 0 {
                                log::info!(
                                    "RTSP stale-session sweep — closed {kicked} client connection(s) owning stale session(s) (alive-but-starved peers now reconnect instead of zombieing)"
                                );
                            }
                            // Pass 3 — the reap the sweep always did. close()
                            // above already cascades session removal for owned
                            // sessions via the closed hook; this pass catches
                            // the ownerless leftovers (true CLOSE_WAIT with
                            // the client object already gone).
                            let mut reaped_count: usize = 0;
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
                            && paths.iter().any(|p| {
                                media
                                    .matches(p)
                                    .map(|m| m as usize == p.len())
                                    .unwrap_or(false)
                            })
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
                log::debug!("{label}: zombie-client kick found no attached clients ({reason})");
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
