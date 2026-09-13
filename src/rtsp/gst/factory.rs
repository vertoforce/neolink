//! Attempts to subclass GstMediaFactory
//!
//! We are now messing with gstreamer glib objects
//! expect issues

use super::AnyResult;
use gstreamer::glib::object_subclass;
use gstreamer::prelude::GstBinExtManual;
use gstreamer::Element;
use gstreamer::{
    glib::{self, Object},
    Structure,
};
use gstreamer_rtsp::RTSPUrl;
use gstreamer_rtsp_server::prelude::*;
use gstreamer_rtsp_server::subclass::prelude::*;
use gstreamer_rtsp_server::RTSPMediaFactory;
use gstreamer_rtsp_server::RTSPTransportMode;
use gstreamer_rtsp_server::{RTSP_PERM_MEDIA_FACTORY_ACCESS, RTSP_PERM_MEDIA_FACTORY_CONSTRUCT};
use log::*;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;

glib::wrapper! {
    /// The wrapped RTSPMediaFactory
    pub(crate) struct NeoMediaFactory(ObjectSubclass<NeoMediaFactoryImpl>) @extends RTSPMediaFactory;
}

impl Default for NeoMediaFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl NeoMediaFactory {
    fn new() -> Self {
        let factory = Object::new::<NeoMediaFactory>();
        // Share a single GStreamer pipeline across all RTSP clients.
        // Without this, each client (go2rtc, ffprobe, health checks) creates
        // its own pipeline and camera stream session, exhausting resources
        // and accumulating CLOSE_WAIT connections during 24/7 operation.
        factory.set_shared(true);
        factory.set_eos_shutdown(false);
        factory.set_stop_on_disconnect(false);
        // factory.set_publish_clock_mode(gstreamer_rtsp_server::RTSPPublishClockMode::Clock);
        //
        // SuspendMode::None — the camera B silent-egress-wedge fix (fix 4).
        //
        // ROOT CAUSE (proven 2026-06-06 at the GStreamer level, isolated
        // container, dot graphs + GST_DEBUG + gst-rtsp-server 1.22 C source):
        //
        // Under the previous `Reset` mode WITH `set_shared(true)`, every RTSP
        // client connect/disconnect drives gst-rtsp-server's per-media suspend
        // path. In `rtsp-media.c::gst_rtsp_media_suspend` →
        // `default_suspend()`, the `RESET` case calls
        // `set_target_state(media, GST_STATE_NULL)` — it tears the WHOLE shared
        // pipeline down to NULL on suspend. That function is prefaced by the
        // upstream `GST_FIXME("suspend for dynamic pipelines needs fixing")`:
        // our appsrc-fed pipeline IS a dynamic pipeline, and the suspend logic
        // is not safe for it.
        //
        // The wedge sequence, captured live on camera B (a slow-preroll
        // 5120-panorama camera, ~370 ms WiFi RTT) under staggered
        // multi-consumer churn (go2rtc preload + camera B crop exec + detect,
        // each connecting/dropping at different times):
        //
        //   set_state PAUSED → (preroll) → "is_complete = 0,
        //   num_complete_sender_streams = 0"  [the requesting client already
        //   timed out and left before this slow camera finished prerolling, so
        //   the RTP transport never completes] → gst_rtsp_media_prepare
        //   "is prerolled" → gst_rtsp_media_suspend "suspend for dynamic
        //   pipelines needs fixing" → default_suspend "suspend to NULL" →
        //   set_state NULL → gst_rtsp_media_unprepare → finish_unprepare
        //   "Removing elements of stream 0/1".
        //
        // i.e. the freshly-built, fully-linked pipeline (the media_configure
        // dot graph shows vidsrc→queue→parser→pay0 all linked, identical to a
        // healthy one — so the element graph is NOT the bug) is reset to NULL
        // the instant after it prerolls, because suspend fires before the
        // transport completes. The frame-pump std::thread then pushes into the
        // now-NULL/detached appsrc forever ("App source is closed" cascade),
        // each new client triggers another build→preroll→suspend-to-NULL
        // thrash, and NO RTP ever egresses. A fresh consumer gets
        // "Invalid data / connection timed out" — the proven prod wedge.
        //
        // FIX: `SuspendMode::None`. In `default_suspend()` the `NONE` case is a
        // no-op ("media %p no suspend") — the shared pipeline is NEVER set to
        // NULL on client churn. It stays prepared/PLAYING; appsrcs stay
        // attached; the frame-pump keeps pushing successfully; a new client
        // just attaches its transport to the already-running rtpbin. The
        // destructive suspend-to-NULL race that produced the wedge cannot occur.
        //
        // Why this is safe for the failure mode `Reset` was guarding (frame-
        // pump thread death):
        //   - The thread-death the old comment feared was itself CAUSED by
        //     Reset detaching the appsrc ("App source is closed" → thread exit).
        //     Removing Reset removes that cause; under None the appsrc is never
        //     detached, so `check_live` never reports "closed" and the thread
        //     does not die that way.
        //   - If the thread ever dies for an unrelated reason, the camthread
        //     FRAME_STALENESS_MS watchdog (no frames pushed for >30 s) still
        //     fires a full BC reconnect, and the EOS-on-100-errors /
        //     back-pressure-EOS paths in factory.rs are still present. So a
        //     recovery path remains; we did not remove a safety net, we removed
        //     the bug that the net was compensating for.
        //   - `stop_on_disconnect(false)` + the go2rtc preload heartbeat keep
        //     at least one client connected, so the media stays prepared.
        //
        // FD/thread non-regression is verified in the fix 4 fault-injection +
        // negative test (no FD growth, no thread accumulation, no false
        // teardown over a 3 min healthy multi-consumer stream).
        factory.set_suspend_mode(gstreamer_rtsp_server::RTSPSuspendMode::None);
        factory.set_launch("videotestsrc pattern=\"snow\" ! video/x-raw,width=896,height=512,framerate=25/1 ! textoverlay name=\"inittextoverlay\" text=\"Stream not Ready\" valignment=top halignment=left font-desc=\"Sans, 32\" ! jpegenc ! rtpjpegpay name=pay0");
        factory.set_transport_mode(RTSPTransportMode::PLAY);
        factory
    }

    pub(crate) async fn new_with_callback<F>(callback: F) -> AnyResult<Self>
    where
        F: Fn(Element) -> AnyResult<Option<Element>> + Send + Sync + 'static,
    {
        let factory = Self::new();
        factory.imp().set_callback(callback).await;
        Ok(factory)
    }

    /// Install the DESCRIBE gate (see `NeoMediaFactoryImpl::describe_gate`).
    /// The closure runs on gst-rtsp-server's glib main-loop thread and must
    /// not block.
    pub(crate) fn set_describe_gate<F>(&self, gate: F)
    where
        F: Fn() -> bool + Send + Sync + 'static,
    {
        self.imp().set_describe_gate(gate);
    }

    pub(crate) fn add_permitted_roles<T: AsRef<str>>(&self, permitted_roles: &HashSet<T>) {
        for permitted_role in permitted_roles {
            let s = permitted_role.as_ref();
            log::debug!("Adding {} as permitted user", s);
            self.add_role_from_structure(
                &Structure::builder(s)
                    .field(RTSP_PERM_MEDIA_FACTORY_ACCESS, true)
                    .field(RTSP_PERM_MEDIA_FACTORY_CONSTRUCT, true)
                    .build(),
            );
        }
        // During auth, first it binds anonymously. At this point it checks
        // RTSP_PERM_MEDIA_FACTORY_ACCESS to see if anyone can connect
        // This is done before the auth token is loaded, possibliy an upstream bug there
        // After checking RTSP_PERM_MEDIA_FACTORY_ACCESS anonymously
        // It loads the auth token of the user and checks that users
        // RTSP_PERM_MEDIA_FACTORY_CONSTRUCT allowing them to play
        // As a result of this we must ensure that if anonymous is not granted RTSP_PERM_MEDIA_FACTORY_ACCESS
        // As a part of permitted users then we must allow it to access
        // at least RTSP_PERM_MEDIA_FACTORY_ACCESS but not RTSP_PERM_MEDIA_FACTORY_CONSTRUCT
        // Watching Actually happens during RTSP_PERM_MEDIA_FACTORY_CONSTRUCT
        // So this should be OK to do.
        // FYI: If no RTSP_PERM_MEDIA_FACTORY_ACCESS then server returns 404 not found
        //      If yes RTSP_PERM_MEDIA_FACTORY_ACCESS but no RTSP_PERM_MEDIA_FACTORY_CONSTRUCT
        //        server returns 401 not authourised
        if !permitted_roles
            .iter()
            .map(|i| i.as_ref())
            .collect::<HashSet<&str>>()
            .contains(&"anonymous")
        {
            self.add_role_from_structure(
                &Structure::builder("anonymous")
                    .field(RTSP_PERM_MEDIA_FACTORY_ACCESS, true)
                    .build(),
            );
        }
    }
}

unsafe impl Send for NeoMediaFactory {}
unsafe impl Sync for NeoMediaFactory {}

pub(crate) struct NeoMediaFactoryImpl {
    #[allow(clippy::type_complexity)]
    call_back: Arc<Mutex<Option<Arc<dyn Fn(Element) -> AnyResult<Option<Element>> + Send + Sync>>>>,
    /// DESCRIBE gate, evaluated in the `construct` vfunc BEFORE any element
    /// exists. `true` refuses the request: `construct` returns None, so
    /// gst-rtsp-server's `find_media` answers the client with 400 and nothing
    /// else happens. Refusing here rather than by returning None from
    /// `create_element` avoids two CRITICALs: the gstreamer-rs 0.23
    /// `create_element` trampoline calls `g_object_force_floating` on the NULL
    /// it is handed (`GLib-GObject-CRITICAL ... G_IS_OBJECT`), and
    /// rtsp-media-factory.c's `default_construct` then logs
    /// `g_critical ("could not create element")`. `construct` returning NULL
    /// hits neither (rtsp-media-factory.c: `media = klass->construct (...)`,
    /// no assertion on a NULL result).
    #[allow(clippy::type_complexity)]
    describe_gate: std::sync::Mutex<Option<Arc<dyn Fn() -> bool + Send + Sync>>>,
}

impl Default for NeoMediaFactoryImpl {
    fn default() -> Self {
        debug!("Constructing Factor Impl");
        // Prepare thread that sends data into the appsrcs
        Self {
            call_back: Arc::new(Mutex::new(None)),
            describe_gate: std::sync::Mutex::new(None),
        }
    }
}

impl NeoMediaFactoryImpl {
    async fn set_callback<F>(&self, callback: F)
    where
        F: Fn(Element) -> AnyResult<Option<Element>> + Send + Sync + 'static,
    {
        self.call_back.lock().await.replace(Arc::new(callback));
    }
    fn set_describe_gate<F>(&self, gate: F)
    where
        F: Fn() -> bool + Send + Sync + 'static,
    {
        self.describe_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .replace(Arc::new(gate));
    }
    /// True if the DESCRIBE gate refuses this request. A poisoned lock or an
    /// unset gate both mean "not gated".
    fn describe_refused(&self) -> bool {
        let gate = self
            .describe_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        gate.map(|g| g()).unwrap_or(false)
    }
    fn build_pipeline(&self, media: Element) -> AnyResult<Option<Element>> {
        match self.call_back.blocking_lock().as_ref() {
            Some(call) => {
                let new_media = call(media);
                match new_media {
                    Ok(new_media) => Ok(new_media),
                    Err(e) => {
                        log::debug!("Media source is currently restarting: {e:?}");
                        Ok(None)
                    }
                }
            }
            None => Ok(None),
        }
    }
}

impl ObjectImpl for NeoMediaFactoryImpl {}
impl RTSPMediaFactoryImpl for NeoMediaFactoryImpl {
    // Refuse a gated DESCRIBE at the media stage, not the element stage.
    // gst_rtsp_media_factory_construct only reaches this vfunc when there is
    // no reusable cached media, i.e. exactly when create_element would have
    // been called, so the gate covers the same requests it did before.
    fn construct(&self, url: &RTSPUrl) -> Option<gstreamer_rtsp_server::RTSPMedia> {
        if self.describe_refused() {
            return None;
        }
        self.parent_construct(url)
    }

    fn create_element(&self, url: &RTSPUrl) -> Option<Element> {
        self.parent_create_element(url)
            .and_then(|orig| self.build_pipeline(orig).expect("Could not build pipeline"))
    }

    // DEBUG INSTRUMENTATION (Phase 1 root-cause capture). When
    // NEOLINK_DUMP_DOT is set, dump the FULL prepared RTSPMedia pipeline —
    // including gst-rtsp-server's own downstream rtpbin / transmit elements,
    // which the pay0-src-pad probe cannot observe — to a .dot file on every
    // media_configure. Comparing a HEALTHY configure against a WEDGED one
    // shows exactly which element/pad is left in the wrong state after a
    // SuspendMode::Reset rebuild on shared media. No-op unless the env var is
    // set, so it is safe to leave compiled in.
    fn media_configure(
        &self,
        media: &gstreamer_rtsp_server::RTSPMedia,
    ) {
        self.parent_media_configure(media);
        if std::env::var("NEOLINK_DUMP_DOT").is_ok() {
            let element = media.element();
            if let Ok(bin) = element.dynamic_cast::<gstreamer::Bin>() {
                let ts = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis())
                    .unwrap_or(0);
                let name = format!("media_configure_{ts}");
                bin.debug_to_dot_file(
                    gstreamer::DebugGraphDetails::all(),
                    &name,
                );
                log::warn!("DOT-DUMP: wrote {name}.dot on media_configure");
            }
        }
    }
}

#[object_subclass]
impl ObjectSubclass for NeoMediaFactoryImpl {
    const NAME: &'static str = "NeoMediaFactory";
    type Type = super::NeoMediaFactory;
    type ParentType = RTSPMediaFactory;
}
