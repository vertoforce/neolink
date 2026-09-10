//! Does client churn tear down a *shared* RTSP media?
//!
//! This is the root cause behind fix 4 (`c34b277`), reproduced with no camera
//! and no neolink: a `gst-rtsp-server` factory with `set_shared(true)` and
//! `RTSPSuspendMode::Reset` — which is what upstream neolink used — has every
//! client detach run `default_suspend`, and for `Reset` that means
//! `set_state(GST_STATE_NULL)` on the whole shared pipeline. Every other client
//! attached to that pipeline loses it, and neolink's frame-pump starts pushing
//! into a detached appsrc ("App source is closed").
//!
//! The fix is `RTSPSuspendMode::None`, under which `default_suspend` is a
//! no-op and the shared pipeline survives churn.
//!
//! Usage:
//!
//! ```text
//! suspend-churn <none|reset> [cycles]
//! ```
//!
//! Prints one line of TSV: `mode<TAB>cycles<TAB>null_transitions<TAB>medias`.
//! `null_transitions` is the number of times a prepared media went to NULL;
//! `medias` is how many distinct media objects the factory built, which shows
//! whether the pipeline was genuinely shared or silently rebuilt per client.
//!
//! Only `gstreamer1.0-plugins-base` and `-good` are needed (JPEG rather than
//! H.264 payloading, so `-ugly`/`x264enc` is not required; the payload format
//! is irrelevant to suspend behaviour).

use glib::translate::IntoGlib;
use gstreamer::prelude::*;
use gstreamer_rtsp_server::prelude::*;
use gstreamer_rtsp_server::{RTSPMediaFactory, RTSPServer, RTSPSuspendMode};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

const LAUNCH: &str = "( videotestsrc is-live=true ! video/x-raw,framerate=15/1,width=320,height=240 \
                       ! jpegenc ! rtpjpegpay name=pay0 pt=26 )";
/// Same pipeline with a 2 s stall before the payloader, so preroll takes
/// long enough for a client to give up first — the fix 4 trigger, where
/// camera B's 5120x1552 panorama prerolled slower than the requesting client's
/// timeout and the transport never completed.
const LAUNCH_SLOW: &str = "( videotestsrc is-live=true ! video/x-raw,framerate=15/1,width=320,height=240 \
                            ! identity sleep-time=2000000 ! jpegenc ! rtpjpegpay name=pay0 pt=26 )";

fn main() {
    let mut args = std::env::args().skip(1);
    let mode_arg = args.next().unwrap_or_else(|| "none".into());
    let cycles: usize = args
        .next()
        .and_then(|c| c.parse().ok())
        .unwrap_or(6);
    // With a holder attached the media is shared across the churn, which is
    // the production shape. Without one, each churning client is the last
    // client to leave, which is the case gst-rtsp-server suspends on — useful
    // as a positive control that the instrumentation fires at all.
    let rest: Vec<String> = args.collect();
    let holder_wanted = !rest.iter().any(|a| a == "no-holder");
    let slow_preroll = rest.iter().any(|a| a == "slow-preroll");
    // `abort` churners speak raw RTSP and hang up after SETUP without ever
    // sending PLAY, which is what a client that times out mid-preroll looks
    // like to the server.
    let abort_churn = rest.iter().any(|a| a == "abort");

    let mode = match mode_arg.as_str() {
        "none" => RTSPSuspendMode::None,
        "reset" => RTSPSuspendMode::Reset,
        "pause" => RTSPSuspendMode::Pause,
        other => {
            eprintln!("unknown suspend mode `{other}`; expected none|reset|pause");
            std::process::exit(2);
        }
    };

    gstreamer::init().expect("gstreamer init");
    for element in ["videotestsrc", "jpegenc", "rtpjpegpay", "rtspsrc", "fakesink"] {
        if gstreamer::ElementFactory::find(element).is_none() {
            eprintln!("SKIP: missing GStreamer element `{element}` (install gstreamer1.0-plugins-base and -good)");
            std::process::exit(77);
        }
    }

    let null_transitions = Arc::new(AtomicUsize::new(0));
    let medias = Arc::new(AtomicUsize::new(0));

    let server = RTSPServer::new();
    server.set_service("0"); // let the kernel pick a free port
    let factory = RTSPMediaFactory::new();
    factory.set_launch(if slow_preroll { LAUNCH_SLOW } else { LAUNCH });
    factory.set_shared(true);
    factory.set_suspend_mode(mode);

    {
        let null_transitions = null_transitions.clone();
        let medias = medias.clone();
        factory.connect_media_constructed(move |_, media| {
            medias.fetch_add(1, Ordering::Relaxed);
            let null_transitions = null_transitions.clone();
            media.connect_new_state(move |_, state| {
                if state == gstreamer::State::Null.into_glib() {
                    null_transitions.fetch_add(1, Ordering::Relaxed);
                }
            });
        });
    }

    server.mount_points().unwrap().add_factory("/test", factory);
    let _id = server.attach(None).expect("attach rtsp server");
    let port = server.bound_port();
    let url = format!("rtsp://127.0.0.1:{port}/test");

    // Run the server's main loop on its own thread.
    let main_loop = glib::MainLoop::new(None, false);
    {
        let main_loop = main_loop.clone();
        std::thread::spawn(move || main_loop.run());
    }

    // A long-lived consumer, so the media stays shared across the churn. This
    // is the client that loses its pipeline when a *different* client's detach
    // resets the shared media.
    let holder = holder_wanted.then(|| client(&url));
    std::thread::sleep(Duration::from_secs(3));

    for _ in 0..cycles {
        if abort_churn {
            abort_client(port);
            std::thread::sleep(Duration::from_millis(1200));
            continue;
        }
        let churner = client(&url);
        std::thread::sleep(Duration::from_millis(1500));
        // PAUSE first: that is the request that reaches
        // gst_rtsp_media_suspend. Then TEARDOWN.
        let _ = churner.set_state(gstreamer::State::Paused);
        std::thread::sleep(Duration::from_millis(1200));
        let _ = churner.set_state(gstreamer::State::Null);
        drop(churner);
        std::thread::sleep(Duration::from_millis(1200));
    }

    if let Some(holder) = holder {
        let _ = holder.set_state(gstreamer::State::Null);
    }
    std::thread::sleep(Duration::from_millis(500));

    println!(
        "{mode_arg}\tholder={holder_wanted}\tslow={slow_preroll}\tabort={abort_churn}\t{cycles}\t{}\t{}",
        null_transitions.load(Ordering::Relaxed),
        medias.load(Ordering::Relaxed)
    );

    main_loop.quit();
    drop(_id);
}

fn client(url: &str) -> gstreamer::Element {
    let pipeline = gstreamer::parse::launch(&format!(
        "rtspsrc location={url} protocols=tcp latency=0 ! fakesink sync=false"
    ))
    .expect("client pipeline");
    pipeline
        .set_state(gstreamer::State::Playing)
        .expect("client to PLAYING");
    pipeline
}

/// A client that DESCRIBEs, SETUPs, then drops the socket without a PLAY or a
/// TEARDOWN.
fn abort_client(port: i32) {
    use std::io::{Read, Write};
    let Ok(mut sock) = std::net::TcpStream::connect(("127.0.0.1", port as u16)) else {
        return;
    };
    let _ = sock.set_read_timeout(Some(Duration::from_millis(800)));
    let url = format!("rtsp://127.0.0.1:{port}/test");
    let mut buf = [0u8; 4096];
    let _ = sock.write_all(
        format!("DESCRIBE {url} RTSP/1.0\r\nCSeq: 1\r\nAccept: application/sdp\r\n\r\n")
            .as_bytes(),
    );
    let _ = sock.read(&mut buf);
    let _ = sock.write_all(
        format!(
            "SETUP {url}/stream=0 RTSP/1.0\r\nCSeq: 2\r\nTransport: RTP/AVP/TCP;unicast;interleaved=0-1\r\n\r\n"
        )
        .as_bytes(),
    );
    let _ = sock.read(&mut buf);
    // Hang up mid-session.
    drop(sock);
}

// ---------------------------------------------------------------------------
// MEASURED RESULT, 2026-09-10, stock gst-rtsp-server 1.26.2 (Debian trixie):
// this harness does NOT reproduce the fix 4 defect.
//
//   mode   holder slow  abort cycles null_transitions medias
//   reset  true   false false 6      0                1
//   none   true   false false 6      0                1
//   reset  false  false false 4      0                1
//   none   false  false false 4      0                1
//   reset  true   true  true  4      0                1
//   none   true   true  true  4      0                1
//
// With GST_DEBUG=rtspmedia:5 the whole run emits 24 log lines and not one of
// them is a suspend: the media is prepared once (take_pipeline, collect_streams,
// create_stream, prepare, set_status, get_status) and is never suspended or
// unprepared, in either mode. So on 1.26.2 none of PLAY/PAUSE/TEARDOWN churn,
// last-client-leaves, or abort-mid-SETUP against a slow-prerolling shared
// factory reaches gst_rtsp_media_suspend at all, and the suspend mode makes no
// difference to anything this harness can see.
//
// The fix 4 root-cause analysis was done against gst-rtsp-server 1.22 with
// neolink's own NeoMediaFactory (appsrc fed asynchronously through
// create_element), not a self-contained videotestsrc launch string. Either the
// trigger needs that factory or 1.26 no longer takes the path. Until one of
// those is shown, SuspendMode::None is UNCONFIRMED by simulation — it must not
// be claimed upstream on the strength of this file.
// ---------------------------------------------------------------------------
