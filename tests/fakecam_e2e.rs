//! End-to-end checks against the fake Baichuan camera (`iso-test/sim/fakecam`).
//!
//! Both tests spawn a real `fakecam` and the `neolink rtsp` binary on
//! loopback, leave the camera with no consumer for longer than every
//! staleness budget in the tree (FRAME_STALENESS_MS 30 s, GATE_STALENESS_MS
//! 60 s), then send a raw RTSP DESCRIBE and read the neolink log.
//!
//! They need the fakecam binary, which is built outside the workspace:
//!
//! ```bash
//! cargo build --release --manifest-path iso-test/sim/fakecam/Cargo.toml
//! NEOLINK_E2E_FAKECAM=iso-test/sim/fakecam/target/release/fakecam cargo test --test fakecam_e2e
//! ```
//!
//! Without `NEOLINK_E2E_FAKECAM` each test prints a SKIP line and passes, so
//! the plain `cargo test` stays green.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct Rig {
    fakecam: Child,
    neolink: Child,
    log: PathBuf,
    rtsp_port: u16,
}

impl Drop for Rig {
    fn drop(&mut self) {
        let _ = self.neolink.kill();
        let _ = self.neolink.wait();
        let _ = self.fakecam.kill();
        let _ = self.fakecam.wait();
    }
}

impl Rig {
    fn log_text(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }
    fn log_count(&self, needle: &str) -> usize {
        self.log_text().lines().filter(|l| l.contains(needle)).count()
    }
}

fn fakecam_bin() -> Option<PathBuf> {
    std::env::var_os("NEOLINK_E2E_FAKECAM").map(PathBuf::from)
}

/// Start fakecam + neolink on private ports. `idle_disconnect` is written
/// into the camera config verbatim.
fn start(label: &str, idle_disconnect: bool, rtsp_port: u16, cam_port: u16, ctl_port: u16) -> Rig {
    let fakecam_path = fakecam_bin().expect("checked by caller");
    let dir = std::env::temp_dir().join(format!("neolink-e2e-{label}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let config = dir.join("neolink.toml");
    std::fs::write(
        &config,
        format!(
            "bind = \"127.0.0.1\"\nbind_port = {rtsp_port}\n\n[[cameras]]\nname = \"testcam\"\n\
             address = \"127.0.0.1:{cam_port}\"\nusername = \"admin\"\npassword = \"password123\"\n\
             stream = \"main\"\ndiscovery = \"none\"\nmax_encryption = \"bcencrypt\"\n\
             push_notifications = false\nidle_disconnect = {idle_disconnect}\n"
        ),
    )
    .expect("write config");
    let fakecam = Command::new(&fakecam_path)
        .args([
            "--bind",
            &format!("127.0.0.1:{cam_port}"),
            "--control",
            &format!("127.0.0.1:{ctl_port}"),
            "--fps",
            "15",
        ])
        .env("RUST_LOG", "info")
        .stdout(Stdio::null())
        .stderr(std::fs::File::create(dir.join("fakecam.log")).expect("fakecam log"))
        .spawn()
        .expect("spawn fakecam");
    std::thread::sleep(Duration::from_secs(2));
    let log = dir.join("neolink.log");
    let neolink = Command::new(env!("CARGO_BIN_EXE_neolink"))
        .args(["rtsp", "--config"])
        .arg(&config)
        .env("RUST_LOG", "neolink=info,neolink_core=warn")
        .stdout(Stdio::null())
        .stderr(std::fs::File::create(&log).expect("neolink log"))
        .spawn()
        .expect("spawn neolink");
    Rig {
        fakecam,
        neolink,
        log,
        rtsp_port,
    }
}

/// One raw DESCRIBE; returns the status line, or the error text.
fn describe(rtsp_port: u16) -> String {
    let request = format!(
        "DESCRIBE rtsp://127.0.0.1:{rtsp_port}/testcam/mainStream RTSP/1.0\r\nCSeq: 1\r\n\
         Accept: application/sdp\r\n\r\n"
    );
    let addr = format!("127.0.0.1:{rtsp_port}");
    let attempt = || -> std::io::Result<String> {
        let mut s = TcpStream::connect_timeout(&addr.parse().unwrap(), Duration::from_secs(5))?;
        s.set_read_timeout(Some(Duration::from_secs(15)))?;
        s.write_all(request.as_bytes())?;
        let mut buf = [0u8; 4096];
        let n = s.read(&mut buf)?;
        Ok(String::from_utf8_lossy(&buf[..n])
            .lines()
            .next()
            .unwrap_or_default()
            .to_string())
    };
    attempt().unwrap_or_else(|e| format!("ERR {e}"))
}

/// DESCRIBE every 5 s until one returns 200 or `budget` runs out; returns
/// every status line seen.
fn describe_until_ok(rtsp_port: u16, budget: Duration) -> Vec<String> {
    let deadline = Instant::now() + budget;
    let mut seen = vec![];
    loop {
        let status = describe(rtsp_port);
        let ok = status.contains(" 200 ");
        seen.push(status);
        if ok || Instant::now() >= deadline {
            return seen;
        }
        std::thread::sleep(Duration::from_secs(5));
    }
}

/// Issue #202: with `idle_disconnect = true` the BC session is dropped on
/// purpose 30 s after the last consumer leaves. A DESCRIBE after more than
/// GATE_STALENESS_MS of idle is the request that reconnects the camera, and
/// the fix 14 gate must let it through.
#[test]
fn idle_disconnect_camera_serves_a_describe_after_a_long_idle() {
    if fakecam_bin().is_none() {
        eprintln!("SKIP idle_disconnect_camera_serves_a_describe_after_a_long_idle: NEOLINK_E2E_FAKECAM unset");
        return;
    }
    let rig = start("idledisc", true, 8561, 9061, 9071);
    std::thread::sleep(Duration::from_secs(75));
    let seen = describe_until_ok(rig.rtsp_port, Duration::from_secs(40));
    let gate_lines = rig.log_count("fix 14 liveness gate");
    let deaths = rig.log_count("declaring camera dead");
    eprintln!("[e2e idle_disconnect] DESCRIBE after 75 s idle: {seen:?}; gate refusals {gate_lines}; dead-declares {deaths}");
    assert!(
        seen.iter().any(|s| s.contains(" 200 ")),
        "no DESCRIBE succeeded after a 75 s idle with idle_disconnect: {:?}", seen
    );
    assert_eq!(gate_lines, 0, "the fix 14 gate refused an idle_disconnect camera");
}

/// fix 17: with `idle_disconnect = false` (the default) and no consumer the
/// BC session stays up and the camera is never asked for video. The
/// watchdog must not read that silence as a death; the session should be
/// the same one for the whole idle period and still serve a DESCRIBE.
#[test]
fn idle_camera_without_a_consumer_keeps_its_bc_session() {
    if fakecam_bin().is_none() {
        eprintln!("SKIP idle_camera_without_a_consumer_keeps_its_bc_session: NEOLINK_E2E_FAKECAM unset");
        return;
    }
    let rig = start("idle", false, 8562, 9062, 9072);
    std::thread::sleep(Duration::from_secs(80));
    let deaths = rig.log_count("declaring camera dead");
    let reconnects = rig.log_count("Attempt reconnect");
    let seen = describe_until_ok(rig.rtsp_port, Duration::from_secs(30));
    eprintln!("[e2e idle] 80 s idle, no consumer: dead-declares {deaths}, reconnects {reconnects}; DESCRIBE {seen:?}");
    assert!(seen.iter().any(|s| s.contains(" 200 ")), "DESCRIBE failed after idle: {:?}", seen);
    assert_eq!(deaths, 0, "an unwatched camera was declared dead {deaths} time(s) in 80 s");
    assert_eq!(reconnects, 0, "an unwatched camera reconnected {reconnects} time(s) in 80 s");
}
