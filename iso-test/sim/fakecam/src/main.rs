//! A fake Baichuan (Reolink) camera, good enough for neolink to log into and
//! pull video from, plus a control port for injecting the failures that
//! neolink's watchdogs are supposed to catch.
//!
//! See `README.md` for what is and is not implemented.

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use log::*;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

mod media;
mod server;

use server::Shared;

/// Fault-injection modes, switchable at runtime over the control port.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Mode {
    /// Answer everything, stream video.
    Normal,
    /// Keep answering pings and other control messages, but stop sending
    /// video frames. Reproduces a camera whose BC session is alive while its
    /// encoder is wedged.
    Freeze,
    /// Accept TCP connections but never write a single byte back. Reproduces
    /// a camera that is reachable at the socket level and dead above it.
    Hang,
}

/// Which encryption the fake camera will announce in the login reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Encryption {
    /// No encryption at all (`0xdd00`).
    None,
    /// The fixed-key XOR (`0xdd01`).
    Bcencrypt,
    /// AES-128-CFB on control messages, plaintext media (`0xdd02`).
    Aes,
    /// AES-128-CFB on control messages and media (`0xdd12`).
    Fullaes,
}

impl Encryption {
    fn byte(self) -> u16 {
        match self {
            Encryption::None => 0x00,
            Encryption::Bcencrypt => 0x01,
            Encryption::Aes => 0x02,
            Encryption::Fullaes => 0x12,
        }
    }
}

/// Resolved runtime configuration, shared by every connection.
pub struct Config {
    /// Username the fake camera accepts.
    pub username: String,
    /// Password the fake camera accepts.
    pub password: String,
    /// Highest encryption byte the fake camera will agree to.
    pub encryption: u16,
    /// Advertised and simulated frame rate.
    pub fps: u8,
    /// Advertised video width.
    pub width: u32,
    /// Advertised video height.
    pub height: u32,
}

#[derive(Parser, Debug)]
#[command(about = "A fake Baichuan camera server for testing neolink")]
struct Opt {
    /// Address to serve the BC protocol on
    #[arg(long, default_value = "127.0.0.1:9000")]
    bind: SocketAddr,
    /// Address to serve the fault-injection control port on
    #[arg(long, default_value = "127.0.0.1:9010")]
    control: SocketAddr,
    /// Username the camera accepts
    #[arg(long, default_value = "admin")]
    username: String,
    /// Password the camera accepts
    #[arg(long, default_value = "password123")]
    password: String,
    /// Encryption level to negotiate
    #[arg(long, value_enum, default_value_t = Encryption::Bcencrypt)]
    encryption: Encryption,
    /// Frames per second to emit
    #[arg(long, default_value_t = 15)]
    fps: u8,
    /// Annex-B H.264 file to loop instead of the built-in test pattern
    #[arg(long)]
    h264: Option<PathBuf>,
    /// Advertised video width
    #[arg(long, default_value_t = 640)]
    width: u32,
    /// Advertised video height
    #[arg(long, default_value_t = 360)]
    height: u32,
    /// Mode to start in
    #[arg(long, value_enum, default_value_t = Mode::Normal)]
    mode: Mode,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let opt = Opt::parse();

    let raw = match opt.h264.as_ref() {
        Some(path) => std::fs::read(path)
            .with_context(|| format!("could not read H.264 fixture {}", path.display()))?,
        None => media::DEFAULT_FIXTURE.to_vec(),
    };
    let frames = Arc::new(media::parse_annexb(&raw));
    anyhow::ensure!(!frames.is_empty(), "H.264 fixture contained no frames");
    info!(
        "loaded {} frames ({} keyframes) from {}",
        frames.len(),
        frames.iter().filter(|f| f.keyframe).count(),
        opt.h264
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "built-in test pattern".to_string())
    );

    let cfg = Arc::new(Config {
        username: opt.username.clone(),
        password: opt.password.clone(),
        encryption: opt.encryption.byte(),
        fps: opt.fps,
        width: opt.width,
        height: opt.height,
    });
    let shared = Shared::new(opt.mode);

    let control = TcpListener::bind(opt.control)
        .await
        .with_context(|| format!("could not bind control port {}", opt.control))?;
    let bc = TcpListener::bind(opt.bind)
        .await
        .with_context(|| format!("could not bind BC port {}", opt.bind))?;
    info!("fake camera on {} (mode {:?})", opt.bind, opt.mode);
    info!("control port on {}", opt.control);

    let ctl_shared = shared.clone();
    tokio::spawn(async move {
        loop {
            match control.accept().await {
                Ok((stream, _)) => {
                    let shared = ctl_shared.clone();
                    tokio::spawn(async move {
                        if let Err(e) = control_session(stream, shared).await {
                            debug!("control session ended: {e}");
                        }
                    });
                }
                Err(e) => {
                    error!("control accept failed: {e}");
                    return;
                }
            }
        }
    });

    loop {
        let (stream, peer) = bc.accept().await.context("BC accept failed")?;
        stream.set_nodelay(true).ok();
        info!("connection from {peer}");
        let cfg = cfg.clone();
        let shared = shared.clone();
        let frames = frames.clone();
        tokio::spawn(async move {
            if let Err(e) = server::serve(stream, cfg, shared, frames).await {
                info!("connection {peer} ended: {e}");
            } else {
                debug!("connection {peer} ended cleanly");
            }
        });
    }
}

/// A line-per-command control channel.
///
/// A second TCP port rather than signals, because the scenarios that matter
/// are "switch to X at second N" and that is a one-liner with netcat from any
/// test harness, with no process tree to find.
async fn control_session(stream: TcpStream, shared: Shared) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();
    writer
        .write_all(b"fakecam control: normal|freeze|hang|die|status|quit\n")
        .await?;
    while let Some(line) = lines.next_line().await? {
        let reply = match line.trim().to_ascii_lowercase().as_str() {
            "" => continue,
            "normal" => {
                shared.mode.send_replace(Mode::Normal);
                info!("control: mode -> normal");
                "ok normal\n".to_string()
            }
            "freeze" => {
                shared.mode.send_replace(Mode::Freeze);
                info!("control: mode -> freeze");
                "ok freeze\n".to_string()
            }
            "hang" => {
                shared.mode.send_replace(Mode::Hang);
                info!("control: mode -> hang");
                "ok hang\n".to_string()
            }
            "die" => {
                // One-shot: drop every live BC connection and stay in
                // whatever mode we were in, so the client's reconnect is
                // what gets exercised.
                let epoch = *shared.kill.borrow() + 1;
                shared.kill.send_replace(epoch);
                info!("control: die (kill epoch {epoch})");
                "ok die\n".to_string()
            }
            "status" => format!("mode {:?}\n", *shared.mode.borrow()),
            "quit" => {
                writer.write_all(b"bye\n").await?;
                return Ok(());
            }
            other => format!("err unknown command {other:?}\n"),
        };
        writer.write_all(reply.as_bytes()).await?;
    }
    Ok(())
}
