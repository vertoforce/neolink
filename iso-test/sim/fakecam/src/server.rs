//! The BC connection state machine of the fake camera.
//!
//! One task per accepted TCP connection. It decodes client requests with
//! [`ServerCodec`] (the server-side mirror of neolink's own BC codec) and
//! writes replies back through an mpsc queue that a writer task drains, so
//! the video pump and the request handler can both emit packets without
//! fighting over the socket.

use crate::media::AccessUnit;
use crate::{Config, Mode};
use anyhow::{anyhow, Result};
use bytes::BytesMut;
use log::*;
use neolink_core::bc::crypto::EncryptionProtocol;
use neolink_core::bc::model::*;
use neolink_core::bc::xml::*;
use neolink_core::bcmedia::model::*;
use neolink_core::test_server::{encode_bcmedia, make_aes_key, ServerCodec};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch, Mutex};

static NONCE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Shared runtime state: the fault-injection mode and a "kill epoch" that is
/// bumped to drop every live connection.
#[derive(Clone)]
pub struct Shared {
    /// Current fault-injection mode.
    pub mode: watch::Sender<Mode>,
    /// Incremented by the `die` command; connections watching it hang up.
    pub kill: watch::Sender<u64>,
}

impl Shared {
    /// Fresh state in [`Mode::Normal`].
    pub fn new(mode: Mode) -> Self {
        Self {
            mode: watch::Sender::new(mode),
            kill: watch::Sender::new(0),
        }
    }
}

fn make_nonce() -> String {
    let n = NONCE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    // Reolink nonces are opaque alphanumeric strings; anything XML-safe works.
    format!("{:016X}{:04X}", t, n & 0xffff)
}

/// The first 31 uppercase hex digits of the MD5, which is what the BC login
/// hash is (`md5_string(.., Truncate)` in `neolink_core`).
fn md5_trunc(input: &str) -> String {
    let full = format!("{:X}", md5::compute(input));
    full[..31].to_string()
}

/// Everything the fake camera needs to answer a request.
struct Conn {
    cfg: Arc<Config>,
    codec: Arc<Mutex<ServerCodec>>,
    out: mpsc::Sender<Vec<u8>>,
    nonce: String,
    logged_in: bool,
    /// msg_num of the currently running video subscription, if any.
    video: Option<tokio::task::JoinHandle<()>>,
    mode: watch::Receiver<Mode>,
    frames: Arc<Vec<AccessUnit>>,
}

impl Conn {
    /// Queue a reply, unless the camera is currently playing dead.
    async fn reply(&self, bc: Bc) -> Result<()> {
        if *self.mode.borrow() == Mode::Hang {
            trace!("hang mode: swallowing reply to msg_id {}", bc.meta.msg_id);
            return Ok(());
        }
        let bytes = {
            let codec = self.codec.lock().await;
            codec.encode(&bc)?
        };
        self.out
            .send(bytes)
            .await
            .map_err(|_| anyhow!("connection writer gone"))
    }

    fn meta(&self, req: &BcMeta, class: u16, code: u16) -> BcMeta {
        BcMeta {
            msg_id: req.msg_id,
            channel_id: req.channel_id,
            stream_type: req.stream_type,
            msg_num: req.msg_num,
            response_code: code,
            class,
        }
    }

    /// A bare `200 OK` acknowledgement.
    async fn ack(&self, req: &BcMeta) -> Result<()> {
        self.reply(Bc::new_from_meta(self.meta(req, 0x0000, 200)))
            .await
    }

    /// A bare `400 Bad Request` — how a real camera reports a message it does
    /// not implement. neolink treats this as a soft, retryable failure.
    async fn nack(&self, req: &BcMeta) -> Result<()> {
        self.reply(Bc::new_from_meta(self.meta(req, 0x0000, 400)))
            .await
    }

    async fn xml_ok(&self, req: &BcMeta, xml: BcXml) -> Result<()> {
        self.reply(Bc::new_from_xml(self.meta(req, 0x0000, 200), xml))
            .await
    }

    async fn handle(&mut self, bc: Bc) -> Result<()> {
        let meta = bc.meta;
        match (meta.msg_id, &bc.body) {
            // ---- Login, stage 1: the legacy message that asks to be upgraded
            (MSG_ID_LOGIN, BcBody::LegacyMsg(_)) => {
                // The low byte of the request's response_code is the highest
                // encryption the client will accept (0xdc00/01/12). We answer
                // with 0xdd<chosen>. Choosing lower than requested is normal
                // camera behaviour, so we always just announce our configured
                // level.
                let requested = meta.response_code & 0xff;
                let chosen = self.cfg.encryption.min(requested);
                debug!(
                    "login stage 1: client max enc 0x{:02x}, answering 0x{:02x}, nonce {}",
                    requested, chosen, self.nonce
                );
                let reply = Bc::new_from_xml(
                    BcMeta {
                        msg_id: MSG_ID_LOGIN,
                        channel_id: meta.channel_id,
                        stream_type: meta.stream_type,
                        msg_num: meta.msg_num,
                        response_code: 0xdd00 | chosen,
                        // 0x6614 is "modern, 20 byte header, no payload
                        // offset" — the one class a real camera uses for this
                        // reply and the reason BcHeader::is_modern exists.
                        class: 0x6614,
                    },
                    BcXml {
                        encryption: Some(Encryption {
                            version: xml_ver(),
                            type_: "md5".to_string(),
                            nonce: self.nonce.clone(),
                        }),
                        ..Default::default()
                    },
                );
                // The protocol has to be armed *before* this reply goes out:
                // the client decrypts the payload of a 0xdd__ login reply
                // with the protocol that reply announces, not with the one
                // that was in force when it was sent.
                {
                    let mut codec = self.codec.lock().await;
                    codec.set_encryption(match chosen {
                        0x00 => EncryptionProtocol::Unencrypted,
                        0x01 => EncryptionProtocol::BCEncrypt,
                        0x02 => EncryptionProtocol::aes(make_aes_key(
                            &self.nonce,
                            &self.cfg.password,
                        )),
                        0x12 => EncryptionProtocol::full_aes(make_aes_key(
                            &self.nonce,
                            &self.cfg.password,
                        )),
                        n => return Err(anyhow!("unsupported encryption byte {n:#x}")),
                    });
                }
                self.reply(reply).await?;
                Ok(())
            }
            // ---- Login, stage 2: the modern message with the salted hashes
            (
                MSG_ID_LOGIN,
                BcBody::ModernMsg(ModernMsg {
                    payload:
                        Some(BcPayloads::BcXml(BcXml {
                            login_user: Some(user),
                            ..
                        })),
                    ..
                }),
            ) => {
                let want_user = md5_trunc(&format!("{}{}", self.cfg.username, self.nonce));
                let want_pass = md5_trunc(&format!("{}{}", self.cfg.password, self.nonce));
                if user.user_name != want_user || user.password != want_pass {
                    warn!("login rejected: hash mismatch");
                    return self.nack(&meta).await;
                }
                self.logged_in = true;
                info!("login accepted for user {}", self.cfg.username);
                self.xml_ok(
                    &meta,
                    BcXml {
                        device_info: Some(DeviceInfo {
                            version: Some(xml_ver()),
                            resolution: Some(Resolution {
                                name: format!("{}*{}", self.cfg.width, self.cfg.height),
                                width: self.cfg.width,
                                height: self.cfg.height,
                            }),
                        }),
                        ..Default::default()
                    },
                )
                .await
            }
            (MSG_ID_LOGOUT, _) => {
                self.logged_in = false;
                self.ack(&meta).await
            }
            // ---- The abilities gate. start_video refuses to run without
            // "preview_rw", so this reply is load bearing.
            (MSG_ID_ABILITY_INFO, _) => {
                let token = |v: &str| {
                    Some(AbilityInfoToken {
                        sub_module: vec![AbilityInfoSubModule {
                            channel_id: Some(meta.channel_id),
                            ability_value: v.to_string(),
                        }],
                    })
                };
                self.xml_ok(
                    &meta,
                    BcXml {
                        ability_info: Some(AbilityInfo {
                            username: self.cfg.username.clone(),
                            system: token("general_rw, version_ro, upgrade_rw, autoReboot_rw"),
                            network: token("port_rw, ddns_rw, email_rw, ftp_rw, ntp_rw"),
                            streaming: token("preview_rw, streamTable_ro, talk_rw"),
                            video: token("osdName_rw, osdTime_rw, videoClip_rw"),
                            image: token("ispBasic_rw, ledState_rw"),
                            security: token("user_rw, onlineUser_rw"),
                            replay: token("playback_rw, record_rw"),
                            alarm: token("motion_rw"),
                            ptz: None,
                            io: None,
                        }),
                        ..Default::default()
                    },
                )
                .await
            }
            // ---- Liveness. neolink's watchdog pings with MSG_ID_PING and
            // wants a LinkType back (get_linktype), so a bare 200 is not
            // enough.
            (MSG_ID_PING, _) => {
                self.xml_ok(
                    &meta,
                    BcXml {
                        link_type: Some(LinkType {
                            link_type: "LAN".to_string(),
                        }),
                        ..Default::default()
                    },
                )
                .await
            }
            (MSG_ID_VERSION, _) => {
                self.xml_ok(
                    &meta,
                    BcXml {
                        version_info: Some(VersionInfo {
                            name: "testcam".to_string(),
                            model: Some("FakeCam".to_string()),
                            serialNumber: "0000000000000000".to_string(),
                            buildDay: "build 00000000".to_string(),
                            hardwareVersion: "IPC_FAKE".to_string(),
                            cfgVersion: "v0.0.0.0".to_string(),
                            firmwareVersion: "v0.0.0.0_00000000".to_string(),
                            detail: "IPC_FAKE00000000000000000".to_string(),
                        }),
                        ..Default::default()
                    },
                )
                .await
            }
            (MSG_ID_UID, _) => {
                self.xml_ok(
                    &meta,
                    BcXml {
                        uid: Some(Uid {
                            version: xml_ver(),
                            uid: "FAKECAM0000000000".to_string(),
                        }),
                        ..Default::default()
                    },
                )
                .await
            }
            (MSG_ID_STREAM_INFO_LIST, _) => {
                let table = |name: &str, w: u32, h: u32| EncodeTable {
                    name: name.to_string(),
                    resolution: StreamResolution {
                        width: w,
                        height: h,
                    },
                    default_framerate: self.cfg.fps as u32,
                    default_bitrate: 1024,
                    framerate_table: "25,22,20,18,16,15,12,10,8,6,4,2".to_string(),
                    bitrate_table: "1024,1536,2048,3072,4096".to_string(),
                };
                self.xml_ok(
                    &meta,
                    BcXml {
                        stream_info_list: Some(StreamInfoList {
                            stream_infos: vec![StreamInfo {
                                channel_bits: 1,
                                encode_tables: vec![
                                    table("mainStream", self.cfg.width, self.cfg.height),
                                    table("subStream", self.cfg.width / 2, self.cfg.height / 2),
                                ],
                            }],
                        }),
                        ..Default::default()
                    },
                )
                .await
            }
            // ---- Clock. neolink reads it on connect and will happily warn
            // and carry on if it fails, but answering keeps the log clean.
            (MSG_ID_GET_GENERAL, _) => {
                self.xml_ok(
                    &meta,
                    BcXml {
                        system_general: Some(SystemGeneral {
                            version: xml_ver(),
                            time_zone: Some(0),
                            year: Some(2026),
                            month: Some(1),
                            day: Some(1),
                            hour: Some(0),
                            minute: Some(0),
                            second: Some(0),
                            osd_format: Some("DMY".to_string()),
                            time_format: Some(0),
                            language: Some("English".to_string()),
                            device_name: Some("testcam".to_string()),
                        }),
                        ..Default::default()
                    },
                )
                .await
            }
            (MSG_ID_SET_GENERAL, _) => self.ack(&meta).await,
            (MSG_ID_MOTION_REQUEST, _) => self.ack(&meta).await,
            // ---- Video
            (MSG_ID_VIDEO, _) => {
                if !self.logged_in {
                    return self.nack(&meta).await;
                }
                self.start_video(meta).await
            }
            (MSG_ID_VIDEO_STOP, _) => {
                if let Some(handle) = self.video.take() {
                    handle.abort();
                }
                self.ack(&meta).await
            }
            // Anything else gets the same 400 a real camera gives for a
            // feature it does not have.
            (id, _) => {
                debug!("unimplemented msg_id {id}, replying 400");
                self.nack(&meta).await
            }
        }
    }

    /// Reply 200 with a BcMedia InfoV2 header, then pump frames forever.
    async fn start_video(&mut self, meta: BcMeta) -> Result<()> {
        if let Some(handle) = self.video.take() {
            handle.abort();
        }
        info!(
            "start_video: channel {} stream_type {} msg_num {}",
            meta.channel_id, meta.stream_type, meta.msg_num
        );

        // A real camera answers the Preview request with a single packet that
        // is *both* the 200 acknowledgement and the first binary payload. The
        // binaryData=1 extension is what puts the client's decoder into
        // binary mode for this msg_num; every packet after it can then omit
        // the extension entirely.
        let info = BcMedia::InfoV2(BcMediaInfoV2 {
            video_width: self.cfg.width,
            video_height: self.cfg.height,
            fps: self.cfg.fps,
            start_year: 26,
            start_month: 1,
            start_day: 1,
            start_hour: 0,
            start_min: 0,
            start_seconds: 0,
            end_year: 26,
            end_month: 1,
            end_day: 1,
            end_hour: 0,
            end_min: 0,
            end_seconds: 0,
        });
        let first = Bc::new(
            BcMeta {
                msg_id: MSG_ID_VIDEO,
                channel_id: meta.channel_id,
                stream_type: meta.stream_type,
                msg_num: meta.msg_num,
                response_code: 200,
                class: 0x0000,
            },
            Some(Extension {
                binary_data: Some(1),
                channel_id: Some(meta.channel_id),
                ..Default::default()
            }),
            Some(BcPayloads::Binary(encode_bcmedia(&info)?)),
        );
        self.reply(first).await?;

        let frames = self.frames.clone();
        let out = self.out.clone();
        let codec = self.codec.clone();
        let mut mode = self.mode.clone();
        let fps = self.cfg.fps.max(1) as u32;
        let handle = tokio::spawn(async move {
            let period = std::time::Duration::from_micros(1_000_000u64 / fps as u64);
            let step = 1_000_000u32 / fps;
            let mut ticker = tokio::time::interval(period);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut micros: u32 = 0;
            let mut idx = 0usize;
            loop {
                ticker.tick().await;
                match *mode.borrow_and_update() {
                    // freeze: keep the connection and the pings alive but
                    // stop delivering pictures. This is the mode that
                    // reproduces neolink's "pings OK but no frames" wedge.
                    Mode::Freeze | Mode::Hang => continue,
                    Mode::Normal => {}
                }
                let au = &frames[idx % frames.len()];
                idx += 1;
                let time = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs() as u32)
                    .unwrap_or(0);
                let media = if au.keyframe {
                    BcMedia::Iframe(BcMediaIframe {
                        video_type: VideoType::H264,
                        microseconds: micros,
                        time: Some(time),
                        data: au.data.clone(),
                    })
                } else {
                    BcMedia::Pframe(BcMediaPframe {
                        video_type: VideoType::H264,
                        microseconds: micros,
                        data: au.data.clone(),
                    })
                };
                micros = micros.wrapping_add(step);
                let payload = match encode_bcmedia(&media) {
                    Ok(p) => p,
                    Err(e) => {
                        error!("bcmedia encode failed: {e}");
                        return;
                    }
                };
                let bc = Bc::new(
                    BcMeta {
                        msg_id: MSG_ID_VIDEO,
                        channel_id: meta.channel_id,
                        stream_type: meta.stream_type,
                        msg_num: meta.msg_num,
                        response_code: 200,
                        class: 0x0000,
                    },
                    None,
                    Some(BcPayloads::Binary(payload)),
                );
                let bytes = {
                    let codec = codec.lock().await;
                    match codec.encode(&bc) {
                        Ok(b) => b,
                        Err(e) => {
                            error!("bc encode failed: {e}");
                            return;
                        }
                    }
                };
                if out.send(bytes).await.is_err() {
                    debug!("video pump stopping: connection closed");
                    return;
                }
            }
        });
        self.video = Some(handle);
        Ok(())
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        if let Some(handle) = self.video.take() {
            handle.abort();
        }
    }
}

/// Serve one accepted TCP connection until it fails, is killed, or the client
/// hangs up.
pub async fn serve(
    stream: TcpStream,
    cfg: Arc<Config>,
    shared: Shared,
    frames: Arc<Vec<AccessUnit>>,
) -> Result<()> {
    let peer = stream.peer_addr().ok();
    let (mut reader, mut writer) = stream.into_split();
    let (out_tx, mut out_rx) = mpsc::channel::<Vec<u8>>(256);
    let writer_task = tokio::spawn(async move {
        while let Some(bytes) = out_rx.recv().await {
            if writer.write_all(&bytes).await.is_err() {
                break;
            }
        }
    });

    let mut kill = shared.kill.subscribe();
    let kill_epoch = *kill.borrow_and_update();
    let mode = shared.mode.subscribe();

    let mut conn = Conn {
        cfg,
        codec: Arc::new(Mutex::new(ServerCodec::new())),
        out: out_tx,
        nonce: make_nonce(),
        logged_in: false,
        video: None,
        mode,
        frames,
    };

    let mut buf = BytesMut::with_capacity(16 * 1024);
    let result = loop {
        tokio::select! {
            _ = kill.changed() => {
                if *kill.borrow() != kill_epoch {
                    info!("die: dropping connection {peer:?}");
                    break Ok(());
                }
            }
            read = reader.read_buf(&mut buf) => {
                match read {
                    Ok(0) => {
                        debug!("client {peer:?} closed the connection");
                        break Ok(());
                    }
                    Ok(_) => {
                        let mut fatal = None;
                        loop {
                            let decoded = {
                                let mut codec = conn.codec.lock().await;
                                codec.decode(&mut buf)
                            };
                            match decoded {
                                Ok(Some(bc)) => {
                                    trace!("<- msg_id {} class {:#06x}", bc.meta.msg_id, bc.meta.class);
                                    if let Err(e) = conn.handle(bc).await {
                                        fatal = Some(anyhow!("handler failed: {e}"));
                                        break;
                                    }
                                }
                                Ok(None) => break,
                                Err(e) => {
                                    fatal = Some(anyhow!("undecodable request: {e}"));
                                    break;
                                }
                            }
                        }
                        if let Some(e) = fatal {
                            break Err(e);
                        }
                    }
                    Err(e) => break Err(e.into()),
                }
            }
        }
    };

    drop(conn);
    writer_task.abort();
    result
}
