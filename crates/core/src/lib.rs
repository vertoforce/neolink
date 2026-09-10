#![warn(unused_crate_dependencies)]
#![warn(missing_docs)]
//! # Neolink-Core
//!
//! Neolink-Core is a rust library for interacting with reolink and family cameras.
//!
//! Most high level camera controls are in the [`bc_protocol`] module
//!
//! A camera can be initialised with
//!
//! ```no_run
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! use neolink_core::bc_protocol::{BcCamera, BcCameraOpt, DiscoveryMethods, ConnectionProtocol, Credentials};
//! let options = BcCameraOpt {
//!     name: "CamName".to_string(),
//!     channel_id: 0,
//!     addrs: ["192.168.1.1".parse().unwrap()].to_vec(),
//!     port: Some(9000),
//!     uid: Some("CAMUID".to_string()),
//!     protocol: ConnectionProtocol::TcpUdp,
//!     discovery: DiscoveryMethods::Relay,
//!     credentials: Credentials {
//!         username: "username".to_string(),
//!         password: Some("password".to_string()),
//!     },
//!     debug: false,
//!     max_discovery_retries: 10,
//! };
//! let mut camera = BcCamera::new(&options).await.unwrap();
//! # })
//! ```
//!
//! After that login can be conducted with
//!
//! ```no_run
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! # use neolink_core::bc_protocol::{BcCamera, BcCameraOpt, DiscoveryMethods, ConnectionProtocol, Credentials};
//! # let options = BcCameraOpt {
//! #    name: "CamName".to_string(),
//! #    channel_id: 0,
//! #    addrs: ["192.168.1.1".parse().unwrap()].to_vec(),
//! #    port: Some(9000),
//! #    uid: Some("CAMUID".to_string()),
//! #    protocol: ConnectionProtocol::TcpUdp,
//! #    discovery: DiscoveryMethods::Relay,
//! #    credentials: Credentials {
//! #        username: "username".to_string(),
//! #        password: Some("password".to_string()),
//! #    },
//! #    debug: false,
//! #    max_discovery_retries: 10,
//! # };
//! # let mut camera = BcCamera::new(&options).await.unwrap();
//! camera.login().await;
//! # })
//! ```
//! For further commands see the [`bc_protocol::BcCamera`] struct.
//!

/// Contains low level BC structures and formats
pub mod bc;
/// Contains high level interfaces for the camera
pub mod bc_protocol;
/// Contains low level structures and formats for the media substream
pub mod bcmedia;
///  Contains low level structures and formats for the udpstream
pub mod bcudp;

/// This is the top level error structure of the library
///
/// Most commands will either return their `Ok(result)` or this `Err(Error)`
pub use bc_protocol::Error;

pub(crate) use bc_protocol::{Credentials, Result};

pub(crate) type NomErrorType<'a> = nom::error::VerboseError<&'a [u8]>;

/// Server-side (fake camera) codec helpers.
///
/// The rest of this crate is written from the *client's* point of view: it
/// encodes requests and decodes replies. A fake camera needs exactly the
/// mirror of that, so this module re-exposes the otherwise crate-private
/// [`bc::model::Bc`] and [`bcmedia::model::BcMedia`] codecs with the direction
/// reversed. Reusing them means the simulator cannot drift from the real
/// wire format.
///
/// Gated behind the non-default `test-server` feature. Nothing in here is
/// reachable from the `neolink` binary; it exists for
/// `iso-test/sim/fakecam`.
#[cfg(feature = "test-server")]
pub mod test_server {
    use crate::bc::crypto::EncryptionProtocol;
    use crate::bc::model::{Bc, BcBody, BcContext, BcMeta, LegacyMsg, MSG_ID_LOGIN};
    use crate::bc_protocol::Credentials;
    use crate::bcmedia::model::BcMedia;
    use crate::Error;
    use bytes::BytesMut;

    /// Per-connection codec state for a fake camera.
    ///
    /// Holds the negotiated encryption protocol, which is what both
    /// directions of the BC codec key off after login.
    pub struct ServerCodec {
        ctx: BcContext,
    }

    impl Default for ServerCodec {
        fn default() -> Self {
            Self::new()
        }
    }

    impl ServerCodec {
        /// A fresh, unencrypted codec — the state every connection starts in.
        pub fn new() -> Self {
            Self {
                ctx: BcContext::new(Credentials {
                    username: String::new(),
                    password: None,
                }),
            }
        }

        /// Set the protocol the client was told to use in the login reply.
        pub fn set_encryption(&mut self, protocol: EncryptionProtocol) {
            self.ctx.set_encrypted(protocol);
        }

        /// The currently negotiated protocol.
        pub fn encryption(&self) -> EncryptionProtocol {
            self.ctx.get_encrypted().clone()
        }

        /// Decode one client request, or `None` if `buf` holds a partial one.
        ///
        /// Legacy messages (class `0x6514`) are framed here rather than by
        /// [`Bc::deserialize`]. The client-side parser only ever consumes the
        /// 64 bytes of username/password out of a legacy login body and never
        /// the 1772 bytes of padding that follow, and it will read 64 bytes
        /// past the end of a body-less `LoginUpgrade`. Neither matters to a
        /// client, which never receives a legacy message; both desynchronise
        /// a server on the very first packet.
        pub fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Bc>, Error> {
            if buf.len() < 20 {
                return Ok(None);
            }
            let class = u16::from_le_bytes([buf[18], buf[19]]);
            if class == 0x6514 {
                let msg_id = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
                let body_len = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]) as usize;
                if buf.len() < 20 + body_len {
                    return Ok(None);
                }
                let meta = BcMeta {
                    msg_id,
                    channel_id: buf[12],
                    stream_type: buf[13],
                    msg_num: u16::from_le_bytes([buf[14], buf[15]]),
                    response_code: u16::from_le_bytes([buf[16], buf[17]]),
                    class,
                };
                let body = buf[20..20 + body_len].to_vec();
                let legacy = match (msg_id, body_len) {
                    (MSG_ID_LOGIN, 0) => LegacyMsg::LoginUpgrade,
                    (MSG_ID_LOGIN, n) if n >= 64 => LegacyMsg::LoginMsg {
                        username: String::from_utf8_lossy(&body[..32]).into_owned(),
                        password: String::from_utf8_lossy(&body[32..64]).into_owned(),
                    },
                    _ => LegacyMsg::UnknownMsg,
                };
                use bytes::Buf;
                buf.advance(20 + body_len);
                return Ok(Some(Bc {
                    meta,
                    body: BcBody::LegacyMsg(legacy),
                }));
            }
            match Bc::deserialize(&self.ctx, buf) {
                Ok(bc) => Ok(Some(bc)),
                Err(Error::NomIncomplete(_)) => Ok(None),
                Err(e) => Err(e),
            }
        }

        /// Encode one camera reply.
        ///
        /// Mirrors the client encoder's special case: while AES is in force
        /// the login message (`msg_id` 1) is still only BCEncrypt, because
        /// the nonce needed for the AES key is what that exchange carries.
        pub fn encode(&self, bc: &Bc) -> Result<Vec<u8>, Error> {
            const BC_ENCRYPTED: EncryptionProtocol = EncryptionProtocol::BCEncrypt;
            let protocol = match self.ctx.get_encrypted() {
                EncryptionProtocol::Aes { .. } | EncryptionProtocol::FullAes { .. }
                    if bc.meta.msg_id == 1 =>
                {
                    &BC_ENCRYPTED
                }
                n => n,
            };
            Ok(bc.serialize(Vec::new(), protocol)?)
        }
    }

    /// Serialise a single BcMedia packet (the payload of a video message).
    pub fn encode_bcmedia(media: &BcMedia) -> Result<Vec<u8>, Error> {
        media.serialize(Vec::new())
    }

    /// Derive the AES key the client will derive, from the nonce the fake
    /// camera issued and the password it expects.
    pub fn make_aes_key(nonce: &str, password: &str) -> [u8; 16] {
        Credentials {
            username: String::new(),
            password: Some(password.to_string()),
        }
        .make_aeskey(nonce)
    }
}
