//! Backend UDP brut pour le streaming PCVR / Remote Desktop natif.
//!
//! # Objectif
//!
//! Ce module implémente `NetworkStreamer` avec un transport UDP minimal,
//! conçu pour les clients natifs (Swift/Metal sur Vision Pro, C++/Vulkan sur Quest).
//!
//! # Format de paquet
//!
//! Chaque datagramme UDP commence par un en-tête de 18 octets :
//!
//! ```text
//! ┌─────────────┬──────────────┬────────────┬───────────────┬───────────┐
//! │ magic [4]   │ pts_µs [8]   │ flags [2]  │ payload_len[2]│ frag [2]  │
//! ├─────────────┼──────────────┼────────────┼───────────────┼───────────┤
//! │ 0x5A4C5644  │ big-endian   │ bit0=KF    │ taille payload│ frag index│
//! └─────────────┴──────────────┴────────────┴───────────────┴───────────┘
//! ```
//!
//! Les NAL units > MTU (1400 octets) sont fragmentés en plusieurs datagrammes.
//!
//! # Swap
//!
//! Dans `main.rs`, remplacer :
//! ```rust
//! let mut streamer = WebRTCStreamer::new();
//! ```
//! par :
//! ```rust
//! let mut streamer = RawUdpStreamer::new();
//! let cfg = StreamConfig { target_addr: "192.168.1.x:5000".into(), ..Default::default() };
//! ```

use crate::{EncodedPacket, NetworkStreamer, Result, StreamConfig, StreamError, StreamerStats};
use std::net::UdpSocket;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Magic number pour identifier les paquets zero-latency dans Wireshark.
const PACKET_MAGIC: u32 = 0x5A4C_5644; // "ZLVD"

/// MTU UDP utilisable (1500 Ethernet - 28 IP/UDP headers - 18 notre header)
const UDP_PAYLOAD_MAX: usize = 1454;

/// Taille de l'en-tête de chaque datagramme.
const HEADER_SIZE: usize = 18;

/// Stub du streamer UDP brut.
///
/// **Statut : implémentation partielle** — la signalisation (échange de configs)
/// entre serveur et client natif n'est pas encore implémentée.
/// La fragmentation NAL et l'envoi UDP sont fonctionnels.
pub struct RawUdpStreamer {
    socket: Option<UdpSocket>,
    target_addr: String,
    packets_sent: Arc<AtomicU64>,
    packets_dropped: Arc<AtomicU64>,
}

impl RawUdpStreamer {
    pub fn new() -> Self {
        Self {
            socket: None,
            target_addr: String::new(),
            packets_sent: Arc::new(AtomicU64::new(0)),
            packets_dropped: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Construit l'en-tête d'un datagramme UDP.
    fn build_header(pts_micros: u64, is_keyframe: bool, payload_len: u16, frag_idx: u16) -> [u8; HEADER_SIZE] {
        let mut h = [0u8; HEADER_SIZE];
        h[0..4].copy_from_slice(&PACKET_MAGIC.to_be_bytes());
        h[4..12].copy_from_slice(&pts_micros.to_be_bytes());
        h[12..14].copy_from_slice(&if is_keyframe { 0x0001u16 } else { 0x0000u16 }.to_be_bytes());
        h[14..16].copy_from_slice(&payload_len.to_be_bytes());
        h[16..18].copy_from_slice(&frag_idx.to_be_bytes());
        h
    }
}

impl NetworkStreamer for RawUdpStreamer {
    fn backend_name(&self) -> &'static str {
        "raw-udp"
    }

    async fn connect(&mut self, cfg: StreamConfig) -> Result<()> {
        self.target_addr = cfg.target_addr.clone();

        // Liaison sur un port éphémère (l'OS choisit)
        let socket = UdpSocket::bind("0.0.0.0:0")
            .map_err(|e| StreamError::NetworkError(format!("Bind UDP: {}", e)))?;

        // SO_SNDBUF large pour éviter les drops noyau sous charge
        // (128KB = ~4 frames à 15Mbps/60fps)
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let fd = socket.as_raw_fd();
            let buf_size: libc::c_int = 128 * 1024;
            unsafe {
                libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_SNDBUF,
                    &buf_size as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
            }
        }

        socket.connect(&self.target_addr)
            .map_err(|e| StreamError::NetworkError(
                format!("Connect UDP vers {}: {}", self.target_addr, e)
            ))?;

        socket.set_nonblocking(true)
            .map_err(|e| StreamError::NetworkError(format!("NonBlocking: {}", e)))?;

        self.socket = Some(socket);
        tracing::info!("[raw-udp] Connecté vers {} (MTU payload: {}B)", self.target_addr, UDP_PAYLOAD_MAX);
        Ok(())
    }

    async fn send_packet(&mut self, packet: EncodedPacket) -> Result<()> {
        let socket = match &self.socket {
            Some(s) => s,
            None => return Ok(()), // pas encore connecté, on drop silencieusement
        };

        let data = &packet.data;
        let total = data.len();

        // Fragmentation si le paquet dépasse le MTU UDP
        let mut frag_idx: u16 = 0;
        let mut offset = 0;

        while offset < total {
            let chunk_end = (offset + UDP_PAYLOAD_MAX).min(total);
            let chunk = &data[offset..chunk_end];
            let payload_len = chunk.len() as u16;

            let header = Self::build_header(
                packet.pts_micros,
                packet.is_keyframe && frag_idx == 0, // keyframe flag sur le premier fragment
                payload_len,
                frag_idx,
            );

            // PERFORMANCE OPTIMIZATION: Utiliser les E/S vectorisées (vectored I/O) via libc::writev
            // pour envoyer l'en-tête et le fragment de charge utile en une seule opération système
            // sans recopier les données en mémoire (Zéro-Copie complet du payload).
            #[cfg(unix)]
            {
                use std::os::unix::io::AsRawFd;
                let fd = socket.as_raw_fd();
                let iov = [
                    libc::iovec {
                        iov_base: header.as_ptr() as *mut libc::c_void,
                        iov_len: header.len(),
                    },
                    libc::iovec {
                        iov_base: chunk.as_ptr() as *mut libc::c_void,
                        iov_len: chunk.len(),
                    },
                ];

                let ret = unsafe {
                    libc::writev(fd, iov.as_ptr(), 2)
                };

                if ret >= 0 {
                    self.packets_sent.fetch_add(1, Ordering::Relaxed);
                } else {
                    let err = std::io::Error::last_os_error();
                    if err.kind() == std::io::ErrorKind::WouldBlock {
                        // Buffer noyau plein → drop ce datagramme et abandon des fragments restants
                        self.packets_dropped.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!("[raw-udp] Buffer noyau plein, fragment {} droppé. Abandon des fragments restants de cette frame.", frag_idx);
                        break;
                    } else {
                        return Err(StreamError::NetworkError(format!("writev: {}", err)));
                    }
                }
            }

            offset = chunk_end;
            frag_idx = frag_idx.saturating_add(1);
        }

        Ok(())
    }

    fn stats(&self) -> StreamerStats {
        StreamerStats {
            packets_sent: self.packets_sent.load(Ordering::Relaxed),
            packets_dropped: self.packets_dropped.load(Ordering::Relaxed),
            estimated_rtt_ms: None, // TODO: implémenter un ping/pong sur le channel de contrôle
        }
    }

    async fn disconnect(&mut self) -> Result<()> {
        self.socket = None;
        tracing::info!("[raw-udp] Déconnecté");
        Ok(())
    }
}
