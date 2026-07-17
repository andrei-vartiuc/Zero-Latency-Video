//! Bibliothèque de streaming vidéo "Zéro Latence"
//!
//! Conçue pour une latence glass-to-glass < 20ms sur LAN, orientée Zero-Copy.
//! Architecture swap-ready : changer `WebRTCStreamer` pour `RawUdpStreamer` est
//! un changement de type, pas un refactoring de `main.rs`.

use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

#[cfg(target_os = "macos")]
pub mod mac_source;

#[cfg(target_os = "macos")]
pub mod mac_encoder;

pub mod webrtc_streamer;
pub mod raw_udp_streamer;

// ═══════════════════════════════════════════════════════════════════════════
// Erreurs
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Error, Debug)]
pub enum StreamError {
    #[error("Erreur de capture vidéo: {0}")]
    CaptureError(String),

    #[error("Erreur d'encodage vidéo: {0}")]
    EncodeError(String),

    #[error("Erreur réseau: {0}")]
    NetworkError(String),

    #[error("Erreur interne: {0}")]
    Internal(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, StreamError>;

// ═══════════════════════════════════════════════════════════════════════════
// Types du chemin Capture → Encodeur (conservés pour zero-copy IOSurface)
// ═══════════════════════════════════════════════════════════════════════════

/// Métadonnées liées à un frame vidéo brut (avant encodage).
#[derive(Debug, Clone, Copy)]
pub struct FrameMetadata {
    pub width: u32,
    pub height: u32,
    /// Timestamp de présentation en microsecondes (depuis CMSampleBuffer SCK).
    pub presentation_timestamp: u64,
    /// Toujours true à ce stade (les frames encodées ont leur propre flag).
    pub is_keyframe: bool,
}

/// Payload d'un frame brut — zero-copy de l'IOSurface vers VideoToolbox.
pub enum FramePayload {
    /// Surface matérielle partagée (IOSurface via CVPixelBuffer/CMSampleBuffer).
    /// Jamais copié en RAM CPU — passé directement à VTCompressionSessionEncodeFrame.
    HardwareBuffer(Arc<dyn std::any::Any + Send + Sync>),

    /// Données en mémoire CPU (fallback ou tests).
    MemoryBuffer(Bytes),
}

/// Frame vidéo brut pour le chemin Capture → Encodeur.
/// Garde le chemin IOSurface → VideoToolbox complètement zero-copy.
pub struct Frame {
    pub metadata: FrameMetadata,
    pub payload: FramePayload,
}

// ═══════════════════════════════════════════════════════════════════════════
// Types du chemin Encodeur → Réseau (hot path)
// ═══════════════════════════════════════════════════════════════════════════

/// Paquet encodé prêt pour le réseau — type central de la hot path.
///
/// `Bytes` utilise un Arc interne → clone en O(1), zéro copie mémoire.
/// Ce type transite entre le callback VideoToolbox et la tâche réseau async
/// via un channel `flume` lock-free.
#[derive(Debug, Clone)]
pub struct EncodedPacket {
    /// NAL units au format Annex-B (start codes 0x00 0x00 0x00 0x01).
    /// Directement envoyable dans une piste WebRTC ou sur UDP.
    pub data: Bytes,

    /// Timestamp de présentation en microsecondes (depuis CMSampleBuffer compressé).
    /// Utilisé pour les timestamps RTP et le calcul de durée inter-frame.
    pub pts_micros: u64,

    /// Durée inter-frame réelle en microsecondes.
    /// Calculée dans l'encodeur à partir de deux PTS successifs.
    /// 0 = inconnu (fallback sur 60fps nominal côté récepteur).
    pub duration_micros: u64,

    /// Vrai si ce paquet contient un IDR frame (I-Frame).
    /// Utile pour que les clients UDP ignorent les P-frames avant le premier I-frame.
    pub is_keyframe: bool,
}

impl EncodedPacket {
    /// Durée sous forme de `std::time::Duration`.
    /// Retourne 1/60s si duration_micros est 0 (60fps nominal).
    pub fn duration(&self) -> Duration {
        if self.duration_micros > 0 {
            Duration::from_micros(self.duration_micros)
        } else {
            Duration::from_nanos(1_000_000_000 / 60)
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Configuration du backend réseau
// ═══════════════════════════════════════════════════════════════════════════

/// Configuration découplée du backend — chaque implémentation lit ce dont elle a besoin.
///
/// Permet de changer de backend sans modifier `main.rs` :
/// ```rust
/// let cfg = StreamConfig::default();
/// // WebRTC : cfg.target_addr est ignoré (signalisation manuelle)
/// // UDP    : cfg.target_addr = "192.168.1.x:5000"
/// ```
#[derive(Debug, Clone)]
pub struct StreamConfig {
    /// WebRTC : ignoré (signalisation manuelle). UDP brut : "192.168.1.x:5000".
    pub target_addr: String,

    /// Bitrate maximum suggéré en bits/s. Le backend peut le transmettre au client
    /// ou l'ignorer si le codec gère lui-même le débit.
    pub max_bandwidth_bps: u32,

    /// Si true, drop les paquets en cas de congestion plutôt que les bufferiser.
    /// Doit **toujours être true** pour du PCVR / Remote Desktop interactif :
    /// on préfère un frame manqué à une latence accumulée.
    pub drop_on_congestion: bool,

    /// Canal optionnel pour demander des keyframes à la demande de manière asynchrone.
    pub request_keyframe: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            target_addr: String::new(),
            max_bandwidth_bps: 15_000_000,
            drop_on_congestion: true,
            request_keyframe: None,
        }
    }
}

/// Statistiques du backend réseau — pour monitoring et adaptation du bitrate.
#[derive(Debug, Default, Clone, Copy)]
pub struct StreamerStats {
    pub packets_sent: u64,
    pub packets_dropped: u64,
    /// RTT estimé en ms. `None` si non disponible (ex: WebRTC avant connexion).
    pub estimated_rtt_ms: Option<f32>,
}

// ═══════════════════════════════════════════════════════════════════════════
// Traits
// ═══════════════════════════════════════════════════════════════════════════

/// Trait de la source vidéo (ex: ScreenCaptureKit).
pub trait VideoSource: Send + Sync {
    fn start_capture(&mut self) -> impl std::future::Future<Output = Result<()>> + Send;
    fn next_frame(&mut self) -> impl std::future::Future<Output = Result<Frame>> + Send;
    fn stop_capture(&mut self) -> impl std::future::Future<Output = Result<()>> + Send;
}

/// Trait de l'encodeur vidéo (ex: VideoToolbox H.264/HEVC).
pub trait VideoEncoder: Send + Sync {
    fn configure(&mut self, width: u32, height: u32, bitrate: u32) -> Result<()>;
    /// Encode un frame brut. Le résultat est émis de manière asynchrone
    /// via le channel `flume` retourné par `new()`.
    fn encode(&mut self, frame: Frame) -> Result<()>;
}

/// Trait du backend de transport réseau.
///
/// # Design Swap-Ready
///
/// Toute implémentation de ce trait peut remplacer une autre sans modifier
/// `main.rs`. Le swap WebRTC → UDP brut est un changement de type :
///
/// ```rust
/// // Aujourd'hui :
/// let mut streamer = WebRTCStreamer::new();
/// // Demain (PCVR natif, < 5ms) :
/// let mut streamer = RawUdpStreamer::new();
/// ```
///
/// # Contrat de latence
///
/// `send_packet` **ne doit jamais bloquer** : si le réseau est en retard,
/// la frame doit être droppée côté émetteur (pas de queue qui grossit).
pub trait NetworkStreamer: Send + Sync {
    /// Identifiant du backend pour les logs et le monitoring.
    fn backend_name(&self) -> &'static str;

    /// Établit la connexion (signalisation WebRTC ou liaison socket UDP).
    fn connect(&mut self, cfg: StreamConfig)
        -> impl std::future::Future<Output = Result<()>> + Send;

    /// Envoie un paquet encodé sur le réseau.
    /// **Hot path** — appelée à 60fps. Doit être non-bloquante.
    fn send_packet(&mut self, packet: EncodedPacket)
        -> impl std::future::Future<Output = Result<()>> + Send;

    /// Retourne les statistiques actuelles (snapshots atomiques).
    fn stats(&self) -> StreamerStats;

    /// Ferme proprement la connexion et libère les ressources.
    fn disconnect(&mut self)
        -> impl std::future::Future<Output = Result<()>> + Send;
}
