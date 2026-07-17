use crate::{EncodedPacket, NetworkStreamer, Result, StreamConfig, StreamError, StreamerStats};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::HashMap;
use axum::{routing::{get, post}, Router, response::Html, Json, http::StatusCode};
use std::net::SocketAddr;
use webrtc::api::media_engine::{MediaEngine, MIME_TYPE_H264};
use webrtc::api::APIBuilder;
use webrtc::interceptor::registry::Registry;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecCapability;
use webrtc::track::track_local::track_local_static_sample::TrackLocalStaticSample;
use webrtc::track::track_local::TrackLocal;
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::media::Sample;

#[derive(serde::Serialize)]
struct OfferResponse {
    id: String,
    sdp: RTCSessionDescription,
}

pub struct SessionState {
    pub peer_connection: Arc<RTCPeerConnection>,
    pub video_track: Arc<TrackLocalStaticSample>,
    pub sender: tokio::sync::mpsc::Sender<EncodedPacket>,
}

/// Implémentation WebRTC du trait `NetworkStreamer`.
/// Supporte la reconnexion automatique et la diffusion multi-clients simultanée isolée.
pub struct WebRTCStreamer {
    api: Option<Arc<webrtc::api::API>>,
    peer_connections: Arc<std::sync::RwLock<HashMap<String, SessionState>>>,
    packets_sent: Arc<AtomicU64>,
    packets_dropped: Arc<AtomicU64>,
    request_keyframe: Option<Arc<std::sync::atomic::AtomicBool>>,
}

impl WebRTCStreamer {
    pub fn new() -> Self {
        Self {
            api: None,
            peer_connections: Arc::new(std::sync::RwLock::new(HashMap::new())),
            packets_sent: Arc::new(AtomicU64::new(0)),
            packets_dropped: Arc::new(AtomicU64::new(0)),
            request_keyframe: None,
        }
    }
}

async fn create_offer_session(
    api: Arc<webrtc::api::API>,
    peer_connections: Arc<std::sync::RwLock<HashMap<String, SessionState>>>,
    request_keyframe: Option<Arc<std::sync::atomic::AtomicBool>>,
) -> std::result::Result<OfferResponse, webrtc::Error> {
    let config = RTCConfiguration {
        ice_servers: vec![],
        ..Default::default()
    };

    let peer_connection = Arc::new(api.new_peer_connection(config).await?);

    // Create the session-specific video track (High Profile, level 3.1)
    let video_track = Arc::new(TrackLocalStaticSample::new(
        RTCRtpCodecCapability {
            mime_type: MIME_TYPE_H264.to_owned(),
            sdp_fmtp_line: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=64001f".to_owned(),
            ..Default::default()
        },
        "video".to_owned(),
        "webrtc-rs".to_owned(),
    ));

    // Ajout de la piste vidéo à cette connexion
    peer_connection
        .add_track(Arc::clone(&video_track) as Arc<dyn TrackLocal + Send + Sync>)
        .await?;

    // Création de l'offre SDP
    let offer = peer_connection.create_offer(None).await?;
    let mut gather_complete = peer_connection.gathering_complete_promise().await;
    peer_connection.set_local_description(offer).await?;
    
    // Attendre la fin du rassemblement des candidats ICE locaux (LAN uniquement, donc immédiat)
    let _ = gather_complete.recv().await;

    // ID de session unique basé sur le timestamp nanoseconde
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let session_id = format!("session-{}", (now & 0xFFFFFFFF) as u32);

    // Configuration du callback de changement d'état ICE pour nettoyer automatiquement
    // les connexions fermées ou échouées, et demander un keyframe à la connexion.
    let session_id_clone = session_id.clone();
    let pcs_clone = Arc::clone(&peer_connections);
    let rkf_clone = request_keyframe.clone();
    let pc_clone = Arc::clone(&peer_connection);
    peer_connection.on_peer_connection_state_change(Box::new(move |s: RTCPeerConnectionState| {
        let sid = session_id_clone.clone();
        let pcs = Arc::clone(&pcs_clone);
        let rkf = rkf_clone.clone();
        let pc = Arc::clone(&pc_clone);
        tracing::info!("[webrtc] Session {} state: {}", sid, s);
        Box::pin(async move {
            if s == RTCPeerConnectionState::Connected {
                if let Some(flag) = rkf {
                    flag.store(true, Ordering::Relaxed);
                    tracing::info!("[webrtc] Client connecté pour la session {}, demande de keyframe.", sid);
                }
            } else if s == RTCPeerConnectionState::Closed || s == RTCPeerConnectionState::Failed || s == RTCPeerConnectionState::Disconnected {
                let removed = {
                    let mut lock = pcs.write().unwrap();
                    lock.remove(&sid).is_some()
                };
                if removed {
                    tracing::info!("[webrtc] Session {} retirée de l'index", sid);
                    let _ = pc.close().await;
                }
            }
        })
    }));

    if let Some(local_desc) = peer_connection.local_description().await {
        // Create packet channel for client isolation (capacity = 2 frames to avoid accumulation)
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<EncodedPacket>(2);
        
        let video_track_clone = Arc::clone(&video_track);
        let session_id_clone2 = session_id.clone();
        tokio::spawn(async move {
            while let Some(packet) = receiver.recv().await {
                let duration = packet.duration();
                let sample = Sample {
                    data: packet.data,
                    duration,
                    ..Default::default()
                };
                // Timeout court (5ms) pour éviter tout blocage d'envoi
                let send_fut = video_track_clone.write_sample(&sample);
                if let Err(e) = tokio::time::timeout(std::time::Duration::from_millis(5), send_fut).await {
                    tracing::warn!("[webrtc] Timeout ou erreur d'écriture échantillon pour la session {}: {:?}", session_id_clone2, e);
                }
            }
            tracing::info!("[webrtc] Tâche de streaming terminée pour la session {}", session_id_clone2);
        });

        let mut lock = peer_connections.write().unwrap();
        lock.insert(session_id.clone(), SessionState {
            peer_connection: Arc::clone(&peer_connection),
            video_track,
            sender,
        });
        
        Ok(OfferResponse {
            id: session_id,
            sdp: local_desc,
        })
    } else {
        Err(webrtc::Error::ErrUnknownType)
    }
}

impl NetworkStreamer for WebRTCStreamer {
    fn backend_name(&self) -> &'static str {
        "webrtc"
    }

    async fn connect(&mut self, cfg: StreamConfig) -> Result<()> {
        self.request_keyframe = cfg.request_keyframe.clone();

        // 1. Configuration du MediaEngine pour H.264
        let mut m = MediaEngine::default();
        m.register_default_codecs()
            .map_err(|e| StreamError::NetworkError(e.to_string()))?;

        // OPT : Pas de pacer/NACK pour minimiser la latence sur réseau local
        let registry = Registry::new();

        let api = Arc::new(APIBuilder::new()
            .with_media_engine(m)
            .with_interceptor_registry(registry)
            .build());

        let peer_connections = Arc::clone(&self.peer_connections);
        let api_clone = Arc::clone(&api);
        let rkf_clone = self.request_keyframe.clone();

        // 3. Configuration des routes Axum avec injection d'ID de session
        let app = Router::new()
            .route("/", get(|| async {
                let html = include_str!("../client.html");
                Html(html)
            }))
            .route("/offer", get({
                let api_c = Arc::clone(&api_clone);
                let pcs_c = Arc::clone(&peer_connections);
                let rkf_c = rkf_clone.clone();
                move || {
                    let api_c = Arc::clone(&api_c);
                    let pcs_c = Arc::clone(&pcs_c);
                    let rkf_c = rkf_c.clone();
                    async move {
                        match create_offer_session(api_c, pcs_c, rkf_c).await {
                            Ok(resp) => (StatusCode::OK, Json(Some(resp))),
                            Err(e) => {
                                tracing::error!("Erreur création offre : {:?}", e);
                                (StatusCode::INTERNAL_SERVER_ERROR, Json(None))
                            }
                        }
                    }
                }
            }))
            .route("/answer/:id", post({
                let pcs_c = Arc::clone(&peer_connections);
                move |axum::extract::Path(session_id): axum::extract::Path<String>, Json(answer): Json<RTCSessionDescription>| async move {
                    let peer_connection = {
                        let lock = pcs_c.read().unwrap();
                        lock.get(&session_id).map(|session| Arc::clone(&session.peer_connection))
                    };
                    if let Some(pc) = peer_connection {
                        if let Err(e) = pc.set_remote_description(answer).await {
                            tracing::error!("Erreur set_remote_description pour {} : {:?}", session_id, e);
                            return StatusCode::BAD_REQUEST;
                        }
                        tracing::info!("[webrtc] Connexion négociée avec succès pour la session {}", session_id);
                        StatusCode::OK
                    } else {
                        tracing::warn!("[webrtc] Session {} non trouvée pour la réponse SDP", session_id);
                        StatusCode::NOT_FOUND
                    }
                }
            }));

        let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
        println!("\n=======================================================");
        println!("=== Serveur WebRTC lancé !                           ===");
        println!("=== Ouvrez http://localhost:3000 dans un navigateur ===");
        println!("=======================================================\n");

        let listener = tokio::net::TcpListener::bind(&addr).await
            .map_err(|e| StreamError::NetworkError(format!("Bind serveur WebRTC: {}", e)))?;
        
        tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, app).await {
                tracing::error!("Serveur Axum arrêté sur erreur : {:?}", e);
            }
        });

        self.api = Some(api);
        Ok(())
    }

    async fn send_packet(&mut self, packet: EncodedPacket) -> Result<()> {
        let lock = self.peer_connections.read().unwrap();
        for (sid, session) in lock.iter() {
            if let Err(e) = session.sender.try_send(packet.clone()) {
                match e {
                    tokio::sync::mpsc::error::TrySendError::Full(_) => {
                        self.packets_dropped.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!("[webrtc] Client congestionné (session {}). Frame droppé pour ce client.", sid);
                    }
                    tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                        // Nettoyé par le gestionnaire d'état
                    }
                }
            } else {
                self.packets_sent.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(())
    }

    fn stats(&self) -> StreamerStats {
        StreamerStats {
            packets_sent: self.packets_sent.load(Ordering::Relaxed),
            packets_dropped: self.packets_dropped.load(Ordering::Relaxed),
            estimated_rtt_ms: None,
        }
    }

    async fn disconnect(&mut self) -> Result<()> {
        let sessions = {
            let mut lock = self.peer_connections.write().unwrap();
            lock.drain().collect::<Vec<(String, SessionState)>>()
        };
        for (sid, session) in sessions {
            tracing::info!("[webrtc] Déconnexion propre de la session {}", sid);
            let _ = session.peer_connection.close().await;
        }
        self.api = None;
        Ok(())
    }
}
