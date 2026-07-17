use crate::{Frame, FrameMetadata, FramePayload, Result, StreamError, VideoSource};
use screencapturekit::{
    cm_sample_buffer::CMSampleBuffer,
    sc_content_filter::{InitParams, SCContentFilter},
    sc_error_handler::StreamErrorHandler,
    sc_output_handler::{SCStreamOutputType, StreamOutput},
    sc_shareable_content::SCShareableContent,
    sc_stream::SCStream,
    sc_stream_configuration::SCStreamConfiguration,
};
use std::sync::Arc;
use crossbeam_queue::ArrayQueue;
use tokio::sync::Notify;

#[link(name = "CoreVideo", kind = "framework")]
extern "C" {
    fn CVPixelBufferGetWidth(pixelBuffer: *const std::ffi::c_void) -> usize;
    fn CVPixelBufferGetHeight(pixelBuffer: *const std::ffi::c_void) -> usize;
}

// Le PTS natif est maintenant extrait via le wrapper de screencapturekit-sys.

/// Enveloppe thread-safe pour CMSampleBuffer (CoreMedia gère la sûreté de concurrence interne).
/// Ceci est nécessaire car CMSampleBuffer contient des pointeurs natifs non déclarés Send/Sync par défaut.
pub struct CMSampleBufferWrapper(pub CMSampleBuffer);
unsafe impl Send for CMSampleBufferWrapper {}
unsafe impl Sync for CMSampleBufferWrapper {}

/// Gestionnaire (Handler) appelé par ScreenCaptureKit lorsqu'un nouveau frame vidéo est capturé.
struct ScreenOutputHandler {
    queue: Arc<ArrayQueue<Frame>>,
    notify: Arc<Notify>,
}

impl StreamOutput for ScreenOutputHandler {
    fn did_output_sample_buffer(&self, sample: CMSampleBuffer, of_type: SCStreamOutputType) {
        if matches!(of_type, SCStreamOutputType::Screen) {
            // Extraction de la résolution exacte via FFI sur le CVImageBuffer sous-jacent
            let (width, height) = if let Some(ref image_buf) = sample.image_buf_ref {
                let raw_ptr = &**image_buf as *const _ as *const std::ffi::c_void;
                unsafe {
                    (
                        CVPixelBufferGetWidth(raw_ptr) as u32,
                        CVPixelBufferGetHeight(raw_ptr) as u32,
                    )
                }
            } else {
                // Les frames sans image_buf sont des statuts (pas de changement à l'écran), on les ignore.
                return;
            };

            // OPT-2 : Extraction du PTS natif (en microsecondes) via le wrapper de la bibliothèque.
            // Crucial pour que VideoToolbox ordonne correctement les frames
            // et pour que le jitter buffer WebRTC côté décodeur fonctionne.
            let pts = sample.sys_ref.get_presentation_timestamp();
            let pts_micros = if pts.flags & 1 == 0 || pts.timescale == 0 || pts.value < 0 {
                0
            } else {
                ((pts.value as u128 * 1_000_000) / pts.timescale as u128) as u64
            };

            let frame = Frame {
                metadata: FrameMetadata {
                    width,
                    height,
                    presentation_timestamp: pts_micros,
                    is_keyframe: true,
                },
                // OBJECTIF ZERO-COPY:
                // Le CMSampleBuffer original est préservé, aucun pixel n'est copié vers le CPU.
                // Il restera sur la mémoire GPU/partagée (IOSurface).
                payload: FramePayload::HardwareBuffer(Arc::new(CMSampleBufferWrapper(sample))),
            };

            // OPT-1 : ArrayQueue avec force_push garantit que l'on garde toujours
            // la dernière frame. L'ancienne est automatiquement écrasée si la file est pleine.
            self.queue.force_push(frame);
            self.notify.notify_waiters();
        }
    }
}

/// Gestionnaire d'erreurs pour le flux SCStream.
struct ScreenErrorHandler {
    error_flag: Arc<std::sync::Mutex<Option<String>>>,
    notify: Arc<Notify>,
}

impl StreamErrorHandler for ScreenErrorHandler {
    fn on_error(&self) {
        let msg = "Erreur matérielle ScreenCaptureKit détectée".to_string();
        tracing::error!("{}", msg);
        if let Ok(mut lock) = self.error_flag.lock() {
            *lock = Some(msg);
        }
        self.notify.notify_waiters();
    }
}

/// Implémentation macOS de notre VideoSource utilisant ScreenCaptureKit.
pub struct SCScreenSource {
    stream: Option<SCStream>,
    queue: Option<Arc<ArrayQueue<Frame>>>,
    notify: Option<Arc<Notify>>,
    error_flag: Option<Arc<std::sync::Mutex<Option<String>>>>,
}

impl SCScreenSource {
    pub fn new() -> Self {
        Self {
            stream: None,
            queue: None,
            notify: None,
            error_flag: None,
        }
    }
}

impl VideoSource for SCScreenSource {
    async fn start_capture(&mut self) -> Result<()> {
        // Obtenir le contenu partageable (fenêtres, écrans, apps) de manière sécurisée (gestion des permissions)
        let mut content = SCShareableContent::try_current()
            .map_err(|e| StreamError::CaptureError(format!("Erreur lors de l'accès aux écrans partagés (vérifiez les permissions de capture d'écran sur macOS) : {}", e)))?;
        
        // Sélection de l'écran principal (le premier retourné)
        let display = content
            .displays
            .pop()
            .ok_or_else(|| StreamError::CaptureError("Aucun écran détecté".into()))?;

        // Créer un filtre pour ne capturer que cet écran
        let filter = SCContentFilter::new(InitParams::Display(display.clone()));
        
        // Configurer la capture matérielle
        let config = SCStreamConfiguration {
            width: display.width as u32,
            height: display.height as u32,
            shows_cursor: true,
            // PERFORMANCE: Utiliser YUV (NV12) en natif pour éviter une conversion de couleur coûteuse BGRA -> NV12 dans l'encodeur
            pixel_format: screencapturekit::sc_stream_configuration::PixelFormat::YCbCr420v,
            ..Default::default()
        };

        // OPT-1 : File d'attente à 1 slot avec écrasement (force_push).
        let queue = Arc::new(ArrayQueue::new(1));
        let notify = Arc::new(Notify::new());
        let error_flag = Arc::new(std::sync::Mutex::new(None));

        // Initialisation de la session SCStream
        let mut stream = SCStream::new(filter, config, ScreenErrorHandler {
            error_flag: Arc::clone(&error_flag),
            notify: Arc::clone(&notify),
        });
        
        // Ajouter notre Output Handler asynchrone
        stream.add_output(ScreenOutputHandler { queue: queue.clone(), notify: notify.clone() }, SCStreamOutputType::Screen);
        
        // Démarrer la capture
        stream.start_capture().map_err(|e| StreamError::CaptureError(e))?;

        self.stream = Some(stream);
        self.queue = Some(queue);
        self.notify = Some(notify);
        self.error_flag = Some(error_flag);

        Ok(())
    }

    async fn next_frame(&mut self) -> Result<Frame> {
        let queue = self.queue.as_ref().ok_or_else(|| StreamError::CaptureError("Le flux de capture n'est pas démarré".into()))?;
        let notify = self.notify.as_ref().ok_or_else(|| StreamError::CaptureError("Le flux de capture n'est pas démarré".into()))?;
        
        loop {
            // Vérifier s'il y a eu une erreur de capture
            if let Some(ref err_flag) = self.error_flag {
                if let Ok(lock) = err_flag.lock() {
                    if let Some(ref err_msg) = *lock {
                        return Err(StreamError::CaptureError(err_msg.clone()));
                    }
                }
            }

            let listener = notify.notified();
            if let Some(frame) = queue.pop() {
                return Ok(frame);
            }
            listener.await;
        }
    }

    async fn stop_capture(&mut self) -> Result<()> {
        if let Some(stream) = self.stream.take() {
            stream.stop_capture().map_err(|e| StreamError::CaptureError(e))?;
        }
        self.queue = None;
        self.notify = None;
        self.error_flag = None;
        Ok(())
    }
}
