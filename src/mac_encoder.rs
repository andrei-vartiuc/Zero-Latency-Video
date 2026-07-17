use crate::{EncodedPacket, Frame, FramePayload, Result, StreamError, VideoEncoder};
use bytes::Bytes;
use std::ffi::c_void;
use std::ptr;
use std::sync::atomic::Ordering;
use videotoolbox::ffi as vt;
use crate::mac_source::CMSampleBufferWrapper;

#[link(name = "CoreMedia", kind = "framework")]
extern "C" {
    fn CMVideoFormatDescriptionGetH264ParameterSetAtIndex(
        videoDesc: *const c_void,
        parameterSetIndex: usize,
        parameterSetPointerOut: *mut *const u8,
        parameterSetSizeOut: *mut usize,
        parameterSetCountOut: *mut usize,
        NALUnitHeaderLengthOut: *mut i32,
    ) -> i32;

    fn CMSampleBufferGetFormatDescription(sbuf: *const c_void) -> *const c_void;
    fn CMSampleBufferGetDataBuffer(sbuf: *const c_void) -> *const c_void;
    fn CMBlockBufferGetDataLength(theBuffer: *const c_void) -> usize;
    fn CMBlockBufferCopyDataBytes(
        theSourceBuffer: *const c_void,
        offsetToData: usize,
        dataLength: usize,
        destination: *mut c_void,
    ) -> i32;
    fn CMSampleBufferGetImageBuffer(sbuf: *const c_void) -> *const c_void;

    // PTS du sample COMPRESSÉ retourné par VideoToolbox
    #[link_name = "CMSampleBufferGetPresentationTimeStamp"]
    fn CMSampleBufferGetPresentationTimeStamp_encoder(sbuf: *const c_void) -> EncoderCMTime;
}

/// CMTime retourné par CoreMedia dans le callback VT.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct EncoderCMTime {
    value: i64,
    timescale: i32,
    flags: u32,
    epoch: i64,
}

impl EncoderCMTime {
    fn to_micros(self) -> u64 {
        if self.flags & 1 == 0 || self.timescale == 0 { return 0; }
        ((self.value as u128 * 1_000_000) / self.timescale as u128) as u64
    }
}

#[link(name = "CoreFoundation", kind = "framework")]
extern "C" {
    fn CFRelease(cf: *const c_void);
    static kCFBooleanTrue: *const c_void;
    static kCFBooleanFalse: *const c_void;
    static kCFTypeArrayCallBacks: c_void;
    static kCFTypeDictionaryKeyCallBacks: c_void;
    static kCFTypeDictionaryValueCallBacks: c_void;

    fn CFNumberCreate(
        allocator: *const c_void,
        theType: isize,
        valuePtr: *const c_void,
    ) -> *const c_void;

    fn CFArrayCreate(
        allocator: *const c_void,
        values: *const *const c_void,
        numValues: isize,
        callBacks: *const c_void,
    ) -> *const c_void;

    fn CFDictionaryCreate(
        allocator: *const c_void,
        keys: *const *const c_void,
        values: *const *const c_void,
        numValues: isize,
        keyCallBacks: *const c_void,
        valueCallBacks: *const c_void,
    ) -> *const c_void;
}

// ── Clés VT low-latency (macOS 12.3+) ────────────────────────────────────

const VT_KEY_LOW_LATENCY_RC: &std::ffi::CStr =
    c"kVTVideoEncoderSpecification_EnableLowLatencyRateControl";

const VT_KEY_SPEED_OVER_QUALITY: &std::ffi::CStr =
    c"kVTCompressionPropertyKey_PrioritizeEncodingSpeedOverQuality";

const VT_KEY_MAX_KF_DURATION: &std::ffi::CStr =
    c"kVTCompressionPropertyKey_MaxKeyFrameIntervalDuration";

/// Crée une CFString depuis une C string statique via l'API VT.
/// SÉCURITÉ: CFRelease() obligatoire après usage.
unsafe fn cf_string(s: &'static std::ffi::CStr) -> vt::CFStringRef {
    vt::CFStringCreateWithCString(ptr::null_mut(), s.as_ptr(), 0x0800_0100)
}

// ── Contexte du callback ──────────────────────────────────────────────────

/// État partagé passé au callback C par VideoToolbox.
///
/// `avcc_scratch` : buffer réutilisé frame-à-frame pour la lecture AVCC brute.
/// Élimine une allocation heap par frame (à 60fps = ~60 allocs/s évitées).
///
/// SÉCURITÉ : VT appelle ce callback séquentiellement quand
/// `AllowFrameReordering = false` (pas de B-frames). L'accès mutable est sûr.
struct EncoderContext {
    tx: flume::Sender<EncodedPacket>,
    /// PTS du frame précédent (en µs) — atomique pour éviter les data races.
    prev_pts_micros: std::sync::atomic::AtomicU64,
}

/// Callback C appelé par VideoToolbox après compression d'un frame.
unsafe extern "C" fn vt_encode_callback(
    output_callback_ref_con: *mut c_void,
    _source_frame_ref_con: *mut c_void,
    status: vt::OSStatus,
    _info_flags: vt::VTEncodeInfoFlags,
    sample_buffer: vt::CMSampleBufferRef,
) {
    if status != 0 || sample_buffer.is_null() {
        return;
    }

    // Accès sécurisé au contexte partagé (utilisation de référence partagée et structures synchronisées)
    let context = &*(output_callback_ref_con as *const EncoderContext);

    // ── Lecture AVCC depuis CMBlockBuffer (Zéro-copie intermédiaire) ──
    let block_buffer = CMSampleBufferGetDataBuffer(sample_buffer as *const c_void);
    if block_buffer.is_null() { return; }

    let data_len = CMBlockBufferGetDataLength(block_buffer);
    if data_len == 0 { return; }

    // 1. Allouer le vecteur final directement avec une capacité suffisante
    let mut annexb = Vec::with_capacity(data_len + 128);
    unsafe {
        annexb.set_len(data_len);
    }

    // 2. Copier directement de la mémoire GPU vers notre vecteur cible
    let copy_status = CMBlockBufferCopyDataBytes(
        block_buffer,
        0,
        data_len,
        annexb.as_mut_ptr() as *mut c_void,
    );
    if copy_status != 0 { return; }

    // ── Détection keyframe (scanne les NAL units en place) ──
    let mut is_keyframe = false;
    let mut offset = 0;
    while offset + 4 <= data_len {
        let nal_len = u32::from_be_bytes([
            annexb[offset],
            annexb[offset + 1],
            annexb[offset + 2],
            annexb[offset + 3],
        ]) as usize;
        offset += 4;
        if nal_len > data_len - offset { break; }
        let nal_type = annexb[offset] & 0x1F;
        if nal_type == 5 {
            is_keyframe = true;
            break;
        }
        offset += nal_len;
    }

    // ── Conversion AVCC → Annex-B en place ──
    let mut offset = 0;
    while offset + 4 <= data_len {
        let nal_len = u32::from_be_bytes([
            annexb[offset],
            annexb[offset + 1],
            annexb[offset + 2],
            annexb[offset + 3],
        ]) as usize;
        annexb[offset..offset + 4].copy_from_slice(&[0, 0, 0, 1]);
        if nal_len > data_len - offset - 4 { break; }
        offset += 4 + nal_len;
    }

    // ── Injection de SPS/PPS si nécessaire (uniquement sur keyframe) ──
    if is_keyframe {
        let format_desc = CMSampleBufferGetFormatDescription(sample_buffer as *const c_void);
        if !format_desc.is_null() {
            let mut count: usize = 0;
            let mut header_len: i32 = 0;

            let mut sps_ptr: *const u8 = ptr::null();
            let mut sps_size: usize = 0;
            let mut pps_ptr: *const u8 = ptr::null();
            let mut pps_size: usize = 0;

            let mut header = Vec::with_capacity(128);

            // SPS
            if CMVideoFormatDescriptionGetH264ParameterSetAtIndex(
                format_desc, 0, &mut sps_ptr, &mut sps_size, &mut count, &mut header_len,
            ) == 0 && !sps_ptr.is_null() && sps_size > 0 {
                header.extend_from_slice(&[0, 0, 0, 1]);
                header.extend_from_slice(std::slice::from_raw_parts(sps_ptr, sps_size));
            }

            // PPS
            if CMVideoFormatDescriptionGetH264ParameterSetAtIndex(
                format_desc, 1, &mut pps_ptr, &mut pps_size, &mut count, &mut header_len,
            ) == 0 && !pps_ptr.is_null() && pps_size > 0 {
                header.extend_from_slice(&[0, 0, 0, 1]);
                header.extend_from_slice(std::slice::from_raw_parts(pps_ptr, pps_size));
            }

            if !header.is_empty() {
                annexb.splice(0..0, header);
            }
        }
    }

    // ── PTS + durée inter-frame ───────────────────────────────────────────────
    let pts_micros = CMSampleBufferGetPresentationTimeStamp_encoder(
        sample_buffer as *const c_void
    ).to_micros();

    let prev_pts = context.prev_pts_micros.load(Ordering::Relaxed);
    let duration_micros = if prev_pts > 0 && pts_micros > prev_pts {
        pts_micros - prev_pts
    } else {
        0 // 0 = fallback 60fps côté récepteur
    };
    context.prev_pts_micros.store(pts_micros, Ordering::Relaxed);

    // ── Envoi via flume (lock-free, non-bloquant) ─────────────────────────────
    let packet = EncodedPacket {
        data: Bytes::from(annexb),
        pts_micros,
        duration_micros,
        is_keyframe,
    };
    let _ = context.tx.try_send(packet);
}

// ── MacVideoEncoder ───────────────────────────────────────────────────────

pub struct MacVideoEncoder {
    session: vt::VTCompressionSessionRef,
    context_ptr: *mut EncoderContext,
    tx: flume::Sender<EncodedPacket>,
    width: u32,
    height: u32,
    force_keyframe: bool,
}

unsafe impl Send for MacVideoEncoder {}
unsafe impl Sync for MacVideoEncoder {}

impl MacVideoEncoder {
    pub fn new(width: u32, height: u32) -> Result<(Self, flume::Receiver<EncodedPacket>)> {
        // Channel flume bounded(5)
        let (tx, rx) = flume::bounded::<EncodedPacket>(5);
        let (session, context_ptr) = Self::create_session(width, height, 15_000_000, tx.clone())?;

        Ok((Self {
            session,
            context_ptr,
            tx,
            width,
            height,
            force_keyframe: false,
        }, rx))
    }

    /// Crée et configure une session d'encodage VideoToolbox avec le contexte associé.
    fn create_session(
        width: u32,
        height: u32,
        bitrate: u32,
        tx: flume::Sender<EncodedPacket>,
    ) -> Result<(vt::VTCompressionSessionRef, *mut EncoderContext)> {
        let context = Box::new(EncoderContext {
            tx,
            prev_pts_micros: std::sync::atomic::AtomicU64::new(0),
        });
        let context_ptr = Box::into_raw(context);

        let mut session: vt::VTCompressionSessionRef = ptr::null_mut();

        unsafe {
            let key = cf_string(VT_KEY_LOW_LATENCY_RC);
            let val = kCFBooleanTrue;
            let keys = [key as *const c_void];
            let values = [val];
            let encoder_spec = CFDictionaryCreate(
                ptr::null(),
                keys.as_ptr(),
                values.as_ptr(),
                1,
                &kCFTypeDictionaryKeyCallBacks as *const _ as *const c_void,
                &kCFTypeDictionaryValueCallBacks as *const _ as *const c_void,
            );

            let status = vt::VTCompressionSessionCreate(
                ptr::null_mut(),
                width as i32,
                height as i32,
                vt::kCMVideoCodecType_H264,
                encoder_spec as *mut _,
                ptr::null_mut(),
                ptr::null_mut(),
                Some(vt_encode_callback),
                context_ptr as *mut c_void,
                &mut session,
            );

            CFRelease(encoder_spec);
            CFRelease(key as *const c_void);

            if status != 0 {
                let _ = Box::from_raw(context_ptr);
                return Err(StreamError::EncodeError(
                    format!("VTCompressionSessionCreate échoué: {}", status)
                ));
            }

            let true_val = kCFBooleanTrue;
            let false_val = kCFBooleanFalse;

            // 1. Mode temps réel
            vt::VTSessionSetProperty(
                session as *mut c_void,
                vt::kVTCompressionPropertyKey_RealTime,
                true_val as *mut c_void,
            );

            // 2. Pas de B-frames
            vt::VTSessionSetProperty(
                session as *mut c_void,
                vt::kVTCompressionPropertyKey_AllowFrameReordering,
                false_val as *mut c_void,
            );

            // 3. Profil H.264 High
            let s = vt::VTSessionSetProperty(
                session as *mut c_void,
                vt::kVTCompressionPropertyKey_ProfileLevel,
                vt::kVTProfileLevel_H264_High_AutoLevel as *mut c_void,
            );
            if s != 0 { tracing::warn!("ProfileLevel: {}", s); }

            // 4. Configurer le bitrate initial et le DataRateLimits
            let cf_bitrate = CFNumberCreate(ptr::null(), 3, &bitrate as *const u32 as *const c_void);
            vt::VTSessionSetProperty(session as *mut c_void, vt::kVTCompressionPropertyKey_AverageBitRate, cf_bitrate);

            let bytes_per_sec: i32 = (bitrate / 8) as i32;
            let one_sec: i32 = 1;
            let cf_bytes = CFNumberCreate(ptr::null(), 3, &bytes_per_sec as *const i32 as *const c_void);
            let cf_sec   = CFNumberCreate(ptr::null(), 3, &one_sec as *const i32 as *const c_void);
            let limit_arr = [cf_bytes, cf_sec];
            let cf_limits = CFArrayCreate(ptr::null(), limit_arr.as_ptr(), 2, &kCFTypeArrayCallBacks as *const _ as *const c_void);
            vt::VTSessionSetProperty(session as *mut c_void, vt::kVTCompressionPropertyKey_DataRateLimits, cf_limits);

            // 5. 60 FPS
            let fps: i32 = 60;
            let cf_fps = CFNumberCreate(ptr::null(), 3, &fps as *const i32 as *const c_void);
            vt::VTSessionSetProperty(session as *mut c_void, vt::kVTCompressionPropertyKey_ExpectedFrameRate, cf_fps);

            // 6. Keyframe max interval : 120 frames (2s)
            let max_kf: i32 = 120;
            let cf_kf = CFNumberCreate(ptr::null(), 3, &max_kf as *const i32 as *const c_void);
            vt::VTSessionSetProperty(session as *mut c_void, vt::kVTCompressionPropertyKey_MaxKeyFrameInterval, cf_kf);

            // 6b. MaxKeyFrameIntervalDuration : 2.0s
            let max_kf_dur: f64 = 2.0;
            let cf_kf_dur_key = cf_string(VT_KEY_MAX_KF_DURATION);
            let cf_kf_dur = CFNumberCreate(ptr::null(), 13, &max_kf_dur as *const f64 as *const c_void);
            let s = vt::VTSessionSetProperty(session as *mut c_void, cf_kf_dur_key as vt::CFStringRef, cf_kf_dur as *mut c_void);
            if s != 0 { tracing::warn!("MaxKeyFrameIntervalDuration non supporté: {}", s); }

            // 7. Qualité 0.8
            let quality: f32 = 0.8;
            let cf_quality = CFNumberCreate(ptr::null(), 5, &quality as *const f32 as *const c_void);
            vt::VTSessionSetProperty(session as *mut c_void, vt::kVTCompressionPropertyKey_Quality, cf_quality);

            // 8. PrioritizeEncodingSpeedOverQuality (macOS 13+)
            let cf_speed_key = cf_string(VT_KEY_SPEED_OVER_QUALITY);
            let s = vt::VTSessionSetProperty(session as *mut c_void, cf_speed_key as vt::CFStringRef, true_val as *mut c_void);
            if s != 0 { tracing::warn!("PrioritizeEncodingSpeedOverQuality non supporté: {}", s); }

            // 10. MaxFrameDelayCount = 0 (zéro buffer interne, encodage ultra-rapide)
            let cf_delay_key = cf_string(c"kVTCompressionPropertyKey_MaxFrameDelayCount");
            let max_delay: i32 = 0;
            let cf_delay = CFNumberCreate(ptr::null(), 3, &max_delay as *const i32 as *const c_void);
            let s = vt::VTSessionSetProperty(session as *mut c_void, cf_delay_key as vt::CFStringRef, cf_delay as *mut c_void);
            if s != 0 { tracing::warn!("MaxFrameDelayCount non supporté: {}", s); }

            // Nettoyage CF
            CFRelease(cf_bitrate);
            CFRelease(cf_bytes);
            CFRelease(cf_sec);
            CFRelease(cf_limits);
            CFRelease(cf_fps);
            CFRelease(cf_kf);
            CFRelease(cf_kf_dur_key as *const c_void);
            CFRelease(cf_kf_dur);
            CFRelease(cf_quality);
            CFRelease(cf_speed_key as *const c_void);
            CFRelease(cf_delay_key as *const c_void);
            CFRelease(cf_delay);

            vt::VTCompressionSessionPrepareToEncodeFrames(session);
        }

        tracing::info!(
            "VTCompressionSession initialisé avec succès : {}x{} H.264 @ 60fps {} bps",
            width, height, bitrate
        );

        Ok((session, context_ptr))
    }

    pub fn force_next_keyframe(&mut self) {
        self.force_keyframe = true;
    }
}

impl VideoEncoder for MacVideoEncoder {
    fn configure(&mut self, width: u32, height: u32, bitrate: u32) -> Result<()> {
        if self.width != width || self.height != height {
            tracing::info!(
                "Reconfiguration de la résolution de l'encodeur : {}x{} -> {}x{}",
                self.width, self.height, width, height
            );

            // 1. Libérer l'ancienne session proprement
            unsafe {
                if !self.session.is_null() {
                    vt::VTCompressionSessionCompleteFrames(
                        self.session,
                        vt::CMTime { value: 0, timescale: 0, flags: 0, epoch: 0 }
                    );
                    vt::VTCompressionSessionInvalidate(self.session);
                    CFRelease(self.session as *mut c_void);
                    self.session = ptr::null_mut();
                }
                if !self.context_ptr.is_null() {
                    let _ = Box::from_raw(self.context_ptr);
                    self.context_ptr = ptr::null_mut();
                }
            }

            // 2. Recréer la session avec les nouvelles dimensions
            let (session, context_ptr) = Self::create_session(width, height, bitrate, self.tx.clone())?;
            self.session = session;
            self.context_ptr = context_ptr;
            self.width = width;
            self.height = height;
        } else {
            // 3. Ajustement dynamique du bitrate (sans recréer la session)
            unsafe {
                let cf_bitrate = CFNumberCreate(ptr::null(), 3, &bitrate as *const u32 as *const c_void);
                let s = vt::VTSessionSetProperty(
                    self.session as *mut c_void,
                    vt::kVTCompressionPropertyKey_AverageBitRate,
                    cf_bitrate,
                );
                CFRelease(cf_bitrate);

                // Mettre également à jour les DataRateLimits
                let bytes_per_sec: i32 = (bitrate / 8) as i32;
                let one_sec: i32 = 1;
                let cf_bytes = CFNumberCreate(ptr::null(), 3, &bytes_per_sec as *const i32 as *const c_void);
                let cf_sec = CFNumberCreate(ptr::null(), 3, &one_sec as *const i32 as *const c_void);
                let limit_arr = [cf_bytes, cf_sec];
                let cf_limits = CFArrayCreate(ptr::null(), limit_arr.as_ptr(), 2, &kCFTypeArrayCallBacks as *const _ as *const c_void);
                let s_limits = vt::VTSessionSetProperty(
                    self.session as *mut c_void,
                    vt::kVTCompressionPropertyKey_DataRateLimits,
                    cf_limits,
                );
                CFRelease(cf_bytes);
                CFRelease(cf_sec);
                CFRelease(cf_limits);

                if s != 0 || s_limits != 0 {
                    tracing::warn!("Impossible d'ajuster dynamiquement le bitrate : average_br_status={}, limits_status={}", s, s_limits);
                } else {
                    tracing::info!("Bitrate et limites ajustés dynamiquement à : {} bps", bitrate);
                }
            }
        }
        Ok(())
    }

    fn encode(&mut self, frame: Frame) -> Result<()> {
        if let FramePayload::HardwareBuffer(arc_any) = frame.payload {
            if let Some(wrapper) = arc_any.downcast_ref::<CMSampleBufferWrapper>() {
                unsafe {
                    if self.session.is_null() {
                        return Err(StreamError::EncodeError("Session VideoToolbox non initialisée".into()));
                    }

                    let sys_ref = &*wrapper.0.sys_ref as *const _ as *const c_void;
                    let image_buffer = CMSampleBufferGetImageBuffer(sys_ref);
                    if image_buffer.is_null() {
                        return Err(StreamError::EncodeError("Pas d'ImageBuffer".into()));
                    }

                    // PTS en microsecondes → CMTime avec timescale=1_000_000
                    let pts_micros = frame.metadata.presentation_timestamp;
                    let cm_pts = vt::CMTime {
                        value: pts_micros as i64,
                        timescale: 1_000_000,
                        flags: 1, // kCMTimeFlags_Valid
                        epoch: 0,
                    };

                    let mut props = ptr::null();
                    if self.force_keyframe {
                        self.force_keyframe = false;
                        let key = vt::kVTEncodeFrameOptionKey_ForceKeyFrame;
                        let val = kCFBooleanTrue;
                        let keys = [key as *const c_void];
                        let vals = [val];
                        props = CFDictionaryCreate(
                            ptr::null(),
                            keys.as_ptr(),
                            vals.as_ptr(),
                            1,
                            &kCFTypeDictionaryKeyCallBacks as *const _ as *const c_void,
                            &kCFTypeDictionaryValueCallBacks as *const _ as *const c_void,
                        );
                    }

                    let status = vt::VTCompressionSessionEncodeFrame(
                        self.session,
                        image_buffer as *mut _,
                        cm_pts,
                        vt::CMTime { value: 0, timescale: 0, flags: 0, epoch: 0 },
                        props as *mut _,
                        ptr::null_mut(),
                        ptr::null_mut(),
                    );

                    if !props.is_null() {
                        CFRelease(props);
                    }

                    if status != 0 {
                        return Err(StreamError::EncodeError(
                            format!("VTCompressionSessionEncodeFrame: {}", status)
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

impl Drop for MacVideoEncoder {
    fn drop(&mut self) {
        unsafe {
            if !self.session.is_null() {
                vt::VTCompressionSessionCompleteFrames(
                    self.session,
                    vt::CMTime { value: 0, timescale: 0, flags: 0, epoch: 0 }
                );
                vt::VTCompressionSessionInvalidate(self.session);
                CFRelease(self.session as *mut c_void);
            }
            if !self.context_ptr.is_null() {
                let _ = Box::from_raw(self.context_ptr);
            }
        }
    }
}
