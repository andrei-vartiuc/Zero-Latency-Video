# zero-latency-video

A high-performance, ultra-low-latency video streaming library for macOS, written in Rust. 

Designed for real-time interactive streaming use cases (such as Remote Desktop, Cloud Gaming, and VR/AR streaming to Apple Vision Pro or Meta Quest) targeting a **glass-to-glass latency of < 20ms** on local networks.

## Features

*   **Zero-Copy Capture:** Leverages macOS native **ScreenCaptureKit** to capture the screen directly into shared hardware buffers (`IOSurface` / `CVPixelBuffer`), avoiding costly CPU-RAM copies.
*   **Hardware Acceleration:** Uses macOS **VideoToolbox** for hardware-accelerated H.264 video compression.
*   **Ultra-Low Latency Rate Control:** Configures `EnableLowLatencyRateControl` (macOS 12.3+) and disables frame reordering (no B-frames) for zero-buffer real-time encoding.
*   **Lock-free Pipeline:** Utilizes `flume` lock-free bounded channels to bridge synchronous C encoding callbacks and asynchronous Tokio network tasks seamlessly.
*   **Flexible Backends ("Swap-Ready"):** Includes standard **WebRTC** transport (multi-client isolation, automatic keyframe requests, client state detection) and a raw vector-I/O **UDP** transport.

---

## Prerequisites

*   **Operating System:** macOS 12.3 or higher.
*   **Compiler:** Xcode Command Line Tools (Clang & Swift compiler) and Rust 1.70+.
*   **Permissions:** Screen Recording permission must be granted to the executable or terminal running the project on macOS.

---

## Installation

Add this to your `Cargo.toml` dependencies:

```toml
[dependencies]
zero-latency-video = "0.1.0"
serde = { version = "1.0", features = ["derive"] }
```

---

## Quick Start

Here is a basic example setting up a ScreenCaptureKit capture stream encoded with VideoToolbox and broadcasted over WebRTC:

```rust
use zero_latency_video::mac_encoder::MacVideoEncoder;
use zero_latency_video::mac_source::SCScreenSource;
use zero_latency_video::webrtc_streamer::WebRTCStreamer;
use zero_latency_video::{NetworkStreamer, StreamConfig, VideoEncoder, VideoSource};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize screen capture source
    let mut source = SCScreenSource::new();
    source.start_capture().await?;

    // 2. Initialize VideoToolbox hardware encoder (returns encoder + packet receiver)
    let (mut encoder, encoded_rx) = MacVideoEncoder::new(1920, 1080)?;

    // 3. Set up network streamer (WebRTC backend)
    let mut streamer = WebRTCStreamer::new();
    let keyframe_request = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let cfg = StreamConfig {
        request_keyframe: Some(std::sync::Arc::clone(&keyframe_request)),
        ..Default::default()
    };
    streamer.connect(cfg).await?;

    // 4. Auto-detect display dimensions and configure encoder
    if let Ok(first_frame) = source.next_frame().await {
        let w = first_frame.metadata.width;
        let h = first_frame.metadata.height;
        encoder.configure(w, h, 15_000_000)?;
        let _ = encoder.encode(first_frame);
    }

    // 5. Spawn async network task
    tokio::spawn(async move {
        while let Ok(packet) = encoded_rx.recv_async().await {
            let _ = streamer.send_packet(packet).await;
        }
        let _ = streamer.disconnect().await;
    });

    // 6. Capture loop
    loop {
        if let Ok(frame) = source.next_frame().await {
            if keyframe_request.swap(false, std::sync::atomic::Ordering::Relaxed) {
                encoder.force_next_keyframe();
            }
            let _ = encoder.encode(frame);
        }
    }
}
```

---

## Swapping Backends

The library is designed to make swapping transport layers extremely simple. To swap from WebRTC to raw UDP streaming (ideal for custom VR clients):

```diff
- let mut streamer = WebRTCStreamer::new();
- streamer.connect(cfg).await?;
+ use zero_latency_video::raw_udp_streamer::RawUdpStreamer;
+ let mut streamer = RawUdpStreamer::new();
+ let udp_cfg = StreamConfig {
+     target_addr: "192.168.1.50:5000".to_string(), // Client IP and port
+     ..cfg
+ };
+ streamer.connect(udp_cfg).await?;
```

---

## License

Licensed under either of:

*   Apache License, Version 2.0 ([LICENSE-APACHE](http://www.apache.org/licenses/LICENSE-2.0))
*   MIT license ([LICENSE-MIT](http://opensource.org/licenses/MIT))

at your option.
