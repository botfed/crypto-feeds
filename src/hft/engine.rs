/// Single-threaded mio/epoll event loop for HFT feeds.
///
/// Polls all exchange WebSocket connections on one thread. When a frame
/// arrives, it's parsed zero-alloc and pushed to the ring buffer. The MM
/// can either embed its logic in the callback or poll `engine.latest()`.
///
/// Architecture:
/// ```text
/// mio::Poll (epoll/kqueue)
///   ├── Token(0) → Exchange A WsFramer → parse → RingBuffer
///   ├── Token(1) → Exchange B WsFramer → parse → RingBuffer
///   └── ...
/// ```
use crate::hft::ws_framer::{WsFramer, OP_BINARY, OP_CLOSE, OP_PING, OP_TEXT};
use crate::hft::{HftFeed, TickScratch};
use crate::market_data::DataSink;
use log::{debug, error, info, warn};
use mio::net::TcpStream;
use mio::{Events, Interest, Poll, Token};
use std::io::{Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Configuration for the HFT engine.
pub struct HftEngineConfig {
    /// How long to wait in epoll before checking heartbeats/shutdown.
    pub poll_timeout: Duration,
    /// Heartbeat interval — send ping if no activity for this long.
    pub heartbeat_interval: Duration,
    /// Reconnect if no message received for this long.
    pub message_timeout: Duration,
    /// Initial backoff on disconnect.
    pub initial_backoff: Duration,
    /// Maximum backoff on disconnect.
    pub max_backoff: Duration,
}

impl Default for HftEngineConfig {
    fn default() -> Self {
        Self {
            poll_timeout: Duration::from_millis(1),
            heartbeat_interval: Duration::from_secs(10),
            message_timeout: Duration::from_secs(90),
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
        }
    }
}

/// State for a single WebSocket connection.
struct Connection {
    framer: WsFramer,
    stream: Option<TlsStream>,
    state: ConnState,
    url: String,
    last_message: Instant,
    last_heartbeat: Instant,
    retry_count: u32,
    connected_at: Option<Instant>,
    /// Pre-allocated buffer for outgoing frames (subscribe, heartbeat, pong).
    write_buf: [u8; 4096],
}

enum ConnState {
    Disconnected,
    Connected,
}

/// Wrapper around a TCP stream with optional TLS (rustls).
///
/// `Tls` variant uses `rustls::StreamOwned` which handles TLS framing
/// transparently. After the TLS handshake (at connect time), the read/write
/// impls reuse rustls's internal buffers — no hot-path allocation.
enum TlsStream {
    Plain(TcpStream),
    Tls(rustls::StreamOwned<rustls::ClientConnection, TcpStream>),
}

impl TlsStream {
    fn as_source_mut(&mut self) -> &mut TcpStream {
        match self {
            TlsStream::Plain(s) => s,
            TlsStream::Tls(s) => s.get_mut(),
        }
    }
}

impl Read for TlsStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            TlsStream::Plain(s) => s.read(buf),
            TlsStream::Tls(s) => s.read(buf),
        }
    }
}

impl Write for TlsStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            TlsStream::Plain(s) => s.write(buf),
            TlsStream::Tls(s) => s.write(buf),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            TlsStream::Plain(s) => s.flush(),
            TlsStream::Tls(s) => s.flush(),
        }
    }
}

/// Single-threaded HFT feed engine.
///
/// Owns all connections and polls them via mio. Call `run()` to enter the
/// event loop (blocks the calling thread). Supports both `ws://` and `wss://`.
pub struct HftEngine<F: HftFeed, S: DataSink<F::Item>> {
    config: HftEngineConfig,
    feed: F,
    sink: S,
    connections: Vec<Connection>,
    poll: Poll,
    events: Events,
    scratch: TickScratch<F::Item>,
    /// Shared TLS client config — built once at startup with system CA roots.
    tls_config: std::sync::Arc<rustls::ClientConfig>,
}

impl<F: HftFeed, S: DataSink<F::Item>> HftEngine<F, S> {
    /// Create a new engine. All memory is pre-allocated here.
    pub fn new(feed: F, sink: S, config: HftEngineConfig) -> std::io::Result<Self> {
        let urls = feed.urls();
        let connections = urls
            .into_iter()
            .map(|url| Connection {
                framer: WsFramer::new(),
                stream: None,
                state: ConnState::Disconnected,
                url,
                last_message: Instant::now(),
                last_heartbeat: Instant::now(),
                retry_count: 0,
                connected_at: None,
                write_buf: [0u8; 4096],
            })
            .collect();

        // Build rustls ClientConfig with Mozilla CA roots.
        // This allocates once at startup — the config is shared across all connections.
        let root_store = rustls::RootCertStore::from_iter(
            webpki_roots::TLS_SERVER_ROOTS.iter().cloned(),
        );
        let tls_config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        Ok(Self {
            config,
            feed,
            sink,
            connections,
            poll: Poll::new()?,
            events: Events::with_capacity(64),
            scratch: TickScratch::new(),
            tls_config: std::sync::Arc::new(tls_config),
        })
    }

    /// Run the event loop. Blocks until `shutdown` is set to true.
    ///
    /// This is the main entry point. Call from a dedicated `std::thread`.
    pub fn run(&mut self, shutdown: &AtomicBool) {
        info!("HFT engine starting with {} connection(s)", self.connections.len());

        // Initial connect
        for i in 0..self.connections.len() {
            self.try_connect(i);
        }

        while !shutdown.load(Ordering::Relaxed) {
            // Poll for I/O events
            match self.poll.poll(&mut self.events, Some(self.config.poll_timeout)) {
                Ok(_) => {}
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    error!("mio poll error: {}", e);
                    std::thread::sleep(Duration::from_millis(100));
                    continue;
                }
            }

            // Process I/O events
            // Collect tokens into a stack array to avoid Vec allocation on hot path
            let mut tokens = [0usize; 64];
            let mut token_count = 0;
            for event in self.events.iter() {
                if token_count < 64 {
                    tokens[token_count] = event.token().0;
                    token_count += 1;
                }
            }
            for &idx in &tokens[..token_count] {
                if idx < self.connections.len() {
                    self.handle_readable(idx);
                }
            }

            self.maintain();
        }

        info!("HFT engine shutting down");
    }

    /// Single poll iteration — **zero-alloc hot path**.
    ///
    /// Polls for I/O events, reads data, parses frames, dispatches ticks.
    /// Does NOT run maintenance (heartbeats, reconnects) — those may allocate.
    ///
    /// The MM should call `maintain()` separately at lower frequency
    /// (e.g. every 100ms) outside the deny-alloc window:
    ///
    /// ```ignore
    /// loop {
    ///     alloc_guard::arm();
    ///     for _ in 0..100 {
    ///         engine.poll_once();  // zero-alloc
    ///         // MM logic ...
    ///     }
    ///     alloc_guard::disarm();
    ///     engine.maintain();  // may allocate (reconnect, heartbeat)
    /// }
    /// ```
    pub fn poll_once(&mut self) -> usize {
        let mut dispatched = 0;

        match self.poll.poll(&mut self.events, Some(self.config.poll_timeout)) {
            Ok(_) => {}
            Err(_) => return 0,
        }

        let mut tokens = [0usize; 64];
        let mut token_count = 0;
        for event in self.events.iter() {
            if token_count < 64 {
                tokens[token_count] = event.token().0;
                token_count += 1;
            }
        }
        for &idx in &tokens[..token_count] {
            if idx < self.connections.len() {
                dispatched += self.handle_readable(idx);
            }
        }

        dispatched
    }

    /// Periodic maintenance: heartbeats, message timeouts, reconnects.
    pub fn maintain(&mut self) {
        let now = Instant::now();
        for i in 0..self.connections.len() {
            match self.connections[i].state {
                ConnState::Connected => {
                    // Message timeout → reconnect
                    if now.duration_since(self.connections[i].last_message)
                        >= self.config.message_timeout
                    {
                        warn!(
                            "Connection {} timed out, reconnecting",
                            self.connections[i].url
                        );
                        self.disconnect(i);
                        self.try_connect(i);
                    }
                    // Heartbeat
                    else if now.duration_since(self.connections[i].last_heartbeat)
                        >= self.config.heartbeat_interval
                    {
                        self.send_heartbeat(i);
                    }
                }
                ConnState::Disconnected => {
                    // Backoff reconnect
                    let backoff = calculate_backoff(
                        self.connections[i].retry_count,
                        self.config.initial_backoff,
                        self.config.max_backoff,
                    );
                    if now.duration_since(self.connections[i].last_message) >= backoff {
                        self.try_connect(i);
                    }
                }
            }
        }
    }

    fn handle_readable(&mut self, idx: usize) -> usize {
        let mut dispatched = 0;

        // Read loop: drain ALL available data from the stream.
        // Critical for TLS: rustls may buffer decrypted plaintext internally.
        // mio/epoll won't fire READABLE for this buffered data since the
        // socket-level buffer may be empty. We must keep reading until WouldBlock
        // to avoid data getting stuck in rustls's buffer.
        loop {
            let conn = &mut self.connections[idx];
            let stream = match conn.stream.as_mut() {
                Some(s) => s,
                None => return dispatched,
            };

            match conn.framer.recv(stream) {
                Ok(0) => {
                    warn!("Connection {} closed (EOF)", conn.url);
                    self.disconnect(idx);
                    return dispatched;
                }
                Ok(_n) => {
                    conn.last_message = Instant::now();
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    break; // No more data available — return to epoll
                }
                Err(e) => {
                    error!("Connection {} read error: {}", conn.url, e);
                    self.disconnect(idx);
                    return dispatched;
                }
            }

            // Process all complete frames from this recv batch.
            dispatched += self.drain_frames(idx);

            // If framer buffer is getting full, break to avoid infinite loop
            // (shouldn't happen in practice — frames are small, buffer is 256KB)
            if self.connections[idx].framer.pending() > 200 * 1024 {
                break;
            }
        }

        dispatched
    }

    /// Drain all complete WS frames from the framer buffer and dispatch ticks.
    fn drain_frames(&mut self, idx: usize) -> usize {
        let mut dispatched = 0;
        let received = Instant::now();  // Cache once per batch, not per frame

        // Drain all complete frames.
        //
        // The frame payload borrows from the framer's buffer. We need to
        // process each frame (accessing self.feed, self.scratch, self.sink)
        // and potentially write back (pong). To satisfy the borrow checker,
        // we split the struct into disjoint borrows via raw pointers.
        //
        // SAFETY: We access disjoint fields of `self`:
        //   - connections[idx].framer: exclusive (for next_frame)
        //   - feed, scratch, sink: used in parse/dispatch (disjoint from connections)
        //   - connections[idx].write_buf, stream: for pong (only after frame is consumed)
        let framer_ptr = &mut self.connections[idx].framer as *mut WsFramer;
        loop {
            let frame = unsafe { (*framer_ptr).next_frame() };
            let frame = match frame {
                Some(f) => f,
                None => break,
            };

            match frame.opcode {
                OP_TEXT => {
                    self.scratch.clear();
                    self.feed
                        .parse_text(frame.payload, received, &mut self.scratch);
                    for tick in self.scratch.as_slice() {
                        self.sink.push(&tick.symbol_id, tick.item);
                        dispatched += 1;
                    }
                }
                OP_BINARY => {
                    self.scratch.clear();
                    self.feed
                        .parse_binary(frame.payload, received, &mut self.scratch);
                    for tick in self.scratch.as_slice() {
                        self.sink.push(&tick.symbol_id, tick.item);
                        dispatched += 1;
                    }
                }
                OP_PING => {
                    // Copy ping payload to stack (max 125 bytes per RFC 6455)
                    let mut ping_data = [0u8; 125];
                    let ping_len = frame.payload.len().min(125);
                    ping_data[..ping_len].copy_from_slice(&frame.payload[..ping_len]);
                    // frame is no longer borrowed — safe to access conn
                    drop(frame);

                    let conn = &mut self.connections[idx];
                    let len = WsFramer::encode_pong(&ping_data[..ping_len], &mut conn.write_buf);
                    if let Some(ref mut stream) = conn.stream {
                        let _ = stream.write_all(&conn.write_buf[..len]);
                    }
                    continue;
                }
                OP_CLOSE => {
                    drop(frame);
                    warn!("Connection {} received close frame", self.connections[idx].url);
                    self.disconnect(idx);
                    return dispatched;
                }
                _ => {} // pong, continuation — ignore
            }
        }

        dispatched
    }

    fn try_connect(&mut self, idx: usize) {
        // Parse URL to extract host:port (clone to avoid long borrow of conn)
        let url = self.connections[idx].url.clone();

        let (host, port, path) = match parse_ws_url(&url) {
            Some(v) => v,
            None => {
                error!("Invalid WS URL: {}", url);
                return;
            }
        };

        // Resolve and connect
        let addr = match format!("{}:{}", host, port).parse::<std::net::SocketAddr>() {
            Ok(a) => a,
            Err(_) => {
                // DNS resolution (allocates, but only at connect time — not hot path)
                match std::net::ToSocketAddrs::to_socket_addrs(&format!("{}:{}", host, port)) {
                    Ok(mut addrs) => match addrs.next() {
                        Some(a) => a,
                        None => {
                            error!("DNS resolution failed for {}", host);
                            self.connections[idx].retry_count += 1;
                            return;
                        }
                    },
                    Err(e) => {
                        error!("DNS error for {}: {}", host, e);
                        self.connections[idx].retry_count += 1;
                        return;
                    }
                }
            }
        };

        let is_tls = url.starts_with("wss://");

        // TCP connect (blocking — only at connect time, never hot path)
        let std_stream = match std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(5)) {
            Ok(s) => s,
            Err(e) => {
                error!("TCP connect failed to {}: {}", url, e);
                self.connections[idx].retry_count += 1;
                self.connections[idx].last_message = Instant::now();
                return;
            }
        };
        std_stream.set_nodelay(true).ok();

        // TLS handshake if wss:// (blocking — done before switching to non-blocking)
        let tls_stream = if is_tls {
            // Keep blocking for TLS handshake — rustls needs to complete
            // the full handshake before we switch to non-blocking mode.
            let server_name = match rustls_pki_types::ServerName::try_from(host.as_str()) {
                Ok(sn) => sn.to_owned(),
                Err(e) => {
                    error!("Invalid TLS server name '{}': {}", host, e);
                    self.connections[idx].retry_count += 1;
                    return;
                }
            };
            let tls_conn = match rustls::ClientConnection::new(
                std::sync::Arc::clone(&self.tls_config),
                server_name,
            ) {
                Ok(c) => c,
                Err(e) => {
                    error!("TLS client init failed for {}: {}", host, e);
                    self.connections[idx].retry_count += 1;
                    return;
                }
            };

            // StreamOwned drives the TLS handshake on first read/write.
            // We do a blocking handshake by writing empty data and reading
            // until the handshake completes.
            let mut tls_stream = rustls::StreamOwned::new(tls_conn, std_stream);

            // Drive the handshake to completion (blocking)
            let handshake_deadline = Instant::now() + Duration::from_secs(10);
            while tls_stream.conn.is_handshaking() {
                if Instant::now() > handshake_deadline {
                    error!("TLS handshake timeout for {}", url);
                    self.connections[idx].retry_count += 1;
                    return;
                }
                // Write pending TLS data to socket
                if tls_stream.conn.wants_write() {
                    match tls_stream.conn.write_tls(&mut tls_stream.sock) {
                        Ok(_) => {}
                        Err(e) => {
                            error!("TLS handshake write error: {}", e);
                            self.connections[idx].retry_count += 1;
                            return;
                        }
                    }
                }
                // Read TLS data from socket
                if tls_stream.conn.wants_read() {
                    match tls_stream.conn.read_tls(&mut tls_stream.sock) {
                        Ok(0) => {
                            error!("TLS handshake: peer closed");
                            self.connections[idx].retry_count += 1;
                            return;
                        }
                        Ok(_) => {}
                        Err(e) => {
                            error!("TLS handshake read error: {}", e);
                            self.connections[idx].retry_count += 1;
                            return;
                        }
                    }
                    if let Err(e) = tls_stream.conn.process_new_packets() {
                        error!("TLS handshake processing error: {}", e);
                        self.connections[idx].retry_count += 1;
                        return;
                    }
                }
            }

            info!("TLS handshake complete for {} ({})",
                host,
                tls_stream.conn.protocol_version()
                    .map(|v| format!("{:?}", v))
                    .unwrap_or_else(|| "unknown".to_string())
            );

            // Now switch underlying TCP to non-blocking for mio
            tls_stream.sock.set_nonblocking(true).ok();
            let mut mio_sock = TcpStream::from_std(tls_stream.sock);

            // Register with mio BEFORE wrapping in TlsStream (need &mut TcpStream)
            if let Err(e) = self.poll.registry().register(
                &mut mio_sock,
                Token(idx),
                Interest::READABLE,
            ) {
                error!("mio register failed: {}", e);
                self.connections[idx].retry_count += 1;
                return;
            }

            let tls_conn = tls_stream.conn;
            TlsStream::Tls(rustls::StreamOwned::new(tls_conn, mio_sock))
        } else {
            std_stream.set_nonblocking(true).ok();
            let mut mio_stream = TcpStream::from_std(std_stream);

            // Register with mio
            if let Err(e) = self.poll.registry().register(
                &mut mio_stream,
                Token(idx),
                Interest::READABLE,
            ) {
                error!("mio register failed: {}", e);
                self.connections[idx].retry_count += 1;
                return;
            }

            TlsStream::Plain(mio_stream)
        };

        // Store stream in conn immediately so disconnect() can deregister on failure.
        self.connections[idx].stream = Some(tls_stream);

        // Send WebSocket upgrade request
        let ws_key = generate_ws_key();
        let ws_key_str = std::str::from_utf8(&ws_key).unwrap_or("dGhlIHNhbXBsZSBub25jZQ==");
        let upgrade = format!(
            "GET {} HTTP/1.1\r\n\
             Host: {}\r\n\
             Upgrade: websocket\r\n\
             Connection: Upgrade\r\n\
             Sec-WebSocket-Key: {}\r\n\
             Sec-WebSocket-Version: 13\r\n\
             \r\n",
            path, host, ws_key_str
        );

        // Write the upgrade request
        if let Some(ref mut stream) = self.connections[idx].stream {
            if let Err(e) = stream.write_all(upgrade.as_bytes()) {
                error!("WS handshake write failed: {}", e);
                self.disconnect(idx);
                return;
            }
        }

        // Read handshake response (blocking-ish via poll)
        // For simplicity, we'll read the HTTP response in a tight loop with a timeout
        let mut response_buf = [0u8; 2048];
        let handshake_deadline = Instant::now() + Duration::from_secs(5);
        let mut response_len = 0;
        loop {
            if Instant::now() > handshake_deadline {
                error!("WS handshake timeout for {}", self.connections[idx].url);
                self.disconnect(idx);
                return;
            }
            let stream = match self.connections[idx].stream.as_mut() {
                Some(s) => s,
                None => return,
            };
            match stream.read(&mut response_buf[response_len..]) {
                Ok(0) => {
                    error!("WS handshake: connection closed");
                    self.disconnect(idx);
                    return;
                }
                Ok(n) => {
                    response_len += n;
                    // Check for end of HTTP response (\r\n\r\n)
                    if response_len >= 4 {
                        if response_buf[..response_len]
                            .windows(4)
                            .position(|w| w == b"\r\n\r\n")
                            .is_some()
                        {
                            break; // handshake complete
                        }
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(e) => {
                    error!("WS handshake read error: {}", e);
                    self.disconnect(idx);
                    return;
                }
            }
        }

        // Verify 101 Switching Protocols
        let response_str = std::str::from_utf8(&response_buf[..response_len]).unwrap_or("");
        if !response_str.contains("101") {
            error!("WS handshake rejected: {}", response_str.lines().next().unwrap_or("?"));
            self.disconnect(idx);
            return;
        }

        // Inject any remaining bytes (WS frames bundled with the HTTP response)
        // into the framer so they aren't lost.
        if let Some(hdr_end_pos) = response_buf[..response_len]
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
        {
            let after_hdr = hdr_end_pos + 4;
            if after_hdr < response_len {
                self.connections[idx]
                    .framer
                    .inject(&response_buf[after_hdr..response_len]);
            }
        }

        info!("Connected to {} (ws://)", self.connections[idx].url);

        // Notify feed of successful connection (e.g. reset dedup state)
        self.feed.on_connected(idx);

        let conn = &mut self.connections[idx];
        conn.state = ConnState::Connected;
        conn.last_message = Instant::now();
        conn.last_heartbeat = Instant::now();
        conn.retry_count = 0;
        conn.connected_at = Some(Instant::now());

        // Send subscription messages
        for sub_msg in self.feed.subscribe_messages() {
            let conn = &mut self.connections[idx];
            let len = WsFramer::encode_text(sub_msg.as_bytes(), &mut conn.write_buf);
            if let Some(ref mut stream) = conn.stream {
                if let Err(e) = stream.write_all(&conn.write_buf[..len]) {
                    error!("Failed to send subscription: {}", e);
                }
            }
        }
    }

    fn disconnect(&mut self, idx: usize) {
        let conn = &mut self.connections[idx];
        if let Some(ref mut stream) = conn.stream {
            let _ = self
                .poll
                .registry()
                .deregister(stream.as_source_mut());
        }
        let was_long_lived = conn
            .connected_at
            .map(|t| t.elapsed() > Duration::from_secs(60))
            .unwrap_or(false);
        conn.stream = None;
        conn.state = ConnState::Disconnected;
        conn.connected_at = None;
        conn.last_message = Instant::now(); // for backoff timing
        if was_long_lived {
            conn.retry_count = 0;
        } else {
            conn.retry_count += 1;
        }
    }

    fn send_heartbeat(&mut self, idx: usize) {
        let conn = &mut self.connections[idx];
        let payload = self.feed.heartbeat_payload();

        let len = if let Some(hb) = payload {
            WsFramer::encode_text(hb, &mut conn.write_buf)
        } else {
            WsFramer::encode_ping(b"", &mut conn.write_buf)
        };

        if let Some(ref mut stream) = conn.stream {
            match stream.write_all(&conn.write_buf[..len]) {
                Ok(_) => {
                    conn.last_heartbeat = Instant::now();
                }
                Err(e) => {
                    debug!("Heartbeat write error on {}: {}", conn.url, e);
                }
            }
        }
    }
}

/// Generate a unique-per-connection WebSocket key (base64-encoded 16-byte nonce).
/// Not cryptographic — just unique enough to avoid proxy confusion.
fn generate_ws_key() -> [u8; 24] {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let raw = nanos.to_le_bytes();
    const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut input = [0u8; 16];
    input[..8].copy_from_slice(&raw);
    input[8..16].copy_from_slice(
        &(nanos
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407))
        .to_le_bytes(),
    );

    let mut out = [b'='; 24];
    for i in 0..(16 / 3) {
        let j = i * 3;
        let k = i * 4;
        let n = ((input[j] as u32) << 16) | ((input[j + 1] as u32) << 8) | (input[j + 2] as u32);
        out[k] = B64[((n >> 18) & 0x3F) as usize];
        out[k + 1] = B64[((n >> 12) & 0x3F) as usize];
        out[k + 2] = B64[((n >> 6) & 0x3F) as usize];
        out[k + 3] = B64[(n & 0x3F) as usize];
    }
    // Last byte (16 % 3 = 1): 1 remaining byte
    let n = (input[15] as u32) << 16;
    out[20] = B64[((n >> 18) & 0x3F) as usize];
    out[21] = B64[((n >> 12) & 0x3F) as usize];
    out[22] = b'=';
    out[23] = b'=';
    out
}

fn calculate_backoff(retry_count: u32, initial: Duration, max: Duration) -> Duration {
    let exponential = initial * 2_u32.saturating_pow(retry_count.min(10));
    std::cmp::min(exponential, max)
}

/// Parse a ws:// or wss:// URL into (host, port, path).
fn parse_ws_url(url: &str) -> Option<(String, u16, String)> {
    let rest = if let Some(r) = url.strip_prefix("ws://") {
        r
    } else if let Some(r) = url.strip_prefix("wss://") {
        r
    } else {
        return None;
    };

    let (host_port, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };

    let (host, port) = match host_port.find(':') {
        Some(i) => (
            &host_port[..i],
            host_port[i + 1..].parse::<u16>().unwrap_or(80),
        ),
        None => {
            let default_port = if url.starts_with("wss://") { 443 } else { 80 };
            (host_port, default_port)
        }
    };

    Some((host.to_string(), port, path.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ws_url() {
        let (host, port, path) = parse_ws_url("ws://localhost:8080/feed").unwrap();
        assert_eq!(host, "localhost");
        assert_eq!(port, 8080);
        assert_eq!(path, "/feed");

        let (host, port, path) = parse_ws_url("ws://example.com/ws/v1").unwrap();
        assert_eq!(host, "example.com");
        assert_eq!(port, 80);
        assert_eq!(path, "/ws/v1");

        let (host, port, path) =
            parse_ws_url("ws://stream.binance.com:9443/stream?streams=btcusdt@bookTicker")
                .unwrap();
        assert_eq!(host, "stream.binance.com");
        assert_eq!(port, 9443);
        assert_eq!(path, "/stream?streams=btcusdt@bookTicker");
    }

    #[test]
    fn test_calculate_backoff() {
        let initial = Duration::from_secs(1);
        let max = Duration::from_secs(60);

        assert_eq!(calculate_backoff(0, initial, max), Duration::from_secs(1));
        assert_eq!(calculate_backoff(1, initial, max), Duration::from_secs(2));
        assert_eq!(calculate_backoff(2, initial, max), Duration::from_secs(4));
        assert_eq!(calculate_backoff(6, initial, max), Duration::from_secs(60)); // capped
        assert_eq!(calculate_backoff(20, initial, max), Duration::from_secs(60)); // still capped
    }
}
