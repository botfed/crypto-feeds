/// Zero-copy WebSocket frame parser.
///
/// Owns a single pre-allocated read buffer. Frames are parsed in-place and
/// returned as `&[u8]` slices pointing directly into the buffer — no heap
/// allocation on the hot path.
///
/// Usage:
///   1. Call `recv()` to read bytes from the socket into the buffer.
///   2. Call `next_frame()` in a loop to drain complete frames.
///   3. Payload slices borrow `&self` so they are invalidated by the next `recv()`.

/// WS opcodes (RFC 6455 §5.2)
pub const OP_CONTINUATION: u8 = 0x0;
pub const OP_TEXT: u8 = 0x1;
pub const OP_BINARY: u8 = 0x2;
pub const OP_CLOSE: u8 = 0x8;
pub const OP_PING: u8 = 0x9;
pub const OP_PONG: u8 = 0xA;

/// Pre-allocated read buffer size. 256KB covers any realistic single WS frame
/// from an exchange (typical bookTicker is 200-500 bytes).
const READ_BUF_CAPACITY: usize = 256 * 1024;

/// A parsed WebSocket frame. Payload borrows the framer's buffer.
pub struct WsFrame<'a> {
    pub opcode: u8,
    pub payload: &'a [u8],
}

pub struct WsFramer {
    buf: Box<[u8; READ_BUF_CAPACITY]>,
    /// `[0..consumed)` = already parsed, `[consumed..filled)` = pending data
    consumed: usize,
    filled: usize,
}

impl WsFramer {
    pub fn new() -> Self {
        Self {
            buf: Box::new([0u8; READ_BUF_CAPACITY]),
            consumed: 0,
            filled: 0,
        }
    }

    /// Read bytes from `stream` into the internal buffer. Returns bytes read.
    /// Compacts the buffer when consumed passes the halfway mark.
    #[inline]
    pub fn recv(&mut self, stream: &mut impl std::io::Read) -> std::io::Result<usize> {
        self.maybe_compact();
        if self.filled >= READ_BUF_CAPACITY {
            return Err(std::io::Error::new(
                std::io::ErrorKind::OutOfMemory,
                "ws_framer buffer full",
            ));
        }
        let n = stream.read(&mut self.buf[self.filled..])?;
        self.filled += n;
        Ok(n)
    }

    /// Inject raw bytes directly (for testing / replay). No I/O.
    pub fn inject(&mut self, data: &[u8]) {
        self.maybe_compact();
        let space = READ_BUF_CAPACITY - self.filled;
        let n = data.len().min(space);
        self.buf[self.filled..self.filled + n].copy_from_slice(&data[..n]);
        self.filled += n;
    }

    /// Reset consumed/filled pointers (for replay / benchmark loops).
    pub fn reset(&mut self) {
        self.consumed = 0;
        self.filled = 0;
    }

    /// Number of unprocessed bytes in the buffer.
    #[inline]
    pub fn pending(&self) -> usize {
        self.filled - self.consumed
    }

    /// Try to extract the next complete WebSocket frame.
    ///
    /// Returns `None` if there isn't enough data for a complete frame.
    /// The returned `WsFrame` borrows `self` — the slice points directly
    /// into `self.buf`. This borrow is released when the frame is dropped,
    /// allowing the next `recv()` or `next_frame()` call.
    #[inline]
    pub fn next_frame(&mut self) -> Option<WsFrame<'_>> {
        let data = &self.buf[self.consumed..self.filled];
        let data_len = data.len();
        if data_len < 2 {
            return None;
        }

        let opcode = data[0] & 0x0F;
        let masked = data[1] & 0x80 != 0;
        let len7 = (data[1] & 0x7F) as usize;

        let (payload_len, mut hdr_len) = if len7 <= 125 {
            (len7, 2usize)
        } else if len7 == 126 {
            if data_len < 4 {
                return None;
            }
            let pl = u16::from_be_bytes([data[2], data[3]]) as usize;
            (pl, 4usize)
        } else {
            // len7 == 127
            if data_len < 10 {
                return None;
            }
            let pl = u64::from_be_bytes([
                data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9],
            ]) as usize;
            (pl, 10usize)
        };

        if masked {
            hdr_len += 4; // 4-byte masking key
        }

        let total = hdr_len + payload_len;
        if data_len < total {
            return None; // incomplete frame
        }

        if masked {
            // Server-to-client frames should not be masked per RFC 6455,
            // but handle it defensively: unmask in place.
            let mask_start = hdr_len - 4;
            let mask = [
                data[mask_start],
                data[mask_start + 1],
                data[mask_start + 2],
                data[mask_start + 3],
            ];
            // We need mutable access to unmask. Safe because we own the buffer
            // and the borrow checker sees &mut self.
            let payload_slice =
                &mut self.buf[self.consumed + hdr_len..self.consumed + hdr_len + payload_len];
            for (i, byte) in payload_slice.iter_mut().enumerate() {
                *byte ^= mask[i & 3];
            }
        }

        let payload = &self.buf[self.consumed + hdr_len..self.consumed + hdr_len + payload_len];
        self.consumed += total;

        Some(WsFrame { opcode, payload })
    }

    /// Build a client-masked text frame into `out`. Returns bytes written.
    /// Used for sending subscribe messages and heartbeats.
    /// The mask is fixed (not random) — fine for non-security contexts.
    pub fn encode_text(text: &[u8], out: &mut [u8]) -> usize {
        Self::encode_frame(OP_TEXT, text, out)
    }

    /// Build a client-masked ping frame into `out`. Returns bytes written.
    pub fn encode_ping(payload: &[u8], out: &mut [u8]) -> usize {
        Self::encode_frame(OP_PING, payload, out)
    }

    /// Build a client-masked pong frame into `out`. Returns bytes written.
    pub fn encode_pong(payload: &[u8], out: &mut [u8]) -> usize {
        Self::encode_frame(OP_PONG, payload, out)
    }

    fn encode_frame(opcode: u8, payload: &[u8], out: &mut [u8]) -> usize {
        let plen = payload.len();
        // Bounds check: header (2-10 bytes) + mask (4 bytes) + payload
        let max_header = if plen <= 125 { 2 } else if plen <= 65535 { 4 } else { 10 };
        let total_size = max_header + 4 + plen; // +4 for mask
        if total_size > out.len() {
            return 0;
        }
        let mut pos = 0;

        // FIN + opcode
        out[pos] = 0x80 | opcode;
        pos += 1;

        // Mask bit + length (client frames MUST be masked per RFC 6455)
        if plen <= 125 {
            out[pos] = 0x80 | plen as u8;
            pos += 1;
        } else if plen <= 65535 {
            out[pos] = 0x80 | 126;
            pos += 1;
            out[pos..pos + 2].copy_from_slice(&(plen as u16).to_be_bytes());
            pos += 2;
        } else {
            out[pos] = 0x80 | 127;
            pos += 1;
            out[pos..pos + 8].copy_from_slice(&(plen as u64).to_be_bytes());
            pos += 8;
        }

        // Mask key (varies per frame based on payload — not for security)
        let mask = [
            (plen as u8).wrapping_add(0x37),
            (plen as u8).rotate_left(3).wrapping_add(0x0A),
            payload.first().copied().unwrap_or(0x52),
            payload.get(1).copied().unwrap_or(0x1D),
        ];
        out[pos..pos + 4].copy_from_slice(&mask);
        pos += 4;

        // Masked payload
        for (i, &b) in payload.iter().enumerate() {
            out[pos + i] = b ^ mask[i & 3];
        }
        pos += plen;

        pos
    }

    #[inline]
    fn maybe_compact(&mut self) {
        if self.consumed > 0 && self.consumed > READ_BUF_CAPACITY / 2 {
            self.buf.copy_within(self.consumed..self.filled, 0);
            self.filled -= self.consumed;
            self.consumed = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build an unmasked server→client text frame (for test injection).
    fn make_server_text_frame(payload: &[u8]) -> Vec<u8> {
        make_server_frame(OP_TEXT, payload)
    }

    fn make_server_frame(opcode: u8, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(0x80 | opcode); // FIN + opcode

        let plen = payload.len();
        if plen <= 125 {
            out.push(plen as u8); // no mask bit (server→client)
        } else if plen <= 65535 {
            out.push(126);
            out.extend_from_slice(&(plen as u16).to_be_bytes());
        } else {
            out.push(127);
            out.extend_from_slice(&(plen as u64).to_be_bytes());
        }

        out.extend_from_slice(payload);
        out
    }

    fn make_masked_server_frame(opcode: u8, payload: &[u8], mask: [u8; 4]) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(0x80 | opcode);

        let plen = payload.len();
        if plen <= 125 {
            out.push(0x80 | plen as u8); // mask bit set
        } else if plen <= 65535 {
            out.push(0x80 | 126);
            out.extend_from_slice(&(plen as u16).to_be_bytes());
        } else {
            out.push(0x80 | 127);
            out.extend_from_slice(&(plen as u64).to_be_bytes());
        }

        out.extend_from_slice(&mask);
        for (i, &b) in payload.iter().enumerate() {
            out.push(b ^ mask[i & 3]);
        }
        out
    }

    // ── Frame parsing ──────────────────────────────────────────────

    #[test]
    fn parse_small_text_frame() {
        let mut framer = WsFramer::new();
        let raw = make_server_text_frame(b"hello");
        framer.inject(&raw);

        let frame = framer.next_frame().unwrap();
        assert_eq!(frame.opcode, OP_TEXT);
        assert_eq!(frame.payload, b"hello");
        assert!(framer.next_frame().is_none());
    }

    #[test]
    fn parse_medium_text_frame() {
        let payload = vec![b'x'; 300]; // > 125, uses 16-bit length
        let mut framer = WsFramer::new();
        framer.inject(&make_server_text_frame(&payload));

        let frame = framer.next_frame().unwrap();
        assert_eq!(frame.opcode, OP_TEXT);
        assert_eq!(frame.payload.len(), 300);
        assert!(frame.payload.iter().all(|&b| b == b'x'));
    }

    #[test]
    fn parse_large_text_frame() {
        let payload = vec![b'A'; 70_000]; // > 65535, uses 64-bit length
        let mut framer = WsFramer::new();
        framer.inject(&make_server_text_frame(&payload));

        let frame = framer.next_frame().unwrap();
        assert_eq!(frame.opcode, OP_TEXT);
        assert_eq!(frame.payload.len(), 70_000);
    }

    #[test]
    fn parse_ping_frame() {
        let mut framer = WsFramer::new();
        framer.inject(&make_server_frame(OP_PING, b""));

        let frame = framer.next_frame().unwrap();
        assert_eq!(frame.opcode, OP_PING);
        assert_eq!(frame.payload.len(), 0);
    }

    #[test]
    fn parse_pong_frame() {
        let mut framer = WsFramer::new();
        framer.inject(&make_server_frame(OP_PONG, b"pongdata"));

        let frame = framer.next_frame().unwrap();
        assert_eq!(frame.opcode, OP_PONG);
        assert_eq!(frame.payload, b"pongdata");
    }

    #[test]
    fn parse_close_frame() {
        let mut framer = WsFramer::new();
        // Close frame with 2-byte status code
        framer.inject(&make_server_frame(OP_CLOSE, &[0x03, 0xE8]));

        let frame = framer.next_frame().unwrap();
        assert_eq!(frame.opcode, OP_CLOSE);
        assert_eq!(frame.payload, &[0x03, 0xE8]);
    }

    #[test]
    fn parse_binary_frame() {
        let mut framer = WsFramer::new();
        framer.inject(&make_server_frame(OP_BINARY, &[0xFF, 0x00, 0xAB]));

        let frame = framer.next_frame().unwrap();
        assert_eq!(frame.opcode, OP_BINARY);
        assert_eq!(frame.payload, &[0xFF, 0x00, 0xAB]);
    }

    #[test]
    fn parse_masked_frame() {
        let mask = [0x12, 0x34, 0x56, 0x78];
        let mut framer = WsFramer::new();
        framer.inject(&make_masked_server_frame(OP_TEXT, b"hello", mask));

        let frame = framer.next_frame().unwrap();
        assert_eq!(frame.opcode, OP_TEXT);
        assert_eq!(frame.payload, b"hello"); // unmasked by framer
    }

    // ── Edge cases ─────────────────────────────────────────────────

    #[test]
    fn partial_header_returns_none() {
        let mut framer = WsFramer::new();
        framer.inject(&[0x81]); // only 1 byte of header
        assert!(framer.next_frame().is_none());
    }

    #[test]
    fn partial_payload_returns_none() {
        let mut framer = WsFramer::new();
        // Header says 10 bytes payload, but only provide 5
        let mut raw = make_server_text_frame(&[0u8; 10]);
        raw.truncate(raw.len() - 5);
        framer.inject(&raw);
        assert!(framer.next_frame().is_none());
    }

    #[test]
    fn partial_extended_length_returns_none() {
        let mut framer = WsFramer::new();
        // 16-bit length header needs 4 bytes, only give 3
        framer.inject(&[0x81, 126, 0x01]); // missing second length byte
        assert!(framer.next_frame().is_none());
    }

    #[test]
    fn partial_64bit_length_returns_none() {
        let mut framer = WsFramer::new();
        // 64-bit length header needs 10 bytes, only give 6
        framer.inject(&[0x81, 127, 0, 0, 0, 0]);
        assert!(framer.next_frame().is_none());
    }

    #[test]
    fn two_frames_in_buffer() {
        let mut framer = WsFramer::new();
        let f1 = make_server_text_frame(b"first");
        let f2 = make_server_text_frame(b"second");
        framer.inject(&f1);
        framer.inject(&f2);

        let frame1 = framer.next_frame().unwrap();
        assert_eq!(frame1.payload, b"first");

        let frame2 = framer.next_frame().unwrap();
        assert_eq!(frame2.payload, b"second");

        assert!(framer.next_frame().is_none());
    }

    #[test]
    fn frame_split_across_recvs() {
        let raw = make_server_text_frame(b"split test");
        let mid = raw.len() / 2;

        let mut framer = WsFramer::new();
        framer.inject(&raw[..mid]);
        assert!(framer.next_frame().is_none()); // incomplete

        framer.inject(&raw[mid..]);
        let frame = framer.next_frame().unwrap();
        assert_eq!(frame.payload, b"split test");
    }

    #[test]
    fn buffer_compaction() {
        let mut framer = WsFramer::new();

        // Fill past halfway to trigger compaction
        let big_payload = vec![b'Z'; READ_BUF_CAPACITY / 2 - 10];
        let big_frame = make_server_text_frame(&big_payload);
        framer.inject(&big_frame);

        // Consume the big frame
        let frame = framer.next_frame().unwrap();
        assert_eq!(frame.payload.len(), big_payload.len());

        // Now consumed > half. Inject another frame — should trigger compaction.
        let small_frame = make_server_text_frame(b"after compact");
        framer.inject(&small_frame);

        let frame = framer.next_frame().unwrap();
        assert_eq!(frame.payload, b"after compact");
    }

    #[test]
    fn payload_slice_points_into_buffer() {
        let mut framer = WsFramer::new();
        framer.inject(&make_server_text_frame(b"zero-copy check"));

        let buf_start = framer.buf.as_ptr() as usize;
        let buf_end = buf_start + READ_BUF_CAPACITY;

        let frame = framer.next_frame().unwrap();
        let payload_start = frame.payload.as_ptr() as usize;

        assert!(
            payload_start >= buf_start && payload_start < buf_end,
            "payload pointer {:#x} not within buffer [{:#x}..{:#x})",
            payload_start,
            buf_start,
            buf_end
        );
    }

    // ── Encode round-trip ──────────────────────────────────────────

    #[test]
    fn encode_text_round_trip() {
        let msg = b"subscribe message";
        let mut wire = [0u8; 256];
        let len = WsFramer::encode_text(msg, &mut wire);

        // The encoded frame is a client (masked) frame.
        // Verify it by parsing with mask support.
        let mut framer = WsFramer::new();
        framer.inject(&wire[..len]);
        let frame = framer.next_frame().unwrap();
        assert_eq!(frame.opcode, OP_TEXT);
        assert_eq!(frame.payload, msg);
    }

    #[test]
    fn encode_ping_round_trip() {
        let mut wire = [0u8; 64];
        let len = WsFramer::encode_ping(b"", &mut wire);

        let mut framer = WsFramer::new();
        framer.inject(&wire[..len]);
        let frame = framer.next_frame().unwrap();
        assert_eq!(frame.opcode, OP_PING);
    }

    // ── Stress ─────────────────────────────────────────────────────

    #[test]
    fn many_frames_sequential() {
        let mut framer = WsFramer::new();

        for i in 0u32..1000 {
            let payload = format!("msg-{}", i);
            let raw = make_server_text_frame(payload.as_bytes());
            framer.inject(&raw);

            let frame = framer.next_frame().unwrap();
            assert_eq!(frame.payload, payload.as_bytes());
        }
    }

    #[test]
    fn empty_payload_frame() {
        let mut framer = WsFramer::new();
        framer.inject(&make_server_text_frame(b""));

        let frame = framer.next_frame().unwrap();
        assert_eq!(frame.opcode, OP_TEXT);
        assert_eq!(frame.payload.len(), 0);
    }

    #[test]
    fn exactly_125_byte_payload() {
        let payload = vec![b'A'; 125]; // boundary: max 7-bit length
        let mut framer = WsFramer::new();
        framer.inject(&make_server_text_frame(&payload));

        let frame = framer.next_frame().unwrap();
        assert_eq!(frame.payload.len(), 125);
    }

    #[test]
    fn exactly_126_byte_payload() {
        let payload = vec![b'B'; 126]; // boundary: switches to 16-bit length
        let mut framer = WsFramer::new();
        framer.inject(&make_server_text_frame(&payload));

        let frame = framer.next_frame().unwrap();
        assert_eq!(frame.payload.len(), 126);
    }

    #[test]
    fn exactly_65535_byte_payload() {
        let payload = vec![b'C'; 65535]; // boundary: max 16-bit length
        let mut framer = WsFramer::new();
        framer.inject(&make_server_text_frame(&payload));

        let frame = framer.next_frame().unwrap();
        assert_eq!(frame.payload.len(), 65535);
    }

    #[test]
    fn exactly_65536_byte_payload() {
        let payload = vec![b'D'; 65536]; // boundary: switches to 64-bit length
        let mut framer = WsFramer::new();
        framer.inject(&make_server_text_frame(&payload));

        let frame = framer.next_frame().unwrap();
        assert_eq!(frame.payload.len(), 65536);
    }
}
