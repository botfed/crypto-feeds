use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU64, Ordering};

const DEFAULT_CAPACITY: usize = 65_536; // 2^16, power of 2 for bitmask indexing

/// A single slot in the ring buffer, aligned to a cache line boundary
/// to prevent false sharing between adjacent slots.
///
/// The `seq` field implements a seqlock:
/// - Odd  → write in progress
/// - Even → write complete; the low bits encode the generation
#[repr(C, align(128))]
struct Slot<T> {
    seq: AtomicU64,
    data: UnsafeCell<T>,
}

/// Lock-free single-writer / multi-reader ring buffer.
///
/// Uses a seqlock per slot so readers never block the writer and always get a
/// consistent snapshot (they retry on torn reads).
pub struct RingBuffer<T> {
    buf: Box<[Slot<T>]>,
    write_pos: AtomicU64,
    capacity: usize,
}

// SAFETY: Only a single writer calls `push` at a time (guaranteed by the
// architecture: one websocket task per exchange-symbol). Readers use the
// seqlock protocol to detect torn reads and retry.
unsafe impl<T: Send> Send for RingBuffer<T> {}
unsafe impl<T: Send> Sync for RingBuffer<T> {}

impl<T: Copy + Default + Send> RingBuffer<T> {
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    pub fn with_capacity(cap: usize) -> Self {
        assert!(cap.is_power_of_two(), "capacity must be a power of 2");
        let mut slots = Vec::with_capacity(cap);
        for _ in 0..cap {
            slots.push(Slot {
                seq: AtomicU64::new(0),
                data: UnsafeCell::new(T::default()),
            });
        }
        Self {
            buf: slots.into_boxed_slice(),
            write_pos: AtomicU64::new(0),
            capacity: cap,
        }
    }

    #[inline]
    fn index(&self, pos: u64) -> usize {
        (pos as usize) & (self.capacity - 1)
    }

    /// Write a new entry into the ring buffer (single-writer only).
    pub fn push(&self, data: T) {
        let pos = self.write_pos.load(Ordering::Relaxed);
        let idx = self.index(pos);
        let slot = &self.buf[idx];

        // Mark write-in-progress (odd seq)
        let seq = pos * 2 + 1;
        slot.seq.store(seq, Ordering::Release);

        // SAFETY: Single writer guarantee — no other thread writes to this slot.
        unsafe {
            slot.data.get().write(data);
        }

        // Mark write-complete (even seq)
        slot.seq.store(seq + 1, Ordering::Release);

        // Advance write position
        self.write_pos.store(pos + 1, Ordering::Release);
    }

    /// Read the latest entry. Returns `None` if nothing has been written yet.
    pub fn latest(&self) -> Option<T> {
        let pos = self.write_pos.load(Ordering::Acquire);
        if pos == 0 {
            return None;
        }
        self.read_at(pos - 1)
    }

    /// Read a specific historical entry by its absolute position.
    ///
    /// Returns `None` if:
    /// - The position hasn't been written yet
    /// - The position has been overwritten (wrapped around)
    /// - A torn read is detected after retries
    pub fn read_at(&self, pos: u64) -> Option<T> {
        let current = self.write_pos.load(Ordering::Acquire);

        // Not yet written
        if pos >= current {
            return None;
        }

        // Already overwritten (too far behind)
        if current - pos > self.capacity as u64 {
            return None;
        }

        let idx = self.index(pos);
        let slot = &self.buf[idx];
        let expected_seq = (pos * 2) + 2; // completed write seq for this position

        for _ in 0..4 {
            let seq1 = slot.seq.load(Ordering::Acquire);

            // Write in progress (odd) or wrong generation
            if seq1 & 1 != 0 || seq1 != expected_seq {
                std::hint::spin_loop();
                continue;
            }

            // SAFETY: seq is even and matches expected, so no concurrent write.
            let data = unsafe { *slot.data.get() };

            let seq2 = slot.seq.load(Ordering::Acquire);
            if seq1 == seq2 {
                return Some(data);
            }

            std::hint::spin_loop();
        }

        None // torn read after retries
    }

    /// Batch read of the most-recent N entries into a caller-provided buffer.
    ///
    /// Walks backward from write_pos, respects seqlock. Returns actual count read.
    /// `out[0]` = most recent entry.
    pub fn read_last_n(&self, n: usize, out: &mut [T]) -> usize {
        let current = self.write_pos.load(Ordering::Acquire);
        if current == 0 {
            return 0;
        }

        let available = current.min(self.capacity as u64) as usize;
        let to_read = n.min(available).min(out.len());
        let mut count = 0;

        for i in 0..to_read {
            let pos = current - 1 - i as u64;
            if let Some(data) = self.read_at(pos) {
                out[count] = data;
                count += 1;
            }
            // skip torn reads, keep going
        }

        count
    }

    /// Current write position (number of entries written so far).
    pub fn write_pos(&self) -> u64 {
        self.write_pos.load(Ordering::Acquire)
    }

    /// Buffer capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market_data::MarketData;
    use chrono::Utc;

    fn make_md(bid: f64) -> MarketData {
        MarketData {
            bid: Some(bid),
            ask: Some(bid + 1.0),
            bid_qty: Some(1.0),
            ask_qty: Some(1.0),
            exchange_ts: None,
            received_ts: Some(Utc::now()),
        }
    }

    #[test]
    fn test_push_and_latest() {
        let rb = RingBuffer::<MarketData>::new();
        assert!(rb.latest().is_none());

        rb.push(make_md(100.0));
        let md = rb.latest().unwrap();
        assert_eq!(md.bid, Some(100.0));

        rb.push(make_md(200.0));
        let md = rb.latest().unwrap();
        assert_eq!(md.bid, Some(200.0));
    }

    #[test]
    fn test_read_at() {
        let rb = RingBuffer::<MarketData>::new();

        rb.push(make_md(10.0));
        rb.push(make_md(20.0));
        rb.push(make_md(30.0));

        assert_eq!(rb.read_at(0).unwrap().bid, Some(10.0));
        assert_eq!(rb.read_at(1).unwrap().bid, Some(20.0));
        assert_eq!(rb.read_at(2).unwrap().bid, Some(30.0));
        assert!(rb.read_at(3).is_none()); // not yet written
    }

    #[test]
    fn test_wraparound() {
        let rb = RingBuffer::<MarketData>::new();

        // Write more than capacity
        for i in 0..(DEFAULT_CAPACITY as u64 + 100) {
            rb.push(make_md(i as f64));
        }

        let total = DEFAULT_CAPACITY as u64 + 100;

        // Latest should be the last one written
        let md = rb.latest().unwrap();
        assert_eq!(md.bid, Some((total - 1) as f64));

        // Old positions should be gone (overwritten)
        assert!(rb.read_at(0).is_none());

        // Recent positions should be readable
        let recent = total - 1;
        assert_eq!(rb.read_at(recent).unwrap().bid, Some(recent as f64));
    }

    #[test]
    fn test_with_capacity() {
        let rb = RingBuffer::<MarketData>::with_capacity(8192);
        for i in 0..100u64 {
            rb.push(make_md(i as f64));
        }
        assert_eq!(rb.latest().unwrap().bid, Some(99.0));
    }

    #[test]
    #[should_panic(expected = "capacity must be a power of 2")]
    fn test_non_power_of_two_panics() {
        let _rb = RingBuffer::<MarketData>::with_capacity(100);
    }

    #[test]
    fn test_read_last_n() {
        let rb = RingBuffer::<MarketData>::new();

        for i in 0..10u64 {
            rb.push(make_md(i as f64));
        }

        let mut buf = [MarketData::default(); 5];
        let count = rb.read_last_n(5, &mut buf);
        assert_eq!(count, 5);
        // out[0] = most recent (9), out[1] = 8, ...
        assert_eq!(buf[0].bid, Some(9.0));
        assert_eq!(buf[1].bid, Some(8.0));
        assert_eq!(buf[4].bid, Some(5.0));
    }

    #[test]
    fn test_read_last_n_more_than_available() {
        let rb = RingBuffer::<MarketData>::new();

        rb.push(make_md(1.0));
        rb.push(make_md(2.0));

        let mut buf = [MarketData::default(); 10];
        let count = rb.read_last_n(10, &mut buf);
        assert_eq!(count, 2);
        assert_eq!(buf[0].bid, Some(2.0));
        assert_eq!(buf[1].bid, Some(1.0));
    }

    #[test]
    fn test_concurrent_reader_writer() {
        use std::sync::Arc;
        use std::thread;

        let rb = Arc::new(RingBuffer::<MarketData>::new());
        let writes = 100_000u64;

        let rb_writer = Arc::clone(&rb);
        let writer = thread::spawn(move || {
            for i in 0..writes {
                rb_writer.push(make_md(i as f64));
            }
        });

        let rb_reader = Arc::clone(&rb);
        let reader = thread::spawn(move || {
            let mut last_seen = None;
            let mut reads = 0u64;
            while reads < writes * 2 {
                if let Some(md) = rb_reader.latest() {
                    // bid and ask must be consistent (ask = bid + 1.0)
                    let bid = md.bid.unwrap();
                    let ask = md.ask.unwrap();
                    assert!(
                        (ask - bid - 1.0).abs() < f64::EPSILON,
                        "Torn read detected: bid={}, ask={}",
                        bid,
                        ask
                    );

                    // Monotonically increasing
                    if let Some(prev) = last_seen {
                        assert!(bid >= prev, "bid went backwards: {} -> {}", prev, bid);
                    }
                    last_seen = Some(bid);
                }
                reads += 1;
            }
        });

        writer.join().unwrap();
        reader.join().unwrap();
    }
}
