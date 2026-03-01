use std::cell::UnsafeCell;
use std::collections::BTreeMap;

/// `UnsafeCell<OrderBook>` wrapper that is `Send + Sync`.
///
/// SAFETY: The caller must guarantee single-writer access. In this codebase
/// each order book is owned by exactly one WebSocket feed task — the same
/// guarantee that [`crate::ring_buffer::RingBuffer`] relies on.
pub struct SyncBook(UnsafeCell<OrderBook>);

// SAFETY: Single writer per book (one WS task per exchange-symbol).
unsafe impl Send for SyncBook {}
unsafe impl Sync for SyncBook {}

impl SyncBook {
    pub fn new() -> Self {
        Self(UnsafeCell::new(OrderBook::new()))
    }

    /// Get a mutable reference to the inner order book.
    ///
    /// # Safety
    /// Must only be called from a single writer task.
    #[inline]
    pub unsafe fn get_mut(&self) -> &mut OrderBook {
        unsafe { &mut *self.0.get() }
    }
}

pub struct OrderBook {
    pub bids: BTreeMap<ordered_float::OrderedFloat<f64>, f64>, // price -> size
    pub asks: BTreeMap<ordered_float::OrderedFloat<f64>, f64>,
}

impl OrderBook {
    pub fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }

    pub fn update_bids(&mut self, updates: Vec<(String, f64)>) {
        for (price, size) in updates {
            if let Ok(price_f64) = price.parse::<f64>() {
                let key = ordered_float::OrderedFloat(price_f64);
                if size == 0.0 {
                    // Size 0 means remove this level
                    self.bids.remove(&key);
                } else {
                    self.bids.insert(key, size);
                }
            }
        }
    }
    pub fn update_asks(&mut self, updates: Vec<(String, f64)>) {
        for (price, size) in updates {
            if let Ok(price_f64) = price.parse::<f64>() {
                let key = ordered_float::OrderedFloat(price_f64);
                if size == 0.0 {
                    // Size 0 means remove this level
                    self.asks.remove(&key);
                } else {
                    self.asks.insert(key, size);
                }
            }
        }
    }

    pub fn best_bid(&self) -> Option<(f64, f64)> {
        // BTreeMap is sorted, get highest bid
        self.bids
            .iter()
            .next_back()
            .map(|(price, &size)| (price.0, size))
    }

    pub fn best_ask(&self) -> Option<(f64, f64)> {
        // Get lowest ask
        self.asks
            .iter()
            .next()
            .map(|(price, &size)| (price.0, size))
    }
}
