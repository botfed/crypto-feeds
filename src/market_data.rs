use crate::ring_buffer::RingBuffer;
use crate::symbol_registry::{MAX_SYMBOLS, SymbolId};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Instant;

#[derive(Debug, Copy, Clone)]
pub struct MarketData {
    pub bid: Option<f64>,
    pub ask: Option<f64>,
    pub bid_qty: Option<f64>,
    pub ask_qty: Option<f64>,
    pub exchange_ts_raw: Option<DateTime<Utc>>,
    pub exchange_ts: Option<DateTime<Utc>>,
    pub received_ts: Option<DateTime<Utc>>,
    pub received_instant: Option<Instant>,
    /// Feed processing latency: WS recv → ring buffer write (nanoseconds)
    pub feed_latency_ns: u64,
}

impl Default for MarketData {
    fn default() -> Self {
        Self {
            bid: None,
            ask: None,
            bid_qty: None,
            ask_qty: None,
            exchange_ts_raw: None,
            exchange_ts: None,
            received_ts: None,
            received_instant: None,
            feed_latency_ns: 0,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClockCorrectionConfig {
    #[serde(default = "default_cc_method")]
    pub method: String,
    #[serde(default = "default_min_latency_ms")]
    pub min_latency_ms: f64,
    #[serde(default = "default_ewma_decay")]
    pub ewma_decay: f64,
}

fn default_cc_method() -> String {
    "ewma".to_string()
}
fn default_min_latency_ms() -> f64 {
    1.0
}
fn default_ewma_decay() -> f64 {
    0.999
}

impl Default for ClockCorrectionConfig {
    fn default() -> Self {
        Self {
            method: default_cc_method(),
            min_latency_ms: default_min_latency_ms(),
            ewma_decay: default_ewma_decay(),
        }
    }
}

impl ClockCorrectionConfig {
    pub fn min_latency_ns(&self) -> i64 {
        (self.min_latency_ms * 1_000_000.0) as i64
    }
}

#[derive(Clone, Copy, Debug)]
pub enum InstrumentType {
    Spot,
    Perp,
    Option,
    Futures,
}

impl InstrumentType {
    pub fn as_str(&self) -> &'static str {
        match self {
            InstrumentType::Spot => "SPOT",
            InstrumentType::Perp => "PERP",
            InstrumentType::Option => "OPTION",
            InstrumentType::Futures => "FUT",
        }
    }
}

impl MarketData {
    pub fn midquote(&self) -> Option<f64> {
        return Some((self.bid? + self.ask?) / 2.0);
    }
    pub fn microprice(&self) -> Option<f64> {
        let bid = self.bid?;
        let ask = self.ask?;
        let bid_qty = self.bid_qty?;
        let ask_qty = self.ask_qty?;
        let sum = bid_qty + ask_qty;
        if sum > 0.0 {
            Some((bid * ask_qty + ask * bid_qty) / (bid_qty + ask_qty))
        } else {
            None
        }
    }
}

/// Trait for data types that can flow through the generic feed infrastructure.
pub trait FeedItem: Copy + Default + Send + Sync + 'static {
    fn exchange_ts_raw(&self) -> Option<DateTime<Utc>>;
    fn set_feed_latency_ns(&mut self, ns: u64);
}

impl FeedItem for MarketData {
    fn exchange_ts_raw(&self) -> Option<DateTime<Utc>> {
        self.exchange_ts_raw
    }
    fn set_feed_latency_ns(&mut self, ns: u64) {
        self.feed_latency_ns = ns;
    }
}

/// Trait for collections that can receive feed items from the connection loop.
pub trait DataSink<T>: Send + Sync {
    fn push(&self, id: &SymbolId, item: T);
}

impl DataSink<MarketData> for MarketDataCollection {
    fn push(&self, id: &SymbolId, item: MarketData) {
        MarketDataCollection::push(self, id, item);
    }
}

struct SymbolSlot {
    ring: OnceLock<Box<RingBuffer<MarketData>>>,
    clock_offset_ewma_ns: AtomicI64,
    clock_offset_max_ns: AtomicI64,
}

pub struct MarketDataCollection {
    slots: Box<[SymbolSlot]>,
    clock_config: ClockCorrectionConfig,
}

// Debug impl since OnceLock<Box<RingBuffer>> doesn't derive Debug
impl std::fmt::Debug for MarketDataCollection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MarketDataCollection")
            .field("capacity", &self.slots.len())
            .finish()
    }
}

pub struct AllMarketData {
    pub binance: Arc<MarketDataCollection>,
    pub coinbase: Arc<MarketDataCollection>,
    pub bybit: Arc<MarketDataCollection>,
    pub kraken: Arc<MarketDataCollection>,
    pub lighter: Arc<MarketDataCollection>,
    pub mexc: Arc<MarketDataCollection>,
    pub extended: Arc<MarketDataCollection>,
    pub nado: Arc<MarketDataCollection>,
    pub okx: Arc<MarketDataCollection>,
    pub kucoin: Arc<MarketDataCollection>,
    pub bingx: Arc<MarketDataCollection>,
    pub apex: Arc<MarketDataCollection>,
    pub hyperliquid: Arc<MarketDataCollection>,
    pub aerodrome: Arc<MarketDataCollection>,
    pub uniswap: Arc<MarketDataCollection>,
    pub hibachi: Arc<MarketDataCollection>,
    pub hotstuff: Arc<MarketDataCollection>,
    pub zeroone: Arc<MarketDataCollection>,
    pub risex: Arc<MarketDataCollection>,
}

// Debug impl
impl std::fmt::Debug for AllMarketData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AllMarketData").finish()
    }
}

#[derive(Clone, Copy, Hash, Eq, PartialEq)]
pub enum Exchange {
    Binance,
    Coinbase,
    Bybit,
    Kraken,
    Lighter,
    Mexc,
    Extended,
    Nado,
    Okx,
    Kucoin,
    Bingx,
    Apex,
    Hyperliquid,
    Aerodrome,
    Uniswap,
    Hibachi,
    Hotstuff,
    ZeroOne,
    RiseX,
}

impl Exchange {
    pub fn as_str(&self) -> &'static str {
        match self {
            Exchange::Binance => "binance",
            Exchange::Coinbase => "coinbase",
            Exchange::Bybit => "bybit",
            Exchange::Kraken => "kraken",
            Exchange::Lighter => "lighter",
            Exchange::Mexc => "mexc",
            Exchange::Extended => "extended",
            Exchange::Nado => "nado",
            Exchange::Okx => "okx",
            Exchange::Kucoin => "kucoin",
            Exchange::Bingx => "bingx",
            Exchange::Apex => "apex",
            Exchange::Hyperliquid => "hyperliquid",
            Exchange::Aerodrome => "aerodrome",
            Exchange::Uniswap => "uniswap",
            Exchange::Hibachi => "hibachi",
            Exchange::Hotstuff => "hotstuff",
            Exchange::ZeroOne => "zeroone",
            Exchange::RiseX => "risex",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "binance" => Some(Exchange::Binance),
            "coinbase" => Some(Exchange::Coinbase),
            "bybit" => Some(Exchange::Bybit),
            "kraken" => Some(Exchange::Kraken),
            "lighter" => Some(Exchange::Lighter),
            "mexc" => Some(Exchange::Mexc),
            "extended" => Some(Exchange::Extended),
            "nado" => Some(Exchange::Nado),
            "okx" => Some(Exchange::Okx),
            "kucoin" => Some(Exchange::Kucoin),
            "bingx" => Some(Exchange::Bingx),
            "apex" => Some(Exchange::Apex),
            "hyperliquid" => Some(Exchange::Hyperliquid),
            "aerodrome" => Some(Exchange::Aerodrome),
            "uniswap" => Some(Exchange::Uniswap),
            "hibachi" => Some(Exchange::Hibachi),
            "hotstuff" => Some(Exchange::Hotstuff),
            "zeroone" => Some(Exchange::ZeroOne),
            "risex" | "rise" => Some(Exchange::RiseX),
            _ => None,
        }
    }
}

impl AllMarketData {
    pub fn iter(&self) -> impl Iterator<Item = (Exchange, &Arc<MarketDataCollection>)> {
        use Exchange::*;
        [
            (Binance, &self.binance),
            (Coinbase, &self.coinbase),
            (Bybit, &self.bybit),
            (Kraken, &self.kraken),
            (Lighter, &self.lighter),
            (Mexc, &self.mexc),
            (Extended, &self.extended),
            (Nado, &self.nado),
            (Okx, &self.okx),
            (Kucoin, &self.kucoin),
            (Bingx, &self.bingx),
            (Apex, &self.apex),
            (Hyperliquid, &self.hyperliquid),
            (Aerodrome, &self.aerodrome),
            (Uniswap, &self.uniswap),
            (Hibachi, &self.hibachi),
            (Hotstuff, &self.hotstuff),
            (ZeroOne, &self.zeroone),
            (RiseX, &self.risex),
        ]
        .into_iter()
    }

    pub fn get_collection(&self, exchange: &Exchange) -> &Arc<MarketDataCollection> {
        match exchange {
            Exchange::Binance => &self.binance,
            Exchange::Coinbase => &self.coinbase,
            Exchange::Bybit => &self.bybit,
            Exchange::Kraken => &self.kraken,
            Exchange::Lighter => &self.lighter,
            Exchange::Mexc => &self.mexc,
            Exchange::Extended => &self.extended,
            Exchange::Nado => &self.nado,
            Exchange::Okx => &self.okx,
            Exchange::Kucoin => &self.kucoin,
            Exchange::Bingx => &self.bingx,
            Exchange::Apex => &self.apex,
            Exchange::Hyperliquid => &self.hyperliquid,
            Exchange::Aerodrome => &self.aerodrome,
            Exchange::Uniswap => &self.uniswap,
            Exchange::Hibachi => &self.hibachi,
            Exchange::Hotstuff => &self.hotstuff,
            Exchange::ZeroOne => &self.zeroone,
            Exchange::RiseX => &self.risex,
        }
    }
}

impl AllMarketData {
    pub fn new() -> Self {
        Self::with_clock_correction(ClockCorrectionConfig::default())
    }

    pub fn with_clock_correction(clock_config: ClockCorrectionConfig) -> Self {
        let new_coll = || Arc::new(MarketDataCollection::new(clock_config.clone()));
        Self {
            binance: new_coll(),
            bybit: new_coll(),
            coinbase: new_coll(),
            kraken: new_coll(),
            lighter: new_coll(),
            mexc: new_coll(),
            extended: new_coll(),
            nado: new_coll(),
            okx: new_coll(),
            kucoin: new_coll(),
            bingx: new_coll(),
            apex: new_coll(),
            hyperliquid: new_coll(),
            aerodrome: new_coll(),
            uniswap: new_coll(),
            hibachi: new_coll(),
            hotstuff: new_coll(),
            zeroone: new_coll(),
            risex: new_coll(),
        }
    }
}

impl MarketDataCollection {
    pub fn new(clock_config: ClockCorrectionConfig) -> Self {
        let mut slots = Vec::with_capacity(MAX_SYMBOLS);
        for _ in 0..MAX_SYMBOLS {
            slots.push(SymbolSlot {
                ring: OnceLock::new(),
                clock_offset_ewma_ns: AtomicI64::new(0),
                clock_offset_max_ns: AtomicI64::new(0),
            });
        }
        Self {
            slots: slots.into_boxed_slice(),
            clock_config,
        }
    }

    /// Push a new tick for the given symbol (interior-mutable, no &mut self needed).
    pub fn push(&self, id: &SymbolId, mut market_data: MarketData) {
        let slot = &self.slots[*id];

        // Clock correction: track per-symbol offset
        let min_latency_ns = self.clock_config.min_latency_ns();
        if let (Some(exch), Some(recv)) = (market_data.exchange_ts_raw, market_data.received_ts) {
            let delta_ns = (recv - exch).num_nanoseconds().unwrap_or(0) - min_latency_ns;
            if delta_ns < 0 {
                let prev = slot.clock_offset_ewma_ns.load(Ordering::Relaxed);
                let new = (self.clock_config.ewma_decay * prev as f64
                    + (1.0 - self.clock_config.ewma_decay) * delta_ns as f64)
                    as i64;
                slot.clock_offset_ewma_ns.store(new, Ordering::Relaxed);
                let cur_max = slot.clock_offset_max_ns.load(Ordering::Relaxed);
                if delta_ns < cur_max {
                    slot.clock_offset_max_ns.store(delta_ns, Ordering::Relaxed);
                }
            }
        }

        let offset = match self.clock_config.method.as_str() {
            "ewma" => slot.clock_offset_ewma_ns.load(Ordering::Relaxed).min(0),
            "max" => slot.clock_offset_max_ns.load(Ordering::Relaxed).min(0),
            _ => 0,
        };
        market_data.exchange_ts = market_data
            .exchange_ts_raw
            .map(|t| t + chrono::Duration::nanoseconds(offset));

        let ring = slot.ring.get_or_init(|| Box::new(RingBuffer::new()));
        ring.push(market_data);
    }

    /// Get the latest tick for a symbol (owned copy via seqlock read).
    pub fn latest(&self, id: &SymbolId) -> Option<MarketData> {
        self.slots[*id].ring.get()?.latest()
    }

    /// Number of ticks written for this symbol. Use to detect new data.
    pub fn write_count(&self, id: &SymbolId) -> u64 {
        self.slots[*id]
            .ring
            .get()
            .map(|r| r.write_count())
            .unwrap_or(0)
    }

    /// Direct access to the underlying ring buffer for a symbol.
    pub fn get_buffer(&self, id: &SymbolId) -> Option<&RingBuffer<MarketData>> {
        self.slots[*id].ring.get().map(|b| b.as_ref())
    }

    pub fn get_midquote(&self, id: &SymbolId) -> Option<f64> {
        let md = self.latest(id)?;
        let bid = md.bid?;
        let ask = md.ask?;
        Some((bid + ask) / 2.0)
    }

    pub fn get_microprice(&self, id: &SymbolId) -> Option<f64> {
        let md = self.latest(id)?;
        let bid = md.bid?;
        let ask = md.ask?;
        let bid_qty = md.bid_qty?;
        let ask_qty = md.ask_qty?;
        let sum = bid_qty + ask_qty;
        if sum > 0.0 {
            Some((bid * ask_qty + ask * bid_qty) / (bid_qty + ask_qty))
        } else {
            None
        }
    }

    /// Blocking latest with retries — for diagnostic/non-hot-path reads.
    pub fn latest_blocking(&self, id: &SymbolId) -> Option<MarketData> {
        self.slots[*id].ring.get()?.latest_blocking()
    }

    pub fn get_midquote_blocking(&self, id: &SymbolId) -> Option<f64> {
        let md = self.latest_blocking(id)?;
        let bid = md.bid?;
        let ask = md.ask?;
        Some((bid + ask) / 2.0)
    }

    pub fn get_midquote_w_timestamps(
        &self,
        id: &SymbolId,
    ) -> Option<(f64, Option<DateTime<Utc>>, Option<DateTime<Utc>>)> {
        let md = self.latest(id)?;
        let mid = (md.bid? + md.ask?) / 2.0;
        Some((mid, md.received_ts, md.exchange_ts))
    }
}
