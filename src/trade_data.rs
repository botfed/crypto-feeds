use crate::market_data::{ClockCorrectionConfig, DataSink, Exchange, FeedItem};
use crate::ring_buffer::RingBuffer;
use crate::symbol_registry::{MAX_SYMBOLS, SymbolId};
use chrono::{DateTime, Utc};
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Instant;

#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum TradeSide {
    #[default]
    Unknown = 0,
    Buy = 1,
    Sell = 2,
}

#[derive(Debug, Copy, Clone)]
pub struct TradeData {
    pub price: f64,
    pub qty: f64,
    pub side: TradeSide,
    pub exchange_ts_raw: Option<DateTime<Utc>>,
    pub exchange_ts: Option<DateTime<Utc>>,
    pub received_ts: Option<DateTime<Utc>>,
    pub received_instant: Option<Instant>,
    pub feed_latency_ns: u64,
}

impl Default for TradeData {
    fn default() -> Self {
        Self {
            price: 0.0,
            qty: 0.0,
            side: TradeSide::Unknown,
            exchange_ts_raw: None,
            exchange_ts: None,
            received_ts: None,
            received_instant: None,
            feed_latency_ns: 0,
        }
    }
}

impl FeedItem for TradeData {
    fn exchange_ts_raw(&self) -> Option<DateTime<Utc>> {
        self.exchange_ts_raw
    }
    fn set_feed_latency_ns(&mut self, ns: u64) {
        self.feed_latency_ns = ns;
    }
}

struct TradeSlot {
    ring: OnceLock<Box<RingBuffer<TradeData>>>,
    clock_offset_ewma_ns: AtomicI64,
    clock_offset_max_ns: AtomicI64,
}

pub struct TradeDataCollection {
    slots: Box<[TradeSlot]>,
    clock_config: ClockCorrectionConfig,
}

impl std::fmt::Debug for TradeDataCollection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TradeDataCollection")
            .field("capacity", &self.slots.len())
            .finish()
    }
}

impl TradeDataCollection {
    pub fn new(clock_config: ClockCorrectionConfig) -> Self {
        let mut slots = Vec::with_capacity(MAX_SYMBOLS);
        for _ in 0..MAX_SYMBOLS {
            slots.push(TradeSlot {
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

    pub fn push(&self, id: &SymbolId, mut trade: TradeData) {
        let slot = &self.slots[*id];

        let min_latency_ns = self.clock_config.min_latency_ns();
        if let (Some(exch), Some(recv)) = (trade.exchange_ts_raw, trade.received_ts) {
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
        trade.exchange_ts = trade
            .exchange_ts_raw
            .map(|t| t + chrono::Duration::nanoseconds(offset));

        let ring = slot.ring.get_or_init(|| Box::new(RingBuffer::new()));
        ring.push(trade);
    }

    pub fn latest(&self, id: &SymbolId) -> Option<TradeData> {
        self.slots[*id].ring.get()?.latest()
    }

    pub fn write_count(&self, id: &SymbolId) -> u64 {
        self.slots[*id].ring.get().map(|r| r.write_count()).unwrap_or(0)
    }

    pub fn get_buffer(&self, id: &SymbolId) -> Option<&RingBuffer<TradeData>> {
        self.slots[*id].ring.get().map(|b| b.as_ref())
    }
}

impl DataSink<TradeData> for TradeDataCollection {
    fn push(&self, id: &SymbolId, item: TradeData) {
        TradeDataCollection::push(self, id, item);
    }
}

pub struct AllTradeData {
    pub binance: Arc<TradeDataCollection>,
    pub coinbase: Arc<TradeDataCollection>,
    pub bybit: Arc<TradeDataCollection>,
    pub kraken: Arc<TradeDataCollection>,
    pub lighter: Arc<TradeDataCollection>,
    pub mexc: Arc<TradeDataCollection>,
    pub extended: Arc<TradeDataCollection>,
    pub nado: Arc<TradeDataCollection>,
    pub okx: Arc<TradeDataCollection>,
    pub kucoin: Arc<TradeDataCollection>,
    pub bingx: Arc<TradeDataCollection>,
    pub apex: Arc<TradeDataCollection>,
    pub hyperliquid: Arc<TradeDataCollection>,
    pub aerodrome: Arc<TradeDataCollection>,
    pub uniswap: Arc<TradeDataCollection>,
    pub hibachi: Arc<TradeDataCollection>,
}

impl std::fmt::Debug for AllTradeData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AllTradeData").finish()
    }
}

impl AllTradeData {
    pub fn iter(&self) -> impl Iterator<Item = (Exchange, &Arc<TradeDataCollection>)> {
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
        ]
        .into_iter()
    }

    pub fn get_collection(&self, exchange: &Exchange) -> &Arc<TradeDataCollection> {
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
        }
    }

    pub fn new() -> Self {
        Self::with_clock_correction(ClockCorrectionConfig::default())
    }

    pub fn with_clock_correction(clock_config: ClockCorrectionConfig) -> Self {
        let new_coll = || Arc::new(TradeDataCollection::new(clock_config.clone()));
        Self {
            binance: new_coll(),
            coinbase: new_coll(),
            bybit: new_coll(),
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
        }
    }
}
