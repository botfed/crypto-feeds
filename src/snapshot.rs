use crate::market_data::{AllMarketData, Exchange};
use crate::ring_buffer::RingBuffer;
use crate::symbol_registry::{MAX_SYMBOLS, SymbolId};
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::sync::Notify;
use tokio::time::{self, MissedTickBehavior};

#[derive(Debug, Default, Copy, Clone)]
#[repr(C)]
pub struct SnapshotData {
    pub bid: f64,
    pub ask: f64,
    pub bid_qty: f64,
    pub ask_qty: f64,
    pub midquote: f64,
    pub spread: f64,
    pub log_return: f64,
    pub mid_high: f64,
    pub mid_low: f64,
    pub bid_high: f64,
    pub ask_low: f64,
    pub exchange_lat_ms: f64,
    pub receive_lat_ms: f64,
    pub exchange_ts_ns: i64,
    pub received_ts_ns: i64,
    pub snap_ts_ns: i64,
}

pub struct SnapshotConfig {
    pub interval_ms: u64,
    pub buffer_capacity: usize,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            interval_ms: 100,
            buffer_capacity: 8192,
        }
    }
}

pub struct SnapshotCollection {
    buffers: Box<[OnceLock<Box<RingBuffer<SnapshotData>>>]>,
    buffer_capacity: usize,
}

impl std::fmt::Debug for SnapshotCollection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotCollection")
            .field("capacity", &self.buffers.len())
            .finish()
    }
}

impl SnapshotCollection {
    pub fn new(buffer_capacity: usize) -> Self {
        let mut buffers = Vec::with_capacity(MAX_SYMBOLS);
        for _ in 0..MAX_SYMBOLS {
            buffers.push(OnceLock::new());
        }
        Self {
            buffers: buffers.into_boxed_slice(),
            buffer_capacity,
        }
    }

    pub fn push(&self, id: &SymbolId, snap: SnapshotData) {
        let ring = self.buffers[*id]
            .get_or_init(|| Box::new(RingBuffer::with_capacity(self.buffer_capacity)));
        ring.push(snap);
    }

    pub fn latest(&self, id: &SymbolId) -> Option<SnapshotData> {
        self.buffers[*id].get()?.latest()
    }

    pub fn get_buffer(&self, id: &SymbolId) -> Option<&RingBuffer<SnapshotData>> {
        self.buffers[*id].get().map(|b| b.as_ref())
    }
}

pub struct AllSnapshotData {
    pub binance: Arc<SnapshotCollection>,
    pub coinbase: Arc<SnapshotCollection>,
    pub bybit: Arc<SnapshotCollection>,
    pub kraken: Arc<SnapshotCollection>,
    pub lighter: Arc<SnapshotCollection>,
    pub mexc: Arc<SnapshotCollection>,
    pub extended: Arc<SnapshotCollection>,
}

impl std::fmt::Debug for AllSnapshotData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AllSnapshotData").finish()
    }
}

impl AllSnapshotData {
    pub fn new(buffer_capacity: usize) -> Self {
        let new_coll = || Arc::new(SnapshotCollection::new(buffer_capacity));
        Self {
            binance: new_coll(),
            coinbase: new_coll(),
            bybit: new_coll(),
            kraken: new_coll(),
            lighter: new_coll(),
            mexc: new_coll(),
            extended: new_coll(),
        }
    }

    pub fn get_collection(&self, exchange: &Exchange) -> &Arc<SnapshotCollection> {
        match exchange {
            Exchange::Binance => &self.binance,
            Exchange::Coinbase => &self.coinbase,
            Exchange::Bybit => &self.bybit,
            Exchange::Kraken => &self.kraken,
            Exchange::Lighter => &self.lighter,
            Exchange::Mexc => &self.mexc,
            Exchange::Extended => &self.extended,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (Exchange, &Arc<SnapshotCollection>)> {
        use Exchange::*;
        [
            (Binance, &self.binance),
            (Coinbase, &self.coinbase),
            (Bybit, &self.bybit),
            (Kraken, &self.kraken),
            (Lighter, &self.lighter),
            (Mexc, &self.mexc),
            (Extended, &self.extended),
        ]
        .into_iter()
    }
}

const NUM_EXCHANGES: usize = 7;

fn exchange_index(exchange: &Exchange) -> usize {
    match exchange {
        Exchange::Binance => 0,
        Exchange::Coinbase => 1,
        Exchange::Bybit => 2,
        Exchange::Kraken => 3,
        Exchange::Lighter => 4,
        Exchange::Mexc => 5,
        Exchange::Extended => 6,
    }
}

pub async fn run_snapshot_task(
    tick_data: Arc<AllMarketData>,
    snap_data: Arc<AllSnapshotData>,
    config: SnapshotConfig,
    shutdown: Arc<Notify>,
) {
    let mut interval = time::interval(std::time::Duration::from_millis(config.interval_ms));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let mut prev_mid = vec![f64::NAN; NUM_EXCHANGES * MAX_SYMBOLS];
    let mut prev_tick_pos = vec![0u64; NUM_EXCHANGES * MAX_SYMBOLS];

    // Create the shutdown future once so we don't miss notifications
    // between loop iterations
    let shutdown_fut = shutdown.notified();
    tokio::pin!(shutdown_fut);

    loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = &mut shutdown_fut => {
                log::info!("Snapshot task shutting down");
                return;
            }
        }

        let snap_ts_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        for (exchange, tick_coll) in tick_data.iter() {
            let ex_idx = exchange_index(&exchange);
            let snap_coll = snap_data.get_collection(&exchange);

            for sym_id in 0..MAX_SYMBOLS {
                let md = match tick_coll.latest(&sym_id) {
                    Some(md) => md,
                    None => continue,
                };

                let (bid, ask) = match (md.bid, md.ask) {
                    (Some(b), Some(a)) => (b, a),
                    _ => continue,
                };

                let midquote = (bid + ask) / 2.0;
                let spread = ask - bid;

                // Scan raw ticks since last snapshot for min/max midquote
                let prev_idx = ex_idx * MAX_SYMBOLS + sym_id;
                let (mid_high, mid_low, bid_high, ask_low) = if let Some(tick_buf) = tick_coll.get_buffer(&sym_id) {
                    let cur_pos = tick_buf.write_pos();
                    let start_pos = prev_tick_pos[prev_idx];
                    let mut m_hi = midquote;
                    let mut m_lo = midquote;
                    let mut b_hi = bid;
                    let mut a_lo = ask;
                    // Walk ticks from prev snapshot pos to current pos
                    // Cap at 1000 to bound work if something is weird
                    let count = (cur_pos.saturating_sub(start_pos)).min(1000);
                    for i in 0..count {
                        if let Some(tick) = tick_buf.read_at(start_pos + i) {
                            if let (Some(b), Some(a)) = (tick.bid, tick.ask) {
                                let m = (b + a) / 2.0;
                                if m > m_hi { m_hi = m; }
                                if m < m_lo { m_lo = m; }
                                if b > b_hi { b_hi = b; }
                                if a < a_lo { a_lo = a; }
                            }
                        }
                    }
                    prev_tick_pos[prev_idx] = cur_pos;
                    (m_hi, m_lo, b_hi, a_lo)
                } else {
                    (midquote, midquote, bid, ask)
                };

                let log_return = if prev_mid[prev_idx].is_nan() {
                    f64::NAN
                } else {
                    (midquote / prev_mid[prev_idx]).ln()
                };
                prev_mid[prev_idx] = midquote;

                let exchange_ts_ns = md
                    .exchange_ts
                    .and_then(|ts| ts.timestamp_nanos_opt())
                    .unwrap_or(0);
                let received_ts_ns = md
                    .received_ts
                    .and_then(|ts| ts.timestamp_nanos_opt())
                    .unwrap_or(0);

                let exchange_lat_ms = if exchange_ts_ns > 0 && received_ts_ns > 0 {
                    (received_ts_ns - exchange_ts_ns) as f64 / 1_000_000.0
                } else {
                    f64::NAN
                };
                let receive_lat_ms = if received_ts_ns > 0 {
                    (snap_ts_ns - received_ts_ns) as f64 / 1_000_000.0
                } else {
                    f64::NAN
                };

                snap_coll.push(
                    &sym_id,
                    SnapshotData {
                        bid,
                        ask,
                        bid_qty: md.bid_qty.unwrap_or(f64::NAN),
                        ask_qty: md.ask_qty.unwrap_or(f64::NAN),
                        midquote,
                        spread,
                        log_return,
                        mid_high,
                        mid_low,
                        bid_high,
                        ask_low,
                        exchange_lat_ms,
                        receive_lat_ms,
                        exchange_ts_ns,
                        received_ts_ns,
                        snap_ts_ns,
                    },
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market_data::MarketData;
    use chrono::Utc;

    #[test]
    fn test_snapshot_push_and_read() {
        let coll = SnapshotCollection::new(1024);
        let sym: SymbolId = 0;

        let snap = SnapshotData {
            bid: 100.0,
            ask: 100.5,
            bid_qty: 1.0,
            ask_qty: 2.0,
            midquote: 100.25,
            spread: 0.5,
            log_return: f64::NAN,
            mid_high: 100.30,
            mid_low: 100.20,
            bid_high: 100.05,
            ask_low: 100.45,
            exchange_lat_ms: f64::NAN,
            receive_lat_ms: f64::NAN,
            exchange_ts_ns: 0,
            received_ts_ns: 0,
            snap_ts_ns: 1000,
        };

        coll.push(&sym, snap);
        let read = coll.latest(&sym).unwrap();
        assert!((read.midquote - 100.25).abs() < f64::EPSILON);
        assert!((read.spread - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_snapshot_read_last_n() {
        let coll = SnapshotCollection::new(1024);
        let sym: SymbolId = 0;

        for i in 0..10u64 {
            coll.push(
                &sym,
                SnapshotData {
                    midquote: i as f64,
                    ..Default::default()
                },
            );
        }

        let buf_ref = coll.get_buffer(&sym).unwrap();
        let mut buf = [SnapshotData::default(); 5];
        let count = buf_ref.read_last_n(5, &mut buf);
        assert_eq!(count, 5);
        assert!((buf[0].midquote - 9.0).abs() < f64::EPSILON);
        assert!((buf[4].midquote - 5.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_snapshot_task_integration() {
        let tick_data = Arc::new(AllMarketData::new());
        let snap_data = Arc::new(AllSnapshotData::new(1024));
        let shutdown = Arc::new(Notify::new());

        // Push some ticks
        let sym: SymbolId = 0;
        tick_data.binance.push(
            &sym,
            MarketData {
                bid: Some(100.0),
                ask: Some(101.0),
                bid_qty: Some(1.0),
                ask_qty: Some(2.0),
                exchange_ts: Some(Utc::now()),
                received_ts: Some(Utc::now()),
            },
        );

        let config = SnapshotConfig {
            interval_ms: 10,
            buffer_capacity: 1024,
        };

        let snap_clone = Arc::clone(&snap_data);
        let shutdown_clone = Arc::clone(&shutdown);
        let handle = tokio::spawn(run_snapshot_task(
            Arc::clone(&tick_data),
            snap_clone,
            config,
            shutdown_clone,
        ));

        // Let a few snapshots fire
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        shutdown.notify_waiters();
        handle.await.unwrap();

        let snap = snap_data.binance.latest(&sym);
        assert!(snap.is_some());
        let s = snap.unwrap();
        assert!((s.bid - 100.0).abs() < f64::EPSILON);
        assert!((s.ask - 101.0).abs() < f64::EPSILON);
        assert!((s.midquote - 100.5).abs() < f64::EPSILON);
        assert!((s.spread - 1.0).abs() < f64::EPSILON);
    }
}
