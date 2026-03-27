use crate::historical_bars::Bar;
use crate::market_data::{AllMarketData, Exchange};
use crate::symbol_registry::SymbolId;
use chrono::Utc;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock, RwLockReadGuard};
use tokio::sync::Notify;
use tokio::time::{self, Duration, MissedTickBehavior};

const ONE_MIN_MS: i64 = 60_000;

// ── BarBuilder ───────────────────────────────────────────────────────

struct BuilderPartial {
    open_time_ms: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
}

pub struct BarBuilder {
    pub(crate) completed: VecDeque<Bar>,
    pub(crate) completed_1m: VecDeque<Bar>,
    partial_1m: Option<BuilderPartial>,
    target_min: usize,
    max_bars: usize,
    max_1m: usize,
}

impl BarBuilder {
    pub fn new(target_min: usize, max_bars: usize) -> Self {
        let max_1m = 2 * target_min + 4;
        Self {
            completed: VecDeque::with_capacity(max_bars + 1),
            completed_1m: VecDeque::with_capacity(max_1m + 1),
            partial_1m: None,
            target_min,
            max_bars,
            max_1m,
        }
    }

    pub fn feed(&mut self, price: f64, ts_ms: i64) {
        let current_1m_start = (ts_ms / ONE_MIN_MS) * ONE_MIN_MS;

        let partial = match self.partial_1m.as_ref() {
            None => {
                self.partial_1m = Some(BuilderPartial {
                    open_time_ms: current_1m_start,
                    open: price,
                    high: price,
                    low: price,
                    close: price,
                });
                return;
            }
            Some(p) => p,
        };

        if current_1m_start > partial.open_time_ms {
            // Freeze partial into a completed 1m bar
            let completed_1m = Bar {
                open_time_ms: partial.open_time_ms,
                open: partial.open,
                high: partial.high,
                low: partial.low,
                close: partial.close,
                volume: 0.0,
            };
            let partial_start = partial.open_time_ms;

            self.completed_1m.push_back(completed_1m);
            while self.completed_1m.len() > self.max_1m {
                self.completed_1m.pop_front();
            }

            // Check target_min boundary
            let target_ms = self.target_min as i64 * ONE_MIN_MS;
            let current_target_start = (ts_ms / target_ms) * target_ms;
            if current_target_start > partial_start
                && (partial_start + ONE_MIN_MS) >= current_target_start
            {
                let n_1m = self.completed_1m.len();
                let take = self.target_min.min(n_1m);
                let slice_start = n_1m - take;
                let slice: Vec<Bar> =
                    self.completed_1m.range(slice_start..).copied().collect();
                if let Some(target_bar) = aggregate_1m_to_target(&slice) {
                    self.completed.push_back(target_bar);
                    while self.completed.len() > self.max_bars {
                        self.completed.pop_front();
                    }
                }
            }

            // Start new partial with current price as OHLC
            self.partial_1m = Some(BuilderPartial {
                open_time_ms: current_1m_start,
                open: price,
                high: price,
                low: price,
                close: price,
            });
        } else {
            // Update partial high/low/close
            let p = self.partial_1m.as_mut().unwrap();
            if price > p.high { p.high = price; }
            if price < p.low { p.low = price; }
            p.close = price;
        }
    }

    pub fn completed(&self) -> &VecDeque<Bar> {
        &self.completed
    }

    pub fn completed_1m(&self) -> &VecDeque<Bar> {
        &self.completed_1m
    }

    pub fn n_completed(&self) -> usize {
        self.completed.len()
    }

    pub fn target_min(&self) -> usize {
        self.target_min
    }

    /// Build the virtual head bar: a sliding window of the last `target_min`
    /// 1-min bars (completed + partial), aggregated into a single OHLC bar.
    ///
    /// `latest_price` extends the partial 1m bar with the most recent BBO mid.
    ///
    /// Falls back to the last completed target_min bar when fewer than
    /// `target_min` 1-min bars are available.
    pub fn virtual_head(&self, latest_price: f64) -> Option<Bar> {
        let partial = self.partial_1m.as_ref()?;

        // Virtual partial 1m with latest tick
        let vp = Bar {
            open_time_ms: partial.open_time_ms,
            open: partial.open,
            high: partial.high.max(latest_price),
            low: if partial.low > 0.0 {
                partial.low.min(latest_price)
            } else {
                latest_price
            },
            close: latest_price,
            volume: 0.0,
        };

        let n_1m = self.completed_1m.len();
        let total_1m = n_1m + 1;

        if total_1m >= self.target_min {
            // Enough 1-min bars — build sliding window aggregate
            let take_1m = self.target_min - 1;
            if take_1m == 0 {
                return Some(vp);
            }
            let start_idx = n_1m - take_1m;
            let first = &self.completed_1m[start_idx];
            let mut agg = Bar {
                open_time_ms: first.open_time_ms,
                open: first.open,
                high: first.high,
                low: first.low,
                close: first.close,
                volume: 0.0,
            };
            for i in (start_idx + 1)..n_1m {
                let b = &self.completed_1m[i];
                if b.high > agg.high { agg.high = b.high; }
                if b.low < agg.low { agg.low = b.low; }
                agg.close = b.close;
            }
            if vp.high > agg.high { agg.high = vp.high; }
            if vp.low < agg.low { agg.low = vp.low; }
            agg.close = vp.close;
            Some(agg)
        } else {
            // Not enough 1-min bars — fall back to last completed target_min bar
            Some(self.completed.back().copied().unwrap_or(vp))
        }
    }
}

// ── Internal state ────────────────────────────────────────────────────

pub(crate) struct SymbolBarState {
    pub builder: BarBuilder,
    pub exchange: Exchange,
    pub symbol_id: SymbolId,
    pub prev_tick_pos: u64,
}

pub(crate) struct BarManagerInner {
    pub symbols: HashMap<String, SymbolBarState>,
    pub target_min: usize,
}

// ── Public types ──────────────────────────────────────────────────────

/// Symbol + venue for bar tracking.
pub struct BarSymbol {
    pub name: String,
    pub exchange: Exchange,
    pub symbol_id: SymbolId,
}

/// Read-only guard into a symbol's bar state.
pub struct BarsView<'a> {
    guard: RwLockReadGuard<'a, BarManagerInner>,
    symbol: String,
}

impl<'a> BarsView<'a> {
    pub fn completed(&self) -> &VecDeque<Bar> {
        self.guard.symbols[&self.symbol].builder.completed()
    }

    pub fn completed_1m(&self) -> &VecDeque<Bar> {
        self.guard.symbols[&self.symbol].builder.completed_1m()
    }

    pub fn n_completed(&self) -> usize {
        self.guard.symbols[&self.symbol].builder.n_completed()
    }

    pub fn target_min(&self) -> usize {
        self.guard.target_min
    }

    pub fn partial_age_secs(&self) -> f64 {
        let state = &self.guard.symbols[&self.symbol];
        match &state.builder.partial_1m {
            Some(p) => {
                (Utc::now().timestamp_millis() - p.open_time_ms) as f64 / 1000.0
            }
            None => 0.0,
        }
    }

    pub fn has_ticks(&self) -> bool {
        self.guard.symbols[&self.symbol].builder.partial_1m.is_some()
    }

    pub fn exchange(&self) -> Exchange {
        self.guard.symbols[&self.symbol].exchange
    }

    pub fn symbol_id(&self) -> SymbolId {
        self.guard.symbols[&self.symbol].symbol_id
    }

    /// Build the virtual head bar: a sliding window of the last `target_min`
    /// 1-min bars (completed + partial), aggregated into a single OHLC bar.
    ///
    /// `latest_mid` extends the partial 1m bar with the most recent BBO mid.
    ///
    /// Falls back to the last completed target_min bar when fewer than
    /// `target_min` 1-min bars are available.
    pub fn virtual_head(&self, latest_mid: f64) -> Option<Bar> {
        self.guard.symbols[&self.symbol].builder.virtual_head(latest_mid)
    }
}

// ── BarManager ────────────────────────────────────────────────────────

pub struct BarManager {
    inner: Arc<RwLock<BarManagerInner>>,
}

impl BarManager {
    pub fn new(symbols: Vec<BarSymbol>, target_min: usize, max_bars: usize) -> Self {
        let mut map = HashMap::new();
        for s in symbols {
            map.insert(
                s.name,
                SymbolBarState {
                    builder: BarBuilder::new(target_min, max_bars),
                    exchange: s.exchange,
                    symbol_id: s.symbol_id,
                    prev_tick_pos: 0,
                },
            );
        }
        Self {
            inner: Arc::new(RwLock::new(BarManagerInner {
                symbols: map,
                target_min,
            })),
        }
    }

    /// Warm up from historical bars.
    pub fn warmup(&self, symbol: &str, target_bars: Vec<Bar>, bars_1m: &[Bar]) {
        let mut inner = self.inner.write().unwrap();
        if let Some(state) = inner.symbols.get_mut(symbol) {
            let max = state.builder.max_bars;
            let start = target_bars.len().saturating_sub(max);
            state.builder.completed.clear();
            for bar in &target_bars[start..] {
                state.builder.completed.push_back(*bar);
            }

            let keep_1m = state.builder.max_1m;
            let start_1m = bars_1m.len().saturating_sub(keep_1m);
            state.builder.completed_1m.clear();
            for bar in &bars_1m[start_1m..] {
                state.builder.completed_1m.push_back(*bar);
            }

            log::info!(
                "bar warmup {}: {} target bars, {} 1m bars",
                symbol,
                state.builder.completed.len(),
                state.builder.completed_1m.len(),
            );
        }
    }

    /// Get a read-only view of a symbol's bars.
    pub fn get_bars(&self, symbol: &str) -> Option<BarsView<'_>> {
        let guard = self.inner.read().ok()?;
        if !guard.symbols.contains_key(symbol) {
            return None;
        }
        Some(BarsView {
            guard,
            symbol: symbol.to_string(),
        })
    }

    pub fn symbols(&self) -> Vec<String> {
        let inner = self.inner.read().unwrap();
        inner.symbols.keys().cloned().collect()
    }

    pub fn target_min(&self) -> usize {
        self.inner.read().unwrap().target_min
    }

    /// Spawn the tick→bar maintenance task.
    pub fn spawn_maintenance(
        &self,
        tick_data: Arc<AllMarketData>,
        shutdown: Arc<Notify>,
    ) -> tokio::task::JoinHandle<()> {
        let inner = Arc::clone(&self.inner);
        tokio::spawn(run_bar_maintenance(inner, tick_data, shutdown))
    }
}

// ── Aggregation helper ────────────────────────────────────────────────

fn aggregate_1m_to_target(bars: &[Bar]) -> Option<Bar> {
    let first = bars.first()?;
    let mut agg = Bar {
        open_time_ms: first.open_time_ms,
        open: first.open,
        high: first.high,
        low: first.low,
        close: first.close,
        volume: first.volume,
    };
    for b in &bars[1..] {
        if b.high > agg.high { agg.high = b.high; }
        if b.low < agg.low { agg.low = b.low; }
        agg.close = b.close;
        agg.volume += b.volume;
    }
    Some(agg)
}

// ── Bar maintenance task ──────────────────────────────────────────────

async fn run_bar_maintenance(
    inner: Arc<RwLock<BarManagerInner>>,
    tick_data: Arc<AllMarketData>,
    shutdown: Arc<Notify>,
) {
    let mut interval = time::interval(Duration::from_millis(100));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = shutdown.notified() => break,
            _ = interval.tick() => {}
        }

        let mut guard = match inner.write() {
            Ok(g) => g,
            Err(_) => continue,
        };

        for state in guard.symbols.values_mut() {
            let coll = tick_data.get_collection(&state.exchange);
            let tick_buf = match coll.get_buffer(&state.symbol_id) {
                Some(b) => b,
                None => continue,
            };
            let cur_pos = tick_buf.write_pos();
            let start_pos = state.prev_tick_pos;
            let count = cur_pos.saturating_sub(start_pos).min(5000);
            for i in 0..count {
                if let Some(md) = tick_buf.read_at(start_pos + i) {
                    if let Some(mid) = md.midquote() {
                        if mid > 0.0 {
                            let ts_ms = md.exchange_ts
                                .map(|t| t.timestamp_millis())
                                .unwrap_or_else(|| Utc::now().timestamp_millis());
                            state.builder.feed(mid, ts_ms);
                        }
                    }
                }
            }
            state.prev_tick_pos = cur_pos;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_bar(open_time_ms: i64, o: f64, h: f64, l: f64, c: f64) -> Bar {
        Bar { open_time_ms, open: o, high: h, low: l, close: c, volume: 1.0 }
    }

    #[test]
    fn test_aggregate_1m_to_target() {
        let bars = vec![
            make_bar(0, 100.0, 102.0, 99.0, 101.0),
            make_bar(60_000, 101.0, 103.0, 100.0, 102.0),
            make_bar(120_000, 102.0, 104.0, 98.0, 99.0),
        ];
        let agg = aggregate_1m_to_target(&bars).unwrap();
        assert_eq!(agg.open, 100.0);
        assert_eq!(agg.high, 104.0);
        assert_eq!(agg.low, 98.0);
        assert_eq!(agg.close, 99.0);
    }

    #[test]
    fn test_bar_builder_feed() {
        let mut builder = BarBuilder::new(3, 10);

        // Feed ticks within minute 0 (0..60_000ms)
        builder.feed(100.0, 0);
        builder.feed(102.0, 10_000);
        builder.feed(99.0, 20_000);
        builder.feed(101.0, 50_000);

        assert_eq!(builder.n_completed(), 0);
        assert_eq!(builder.completed_1m().len(), 0);
        // partial should exist
        assert!(builder.partial_1m.is_some());

        // Cross into minute 1 — freezes minute 0
        builder.feed(103.0, 60_000);
        assert_eq!(builder.completed_1m().len(), 1);
        let bar0 = &builder.completed_1m()[0];
        assert_eq!(bar0.open_time_ms, 0);
        assert_eq!(bar0.open, 100.0);
        assert_eq!(bar0.high, 102.0);
        assert_eq!(bar0.low, 99.0);
        assert_eq!(bar0.close, 101.0);

        // Feed more in minute 1
        builder.feed(105.0, 90_000);

        // Cross into minute 2 — freezes minute 1
        builder.feed(104.0, 120_000);
        assert_eq!(builder.completed_1m().len(), 2);
        let bar1 = &builder.completed_1m()[1];
        assert_eq!(bar1.open, 103.0);
        assert_eq!(bar1.high, 105.0);
        assert_eq!(bar1.low, 103.0);
        assert_eq!(bar1.close, 105.0);

        // Cross into minute 3 — freezes minute 2, and hits target boundary (3 min)
        builder.feed(106.0, 180_000);
        assert_eq!(builder.completed_1m().len(), 3);
        // Should have aggregated a 3-min target bar
        assert_eq!(builder.n_completed(), 1);
        let target = &builder.completed()[0];
        assert_eq!(target.open, 100.0);
        assert_eq!(target.high, 105.0);
        assert_eq!(target.low, 99.0);
        assert_eq!(target.close, 104.0);
    }

    #[test]
    fn test_bar_builder_virtual_head() {
        let mut builder = BarBuilder::new(2, 10);

        // No partial → None
        assert!(builder.virtual_head(100.0).is_none());

        // Start partial
        builder.feed(100.0, 0);
        // With partial but no completed 1m, falls back (no completed target bars either)
        let vh = builder.virtual_head(101.0).unwrap();
        assert_eq!(vh.close, 101.0);

        // Complete minute 0, start minute 1
        builder.feed(102.0, 60_000);
        // Now have 1 completed 1m + 1 partial = 2 = target_min
        let vh = builder.virtual_head(103.0).unwrap();
        assert_eq!(vh.open, 100.0);     // from completed_1m[0]
        assert_eq!(vh.high, 103.0);     // from virtual partial
        assert_eq!(vh.close, 103.0);    // latest_price
    }
}
