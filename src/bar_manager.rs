use crate::historical_bars::Bar;
use crate::market_data::{AllMarketData, Exchange};
use crate::symbol_registry::SymbolId;
use chrono::Utc;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock, RwLockReadGuard};
use tokio::sync::Notify;
use tokio::time::{self, Duration, MissedTickBehavior};

const ONE_MIN_MS: i64 = 60_000;

// ── Internal state ────────────────────────────────────────────────────

struct Partial1m {
    open_time_ms: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    prev_tick_pos: u64,
    n_ticks: u64,
}

pub(crate) struct SymbolBarState {
    pub completed: VecDeque<Bar>,
    pub completed_1m: VecDeque<Bar>,
    partial_1m: Option<Partial1m>,
    pub exchange: Exchange,
    pub symbol_id: SymbolId,
    max_bars: usize,
    max_1m: usize,
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
        &self.guard.symbols[&self.symbol].completed
    }

    pub fn completed_1m(&self) -> &VecDeque<Bar> {
        &self.guard.symbols[&self.symbol].completed_1m
    }

    pub fn n_completed(&self) -> usize {
        self.guard.symbols[&self.symbol].completed.len()
    }

    pub fn target_min(&self) -> usize {
        self.guard.target_min
    }

    pub fn partial_age_secs(&self) -> f64 {
        let state = &self.guard.symbols[&self.symbol];
        match &state.partial_1m {
            Some(p) => {
                (Utc::now().timestamp_millis() - p.open_time_ms) as f64 / 1000.0
            }
            None => 0.0,
        }
    }

    pub fn has_ticks(&self) -> bool {
        self.guard.symbols[&self.symbol].partial_1m.is_some()
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
        let state = &self.guard.symbols[&self.symbol];
        let target_min = self.guard.target_min;
        let partial = state.partial_1m.as_ref()?;

        // Virtual partial 1m with latest tick
        let vp = Bar {
            open_time_ms: partial.open_time_ms,
            open: partial.open,
            high: partial.high.max(latest_mid),
            low: if partial.low > 0.0 {
                partial.low.min(latest_mid)
            } else {
                latest_mid
            },
            close: latest_mid,
            volume: 0.0,
        };

        let n_1m = state.completed_1m.len();
        let total_1m = n_1m + 1;

        if total_1m >= target_min {
            // Enough 1-min bars — build sliding window aggregate
            let take_1m = target_min - 1;
            let start_idx = n_1m - take_1m;
            let first = &state.completed_1m[start_idx];
            let mut agg = Bar {
                open_time_ms: first.open_time_ms,
                open: first.open,
                high: first.high,
                low: first.low,
                close: first.close,
                volume: 0.0,
            };
            for i in (start_idx + 1)..n_1m {
                let b = &state.completed_1m[i];
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
            Some(state.completed.back().copied().unwrap_or(vp))
        }
    }
}

// ── BarManager ────────────────────────────────────────────────────────

pub struct BarManager {
    inner: Arc<RwLock<BarManagerInner>>,
}

impl BarManager {
    pub fn new(symbols: Vec<BarSymbol>, target_min: usize, max_bars: usize) -> Self {
        let max_1m = 2 * target_min + 4;
        let mut map = HashMap::new();
        for s in symbols {
            map.insert(
                s.name,
                SymbolBarState {
                    completed: VecDeque::with_capacity(max_bars + 1),
                    completed_1m: VecDeque::with_capacity(max_1m + 1),
                    partial_1m: None,
                    exchange: s.exchange,
                    symbol_id: s.symbol_id,
                    max_bars,
                    max_1m,
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
            let max = state.max_bars;
            let start = target_bars.len().saturating_sub(max);
            state.completed.clear();
            for bar in &target_bars[start..] {
                state.completed.push_back(*bar);
            }

            let keep_1m = state.max_1m;
            let start_1m = bars_1m.len().saturating_sub(keep_1m);
            state.completed_1m.clear();
            for bar in &bars_1m[start_1m..] {
                state.completed_1m.push_back(*bar);
            }

            log::info!(
                "bar warmup {}: {} target bars, {} 1m bars",
                symbol,
                state.completed.len(),
                state.completed_1m.len(),
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

        let now_ms = Utc::now().timestamp_millis();
        let mut guard = match inner.write() {
            Ok(g) => g,
            Err(_) => continue,
        };

        let target_min = guard.target_min;
        let target_ms = target_min as i64 * ONE_MIN_MS;

        for state in guard.symbols.values_mut() {
            let coll = tick_data.get_collection(&state.exchange);
            let tick_buf = match coll.get_buffer(&state.symbol_id) {
                Some(b) => b,
                None => continue,
            };
            let cur_pos = tick_buf.write_pos();

            // Initialize partial_1m if needed
            if state.partial_1m.is_none() {
                let bar_start = (now_ms / ONE_MIN_MS) * ONE_MIN_MS;
                if let Some(md) = tick_buf.latest() {
                    if let Some(mid) = md.midquote() {
                        state.partial_1m = Some(Partial1m {
                            open_time_ms: bar_start,
                            open: mid,
                            high: mid,
                            low: mid,
                            close: mid,
                            prev_tick_pos: cur_pos,
                            n_ticks: 1,
                        });
                    }
                }
                continue;
            }

            // Check 1-min boundary
            let current_1m_start = (now_ms / ONE_MIN_MS) * ONE_MIN_MS;
            let partial_start = state.partial_1m.as_ref().unwrap().open_time_ms;

            if current_1m_start > partial_start {
                let p = state.partial_1m.as_ref().unwrap();
                let completed_1m = Bar {
                    open_time_ms: p.open_time_ms,
                    open: p.open,
                    high: p.high,
                    low: p.low,
                    close: p.close,
                    volume: 0.0,
                };
                let prev_close = p.close;

                state.completed_1m.push_back(completed_1m);
                while state.completed_1m.len() > state.max_1m {
                    state.completed_1m.pop_front();
                }

                // Check target_min boundary: did this 1-min bar end at or past a target boundary?
                let current_target_start = (now_ms / target_ms) * target_ms;
                if current_target_start > partial_start
                    && (partial_start + ONE_MIN_MS) >= current_target_start
                {
                    let n_1m = state.completed_1m.len();
                    let take = target_min.min(n_1m);
                    let slice_start = n_1m - take;
                    let slice: Vec<Bar> =
                        state.completed_1m.range(slice_start..).copied().collect();
                    if let Some(target_bar) = aggregate_1m_to_target(&slice) {
                        state.completed.push_back(target_bar);
                        while state.completed.len() > state.max_bars {
                            state.completed.pop_front();
                        }
                    }
                }

                // Start new partial_1m
                let mid = tick_buf
                    .latest()
                    .and_then(|md| md.midquote())
                    .unwrap_or(prev_close);
                state.partial_1m = Some(Partial1m {
                    open_time_ms: current_1m_start,
                    open: mid,
                    high: mid,
                    low: mid,
                    close: mid,
                    prev_tick_pos: cur_pos,
                    n_ticks: 1,
                });
                continue;
            }

            // Scan new ticks into partial_1m
            let partial = state.partial_1m.as_mut().unwrap();
            let start_pos = partial.prev_tick_pos;
            let count = cur_pos.saturating_sub(start_pos).min(5000);
            for i in 0..count {
                if let Some(md) = tick_buf.read_at(start_pos + i) {
                    if let Some(mid) = md.midquote() {
                        if mid > 0.0 {
                            if mid > partial.high { partial.high = mid; }
                            if mid < partial.low { partial.low = mid; }
                            partial.close = mid;
                            partial.n_ticks += 1;
                        }
                    }
                }
            }
            partial.prev_tick_pos = cur_pos;
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
}
