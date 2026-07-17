/// Fastest-of-N WebSocket connections with Darwinian selection.
///
/// Wraps N `HftEngine` instances connecting to the same feed URL. Each engine
/// pushes through a `DedupSink` that gates on `MarketData::update_id` — only
/// the first arrival reaches the real sink. Periodically reconnects the worst
/// performer to roll the load-balancer dice for a better path.
///
/// Exposes the same `poll_once()` / `maintain()` API as `HftEngine`.
use crate::hft::engine::{HftEngine, HftEngineConfig};
use crate::hft::HftFeed;
use crate::market_data::{DataSink, MarketData};
use crate::symbol_registry::{MAX_SYMBOLS, SymbolId};
use log::{info, warn};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const MAX_CONNS: usize = 8;

/// Per-symbol cross-connection dedup state + per-connection win tracking.
struct SharedState {
    last_update_id: [AtomicU64; MAX_SYMBOLS],
    wins: [AtomicU64; MAX_CONNS],
}

impl SharedState {
    fn new() -> Self {
        Self {
            last_update_id: std::array::from_fn(|_| AtomicU64::new(0)),
            wins: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

/// Dedup wrapper around a real sink. Forwards only the first arrival of each
/// update_id; tracks which connection won.
pub struct DedupSink<S> {
    conn_idx: usize,
    state: Arc<SharedState>,
    inner: S,
}

impl<S: DataSink<MarketData>> DataSink<MarketData> for DedupSink<S> {
    fn push(&self, id: &SymbolId, item: MarketData) {
        if let Some(uid) = item.update_id {
            // Relaxed: single-threaded poll loop, no cross-thread visibility needed.
            // On x86 this compiles to the same `lock cmpxchg` regardless of ordering.
            let prev = self.state.last_update_id[*id].fetch_max(uid, Ordering::Relaxed);
            if uid <= prev {
                return; // already delivered by a faster connection
            }
            self.state.wins[self.conn_idx].fetch_add(1, Ordering::Relaxed);
        }
        self.inner.push(id, item);
    }
}

pub struct MultiConnConfig {
    /// How often to evaluate connections and potentially cull the worst.
    pub selection_interval: Duration,
    /// Minimum updates before making a selection decision.
    pub min_samples: u64,
    /// Reconnect if win rate is below this fraction of fair share (e.g. 0.1 = 10%).
    pub min_win_fraction: f64,
}

impl Default for MultiConnConfig {
    fn default() -> Self {
        Self {
            selection_interval: Duration::from_secs(30),
            min_samples: 1000,
            min_win_fraction: 0.1,
        }
    }
}

pub struct MultiConnEngine<F: HftFeed<Item = MarketData>, S: DataSink<MarketData>> {
    engines: Vec<HftEngine<F, DedupSink<S>>>,
    state: Arc<SharedState>,
    n_conns: usize,
    multi_config: MultiConnConfig,
    last_selection: Instant,
}

impl<F, S> MultiConnEngine<F, S>
where
    F: HftFeed<Item = MarketData>,
    S: DataSink<MarketData> + Clone,
{
    /// Create a multi-connection engine.
    ///
    /// `feed_factory` is called `n_conns` times to create independent feeds
    /// (each gets its own WS connection, dedup state, etc).
    pub fn new(
        n_conns: usize,
        mut feed_factory: impl FnMut() -> F,
        sink: S,
        engine_config: HftEngineConfig,
        multi_config: MultiConnConfig,
    ) -> std::io::Result<Self> {
        assert!(n_conns > 0 && n_conns <= MAX_CONNS);

        let state = Arc::new(SharedState::new());

        let engines = (0..n_conns)
            .map(|idx| {
                let feed = feed_factory();
                let dedup_sink = DedupSink {
                    conn_idx: idx,
                    state: state.clone(),
                    inner: sink.clone(),
                };
                HftEngine::new(feed, dedup_sink, engine_config.clone())
            })
            .collect::<Result<Vec<_>, _>>()?;

        info!("multi-conn engine: {} connections", n_conns);

        Ok(Self {
            engines,
            state,
            n_conns,
            multi_config,
            last_selection: Instant::now(),
        })
    }

    /// Poll all connections. Zero-alloc hot path.
    pub fn poll_once(&mut self) -> usize {
        let mut total = 0;
        for engine in &mut self.engines {
            total += engine.poll_once();
        }
        total
    }

    /// Periodic maintenance: heartbeats, reconnects, and Darwinian selection.
    pub fn maintain(&mut self) {
        for engine in &mut self.engines {
            engine.maintain();
        }

        if self.last_selection.elapsed() >= self.multi_config.selection_interval {
            self.darwinian_selection();
            self.last_selection = Instant::now();
        }
    }

    fn darwinian_selection(&mut self) {
        // Snapshot and reset win counts atomically
        let wins: Vec<u64> = self.state.wins[..self.n_conns]
            .iter()
            .map(|w| w.swap(0, Ordering::Relaxed))
            .collect();
        let total: u64 = wins.iter().sum();

        if total < self.multi_config.min_samples {
            return; // not enough data to judge
        }

        let fair_share = total as f64 / self.n_conns as f64;
        let threshold = fair_share * self.multi_config.min_win_fraction;

        // Find worst performer
        let (worst_idx, &worst_wins) = wins
            .iter()
            .enumerate()
            .min_by_key(|(_, w)| *w)
            .unwrap();

        // Log stats
        let pcts: Vec<String> = wins
            .iter()
            .enumerate()
            .map(|(i, w)| format!("c{}={:.1}%", i, *w as f64 / total as f64 * 100.0))
            .collect();
        debug!("multi-conn wins: {} (total={})", pcts.join(" "), total);

        if (worst_wins as f64) < threshold {
            warn!(
                "multi-conn: culling conn {} ({:.1}% wins, threshold={:.1}%)",
                worst_idx,
                worst_wins as f64 / total as f64 * 100.0,
                self.multi_config.min_win_fraction / self.n_conns as f64 * 100.0,
            );
            self.engines[worst_idx].force_reconnect_all();
        }
    }
}
