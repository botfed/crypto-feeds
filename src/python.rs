use crate::analytics::{Analytics, QuoteSide, RangeStat, SnapshotField};
use crate::app_config::{AppConfig, load_config, load_perp, load_spot};
use crate::market_data::{AllMarketData, Exchange, InstrumentType, MarketDataCollection};
use crate::snapshot::{AllSnapshotData, SnapshotConfig, run_snapshot_task};
use crate::symbol_registry::{SymbolId, REGISTRY};
use chrono::{DateTime, Utc};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::sync::Arc;
use std::sync::Once;
use tokio::runtime::Runtime;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

static INIT_LOGGER: Once = Once::new();

fn parse_exchange(exchange: &str) -> PyResult<Exchange> {
    match exchange.to_lowercase().as_str() {
        "binance" => Ok(Exchange::Binance),
        "coinbase" => Ok(Exchange::Coinbase),
        "bybit" => Ok(Exchange::Bybit),
        "kraken" => Ok(Exchange::Kraken),
        "lighter" => Ok(Exchange::Lighter),
        "mexc" => Ok(Exchange::Mexc),
        "extended" => Ok(Exchange::Extended),
        "nado" => Ok(Exchange::Nado),
        _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "Unknown exchange: {}",
            exchange
        ))),
    }
}

fn parse_field(field: &str) -> PyResult<SnapshotField> {
    SnapshotField::from_str(field).ok_or_else(|| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "Unknown field: {}. Valid: bid, ask, bid_qty, ask_qty, midquote, spread, log_return, mid_high, mid_low, bid_high, ask_low, exchange_lat_ms (elat), receive_lat_ms (rlat)",
            field
        ))
    })
}

/// Initialize Rust logging for the Python module.
/// Must be called explicitly by Python users to enable logging.
///
/// Args:
///     level: Log level ("trace", "debug", "info", "warn", "error"). Defaults to "info".
#[pyfunction]
#[pyo3(signature = (level = "info"))]
fn init_logging(level: &str) -> PyResult<()> {
    INIT_LOGGER.call_once(|| {
        let log_level = match level.to_lowercase().as_str() {
            "trace" => log::LevelFilter::Trace,
            "debug" => log::LevelFilter::Debug,
            "info" => log::LevelFilter::Info,
            "warn" => log::LevelFilter::Warn,
            "error" => log::LevelFilter::Error,
            _ => log::LevelFilter::Info,
        };

        env_logger::Builder::from_default_env()
            .filter_level(log_level)
            .init();
    });

    Ok(())
}

#[pyclass]
pub struct PySymbolRegistry;

#[pymethods]
impl PySymbolRegistry {
    #[new]
    fn new() -> Self {
        Self
    }

    /// Lookup a symbol and return its integer ID.
    fn lookup(&self, symbol: &str, instrument_type: &str) -> PyResult<Option<SymbolId>> {
        let itype = match instrument_type.to_lowercase().as_str() {
            "spot" => InstrumentType::Spot,
            "perp" => InstrumentType::Perp,
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Invalid instrument type: {}. Must be 'spot' or 'perp'",
                    instrument_type
                )))
            }
        };

        Ok(REGISTRY.lookup(symbol, &itype).copied())
    }

    /// Get the canonical symbol name from an integer ID.
    fn get_symbol(&self, symbol_id: SymbolId) -> PyResult<Option<String>> {
        if symbol_id >= crate::symbol_registry::MAX_SYMBOLS {
            return Ok(None);
        }
        Ok(REGISTRY.get_symbol(symbol_id).map(|s| s.to_string()))
    }
}

#[pyclass]
pub struct PyMarketData {
    all_data: Arc<AllMarketData>,
}

#[pymethods]
impl PyMarketData {
    #[new]
    fn new() -> Self {
        Self {
            all_data: Arc::new(AllMarketData::new()),
        }
    }

    fn get_bid(&self, exchange: &str, symbol_id: SymbolId) -> PyResult<Option<f64>> {
        let collection = self.get_collection(exchange)?;
        Ok(collection.latest(&symbol_id).and_then(|md| md.bid))
    }

    fn get_ask(&self, exchange: &str, symbol_id: SymbolId) -> PyResult<Option<f64>> {
        let collection = self.get_collection(exchange)?;
        Ok(collection.latest(&symbol_id).and_then(|md| md.ask))
    }

    fn get_bid_qty(&self, exchange: &str, symbol_id: SymbolId) -> PyResult<Option<f64>> {
        let collection = self.get_collection(exchange)?;
        Ok(collection.latest(&symbol_id).and_then(|md| md.bid_qty))
    }

    fn get_ask_qty(&self, exchange: &str, symbol_id: SymbolId) -> PyResult<Option<f64>> {
        let collection = self.get_collection(exchange)?;
        Ok(collection.latest(&symbol_id).and_then(|md| md.ask_qty))
    }

    fn get_midquote(&self, exchange: &str, symbol_id: SymbolId) -> PyResult<Option<f64>> {
        let collection = self.get_collection(exchange)?;
        Ok(collection.get_midquote(&symbol_id))
    }

    fn get_spread(&self, exchange: &str, symbol_id: SymbolId) -> PyResult<Option<f64>> {
        let collection = self.get_collection(exchange)?;
        if let Some(md) = collection.latest(&symbol_id) {
            if let (Some(bid), Some(ask)) = (md.bid, md.ask) {
                return Ok(Some(ask - bid));
            }
        }
        Ok(None)
    }

    #[pyo3(signature = (symbol_id, max_receive_age_ns=50_000_000, max_exchange_age_ns=150_000_000))]
    fn get_midquote_mean(
        &self,
        symbol_id: SymbolId,
        max_receive_age_ns: Option<i64>,
        max_exchange_age_ns: Option<i64>,
    ) -> PyResult<Option<f64>> {
        let now = Utc::now();
        let quotes: Vec<Option<(f64, Option<DateTime<Utc>>, Option<DateTime<Utc>>)>> = self
            .all_data
            .iter()
            .map(|(_, data)| data.get_midquote_w_timestamps(&symbol_id))
            .collect();

        let (sum, count) = quotes
            .iter()
            .flatten()
            .filter(|(_, received_ts, exchange_ts)| {
                if let Some(max_recv) = max_receive_age_ns {
                    match received_ts {
                        Some(ts) => {
                            let age_ns = (now - *ts).num_nanoseconds().unwrap_or(i64::MAX);
                            if age_ns >= max_recv {
                                return false;
                            }
                        }
                        None => return false,
                    }
                }
                if let Some(max_exch) = max_exchange_age_ns {
                    match exchange_ts {
                        Some(ts) => {
                            let age_ns = (now - *ts).num_nanoseconds().unwrap_or(i64::MAX);
                            if age_ns >= max_exch {
                                return false;
                            }
                        }
                        None => return false,
                    }
                }
                true
            })
            .map(|(val, _, _)| val)
            .fold((0.0, 0), |(sum, count), &val| (sum + val, count + 1));

        if count > 0 {
            Ok(Some(sum / count as f64))
        } else {
            Ok(None)
        }
    }

    fn get_market_data(
        &self,
        exchange: &str,
        symbol_id: SymbolId,
        py: Python,
    ) -> PyResult<Option<PyObject>> {
        let collection = self.get_collection(exchange)?;

        if let Some(md) = collection.latest(&symbol_id) {
            let dict = PyDict::new_bound(py);
            dict.set_item("bid", md.bid)?;
            dict.set_item("ask", md.ask)?;
            dict.set_item("bid_qty", md.bid_qty)?;
            dict.set_item("ask_qty", md.ask_qty)?;
            dict.set_item(
                "exchange_ts",
                md.exchange_ts.map(|ts| ts.timestamp_millis()),
            )?;
            dict.set_item(
                "received_ts",
                md.received_ts.map(|ts| ts.timestamp_millis()),
            )?;
            Ok(Some(dict.into()))
        } else {
            Ok(None)
        }
    }
}

impl PyMarketData {
    fn get_collection(
        &self,
        exchange: &str,
    ) -> PyResult<&Arc<MarketDataCollection>> {
        let ex = parse_exchange(exchange)?;
        Ok(self.all_data.get_collection(&ex))
    }

    fn get_arc(&self) -> Arc<AllMarketData> {
        Arc::clone(&self.all_data)
    }
}

#[pyclass]
pub struct PyAnalytics {
    analytics: Arc<Analytics>,
}

#[pymethods]
impl PyAnalytics {
    fn midquote_twap(
        &self,
        exchange: &str,
        symbol_id: SymbolId,
        n: usize,
    ) -> PyResult<Option<f64>> {
        let ex = parse_exchange(exchange)?;
        Ok(self.analytics.midquote_twap(&ex, symbol_id, n))
    }

    fn mean_spread(
        &self,
        exchange: &str,
        symbol_id: SymbolId,
        n: usize,
    ) -> PyResult<Option<f64>> {
        let ex = parse_exchange(exchange)?;
        Ok(self.analytics.mean_spread(&ex, symbol_id, n))
    }

    fn midquote_stdev(
        &self,
        exchange: &str,
        symbol_id: SymbolId,
        n: usize,
    ) -> PyResult<Option<f64>> {
        let ex = parse_exchange(exchange)?;
        Ok(self.analytics.midquote_stdev(&ex, symbol_id, n))
    }

    fn max_abs_log_return(
        &self,
        exchange: &str,
        symbol_id: SymbolId,
        n: usize,
    ) -> PyResult<Option<f64>> {
        let ex = parse_exchange(exchange)?;
        Ok(self.analytics.max_abs_log_return(&ex, symbol_id, n))
    }

    fn realized_vol(
        &self,
        exchange: &str,
        symbol_id: SymbolId,
        n: usize,
    ) -> PyResult<Option<f64>> {
        let ex = parse_exchange(exchange)?;
        Ok(self.analytics.realized_vol(&ex, symbol_id, n))
    }

    fn log_returns(
        &self,
        py: Python,
        exchange: &str,
        symbol_id: SymbolId,
        n: usize,
    ) -> PyResult<PyObject> {
        let ex = parse_exchange(exchange)?;
        let returns = self.analytics.log_returns(&ex, symbol_id, n);
        Ok(PyList::new_bound(py, &returns).into())
    }

    fn snap_mean(
        &self,
        exchange: &str,
        symbol_id: SymbolId,
        field: &str,
        n: usize,
    ) -> PyResult<Option<f64>> {
        let ex = parse_exchange(exchange)?;
        let f = parse_field(field)?;
        Ok(self.analytics.snap_mean(&ex, symbol_id, f, n))
    }

    fn snap_stdev(
        &self,
        exchange: &str,
        symbol_id: SymbolId,
        field: &str,
        n: usize,
    ) -> PyResult<Option<f64>> {
        let ex = parse_exchange(exchange)?;
        let f = parse_field(field)?;
        Ok(self.analytics.snap_stdev(&ex, symbol_id, f, n))
    }

    fn snap_median(
        &self,
        exchange: &str,
        symbol_id: SymbolId,
        field: &str,
        n: usize,
    ) -> PyResult<Option<f64>> {
        let ex = parse_exchange(exchange)?;
        let f = parse_field(field)?;
        Ok(self.analytics.snap_median(&ex, symbol_id, f, n))
    }

    fn snap_quantile(
        &self,
        exchange: &str,
        symbol_id: SymbolId,
        field: &str,
        n: usize,
        q: f64,
    ) -> PyResult<Option<f64>> {
        if !(0.0..=1.0).contains(&q) {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "q must be between 0.0 and 1.0",
            ));
        }
        let ex = parse_exchange(exchange)?;
        let f = parse_field(field)?;
        Ok(self.analytics.snap_quantile(&ex, symbol_id, f, n, q))
    }

    fn tick_midquote_mean(
        &self,
        exchange: &str,
        symbol_id: SymbolId,
        n: usize,
    ) -> PyResult<Option<f64>> {
        let ex = parse_exchange(exchange)?;
        Ok(self.analytics.tick_midquote_mean(&ex, symbol_id, n))
    }

    /// Simulate quoting at a fixed spread from mid and compute fill rate + markout.
    ///
    /// Args:
    ///     exchange: Exchange name (e.g. "binance")
    ///     symbol_id: Symbol ID
    ///     side: "ask"/"a" or "bid"/"b"
    ///     spread_bps: Distance from mid in basis points
    ///     lookback_snaps: Number of snapshots to look back
    ///
    /// Returns dict with n_fills, n_total, elapsed_secs, mean_markout_bps, stdev_markout_bps or None.
    fn quote_fill_analysis(
        &self,
        exchange: &str,
        symbol_id: SymbolId,
        side: &str,
        spread_bps: f64,
        lookback_snaps: usize,
        py: Python,
    ) -> PyResult<Option<PyObject>> {
        let ex = parse_exchange(exchange)?;
        let qs = QuoteSide::from_str(side).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "Unknown side: {}. Valid: bid/b, ask/a",
                side
            ))
        })?;
        if spread_bps < 0.0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "spread_bps must be >= 0",
            ));
        }
        match self
            .analytics
            .quote_fill_analysis(&ex, symbol_id, qs, spread_bps, lookback_snaps)
        {
            Some(r) => {
                let dict = PyDict::new_bound(py);
                dict.set_item("n_fills", r.n_fills)?;
                dict.set_item("n_total", r.n_total)?;
                dict.set_item("elapsed_secs", r.elapsed_secs)?;
                dict.set_item("mean_markout_bps", r.mean_markout_bps)?;
                dict.set_item("stdev_markout_bps", r.stdev_markout_bps)?;
                Ok(Some(dict.into()))
            }
            None => Ok(None),
        }
    }

    /// Get resampled mid-range in bps per bucket.
    ///
    /// Args:
    ///     exchange: Exchange name
    ///     symbol_id: Symbol ID
    ///     window_snaps: Total snapshots to look back (e.g. 36000 = 1 hour at 100ms)
    ///     bucket_size: Snapshots per bucket (e.g. 10 = 1 second at 100ms)
    ///
    /// Returns list of range-in-bps values, one per bucket.
    fn mid_range_bps_buckets(
        &self,
        py: Python,
        exchange: &str,
        symbol_id: SymbolId,
        window_snaps: usize,
        bucket_size: usize,
    ) -> PyResult<Option<PyObject>> {
        let ex = parse_exchange(exchange)?;
        match self
            .analytics
            .mid_range_bps_buckets(&ex, symbol_id, window_snaps, bucket_size)
        {
            Some(v) => Ok(Some(PyList::new_bound(py, &v).into())),
            None => Ok(None),
        }
    }

    /// Compute a summary statistic over resampled mid-range bps buckets.
    ///
    /// Args:
    ///     exchange: Exchange name
    ///     symbol_id: Symbol ID
    ///     window_snaps: Total snapshots to look back
    ///     bucket_size: Snapshots per bucket
    ///     stat: "mean", "median", or "quantile"
    ///     q: Quantile value (0.0-1.0), only used when stat="quantile"
    ///
    /// Returns single f64 value or None.
    #[pyo3(signature = (exchange, symbol_id, window_snaps, bucket_size, stat="median", q=0.5))]
    fn mid_range_bps_stat(
        &self,
        exchange: &str,
        symbol_id: SymbolId,
        window_snaps: usize,
        bucket_size: usize,
        stat: &str,
        q: f64,
    ) -> PyResult<Option<f64>> {
        let ex = parse_exchange(exchange)?;
        let range_stat = RangeStat::from_str(stat, Some(q)).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "Unknown stat: {}. Valid: mean, median, quantile",
                stat
            ))
        })?;
        if matches!(range_stat, RangeStat::Quantile(_)) && !(0.0..=1.0).contains(&q) {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "q must be between 0.0 and 1.0",
            ));
        }
        Ok(self
            .analytics
            .mid_range_bps_stat(&ex, symbol_id, window_snaps, bucket_size, range_stat))
    }

    fn get_latest_snapshot(
        &self,
        exchange: &str,
        symbol_id: SymbolId,
        py: Python,
    ) -> PyResult<Option<PyObject>> {
        let ex = parse_exchange(exchange)?;
        match self.analytics.get_latest_snapshot(&ex, symbol_id) {
            Some(s) => {
                let dict = PyDict::new_bound(py);
                dict.set_item("bid", s.bid)?;
                dict.set_item("ask", s.ask)?;
                dict.set_item("bid_qty", s.bid_qty)?;
                dict.set_item("ask_qty", s.ask_qty)?;
                dict.set_item("midquote", s.midquote)?;
                dict.set_item("spread", s.spread)?;
                dict.set_item(
                    "log_return",
                    if s.log_return.is_nan() {
                        None
                    } else {
                        Some(s.log_return)
                    },
                )?;
                dict.set_item("mid_high", s.mid_high)?;
                dict.set_item("mid_low", s.mid_low)?;
                dict.set_item("bid_high", s.bid_high)?;
                dict.set_item("ask_low", s.ask_low)?;
                dict.set_item(
                    "exchange_lat_ms",
                    if s.exchange_lat_ms.is_nan() { None } else { Some(s.exchange_lat_ms) },
                )?;
                dict.set_item(
                    "receive_lat_ms",
                    if s.receive_lat_ms.is_nan() { None } else { Some(s.receive_lat_ms) },
                )?;
                dict.set_item("exchange_ts_ns", s.exchange_ts_ns)?;
                dict.set_item("received_ts_ns", s.received_ts_ns)?;
                dict.set_item("snap_ts_ns", s.snap_ts_ns)?;
                Ok(Some(dict.into()))
            }
            None => Ok(None),
        }
    }
}

#[pyclass]
pub struct PyAppConfig {
    config: AppConfig,
}

#[pymethods]
impl PyAppConfig {
    #[staticmethod]
    fn from_file(path: &str) -> PyResult<Self> {
        let config = load_config(path).map_err(|e| {
            pyo3::exceptions::PyIOError::new_err(format!("Failed to load config: {}", e))
        })?;
        Ok(Self { config })
    }

    #[staticmethod]
    fn from_dict(py: Python, dict: &Bound<PyDict>) -> PyResult<Self> {
        let mut spot: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        let mut perp: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();

        if let Ok(Some(spot_dict)) = dict.get_item("spot") {
            let spot_dict: &Bound<PyDict> = spot_dict.downcast()?;
            for (key, value) in spot_dict.iter() {
                let exchange: String = key.extract()?;
                let symbols: Vec<String> = value.extract()?;
                spot.insert(exchange, symbols);
            }
        }

        if let Ok(Some(perp_dict)) = dict.get_item("perp") {
            let perp_dict: &Bound<PyDict> = perp_dict.downcast()?;
            for (key, value) in perp_dict.iter() {
                let exchange: String = key.extract()?;
                let symbols: Vec<String> = value.extract()?;
                perp.insert(exchange, symbols);
            }
        }

        Ok(Self {
            config: AppConfig { spot, perp },
        })
    }

    fn to_dict(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new_bound(py);

        let spot_dict = PyDict::new_bound(py);
        for (exchange, symbols) in &self.config.spot {
            spot_dict.set_item(exchange, symbols.clone())?;
        }
        dict.set_item("spot", spot_dict)?;

        let perp_dict = PyDict::new_bound(py);
        for (exchange, symbols) in &self.config.perp {
            perp_dict.set_item(exchange, symbols.clone())?;
        }
        dict.set_item("perp", perp_dict)?;

        Ok(dict.into())
    }
}

#[pyclass]
pub struct PyFeedManager {
    runtime: Runtime,
    market_data: Py<PyMarketData>,
    shutdown: Arc<Notify>,
    perp_handles: Vec<JoinHandle<()>>,
    spot_handles: Vec<JoinHandle<()>>,
    analytics: Option<Py<PyAnalytics>>,
    snapshot_handle: Option<JoinHandle<()>>,
}

#[pymethods]
impl PyFeedManager {
    #[new]
    fn new(py: Python) -> PyResult<Self> {
        let runtime = Runtime::new().map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to create runtime: {}", e))
        })?;

        let market_data = Py::new(py, PyMarketData::new())?;
        let shutdown = Arc::new(Notify::new());

        Ok(Self {
            runtime,
            market_data,
            shutdown,
            perp_handles: Vec::new(),
            spot_handles: Vec::new(),
            analytics: None,
            snapshot_handle: None,
        })
    }

    fn start_spot_feeds(&mut self, py: Python, config: &PyAppConfig) -> PyResult<()> {
        let market_data_ref = self.market_data.borrow(py);
        let all_data = market_data_ref.get_arc();

        self.runtime
            .block_on(async {
                load_spot(
                    &mut self.spot_handles,
                    &config.config,
                    &all_data,
                    &self.shutdown,
                )
            })
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to start spot feeds: {}",
                    e
                ))
            })?;

        Ok(())
    }

    fn start_perp_feeds(&mut self, py: Python, config: &PyAppConfig) -> PyResult<()> {
        let market_data_ref = self.market_data.borrow(py);
        let all_data = market_data_ref.get_arc();

        self.runtime
            .block_on(async {
                load_perp(&mut self.perp_handles, &config.config, &all_data, &self.shutdown)
            })
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to start perp feeds: {}",
                    e
                ))
            })?;

        Ok(())
    }

    #[pyo3(signature = (interval_ms=100, buffer_capacity=65536))]
    fn start_snapshots(
        &mut self,
        py: Python,
        interval_ms: u64,
        buffer_capacity: usize,
    ) -> PyResult<()> {
        if self.snapshot_handle.is_some() {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Snapshots already started",
            ));
        }

        let market_data_ref = self.market_data.borrow(py);
        let tick_data = market_data_ref.get_arc();

        let snap_data = Arc::new(AllSnapshotData::new(buffer_capacity));
        let analytics = Arc::new(Analytics::new(
            Arc::clone(&tick_data),
            Arc::clone(&snap_data),
        ));

        let config = SnapshotConfig {
            interval_ms,
            buffer_capacity,
        };

        let shutdown = Arc::clone(&self.shutdown);
        let snap_clone = Arc::clone(&snap_data);
        let handle = self
            .runtime
            .spawn(run_snapshot_task(tick_data, snap_clone, config, shutdown));

        self.snapshot_handle = Some(handle);
        self.analytics = Some(Py::new(py, PyAnalytics { analytics })?);

        Ok(())
    }

    fn get_analytics(&self, py: Python) -> PyResult<Option<Py<PyAnalytics>>> {
        Ok(self.analytics.as_ref().map(|a| a.clone_ref(py)))
    }

    fn get_market_data(&self, py: Python) -> PyResult<Py<PyMarketData>> {
        Ok(self.market_data.clone_ref(py))
    }

    fn shutdown(&self) {
        self.shutdown.notify_waiters();
    }
}

#[pymodule]
fn crypto_feeds(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(init_logging, m)?)?;
    m.add_class::<PySymbolRegistry>()?;
    m.add_class::<PyMarketData>()?;
    m.add_class::<PyAppConfig>()?;
    m.add_class::<PyFeedManager>()?;
    m.add_class::<PyAnalytics>()?;
    Ok(())
}
