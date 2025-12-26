use crate::app_config::{AppConfig, load_config, load_perp, load_spot};
use crate::market_data::{AllMarketData, MarketData};
use chrono::{DateTime, Duration, Utc};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Once;
use tokio::runtime::Runtime;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

static INIT_LOGGER: Once = Once::new();

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

    fn get_bid(&self, exchange: &str, symbol: &str) -> PyResult<Option<f64>> {
        let collection = self.get_collection(exchange)?;
        let lock = collection.lock().unwrap();
        Ok(lock.get(symbol).and_then(|md| md.bid))
    }

    fn get_ask(&self, exchange: &str, symbol: &str) -> PyResult<Option<f64>> {
        let collection = self.get_collection(exchange)?;
        let lock = collection.lock().unwrap();
        Ok(lock.get(symbol).and_then(|md| md.ask))
    }

    fn get_bid_qty(&self, exchange: &str, symbol: &str) -> PyResult<Option<f64>> {
        let collection = self.get_collection(exchange)?;
        let lock = collection.lock().unwrap();
        Ok(lock.get(symbol).and_then(|md| md.bid_qty))
    }

    fn get_ask_qty(&self, exchange: &str, symbol: &str) -> PyResult<Option<f64>> {
        let collection = self.get_collection(exchange)?;
        let lock = collection.lock().unwrap();
        Ok(lock.get(symbol).and_then(|md| md.ask_qty))
    }

    fn get_midquote(&self, exchange: &str, symbol: &str) -> PyResult<Option<f64>> {
        let collection = self.get_collection(exchange)?;
        let lock = collection.lock().unwrap();
        Ok(lock.get_midquote(symbol))
    }

    fn get_spread(&self, exchange: &str, symbol: &str) -> PyResult<Option<f64>> {
        let collection = self.get_collection(exchange)?;
        let lock = collection.lock().unwrap();
        if let Some(md) = lock.get(symbol) {
            if let (Some(bid), Some(ask)) = (md.bid, md.ask) {
                return Ok(Some(ask - bid));
            }
        }
        Ok(None)
    }

    fn get_midquote_mean(&self, symbol: &str) -> PyResult<Option<f64>> {
        let threshold = Utc::now() - Duration::seconds(1);
        let quotes: Vec<Option<(f64, DateTime<Utc>)>> = self
            .all_data
            .iter()
            .map(|(_, data)| data.lock().unwrap().get_midquote_w_timestamp(symbol))
            .collect();
        // Efficient - one pass without allocating
        let (sum, count) = quotes
            .iter()
            .flatten()
            .filter(|(_, dt)| dt > &threshold)
            .map(|(val, _)| val)
            .fold((0.0, 0), |(sum, count), &val| (sum + val, count + 1));

        if count > 0 {
            return Ok(Some(sum / count as f64));
        } else {
            return Ok(None); // No values matched the filter
        };
    }

    fn get_all_symbols(&self, exchange: &str) -> PyResult<Vec<String>> {
        let collection = self.get_collection(exchange)?;
        let lock = collection.lock().unwrap();
        Ok(lock.data.keys().cloned().collect())
    }

    fn get_market_data(
        &self,
        exchange: &str,
        symbol: &str,
        py: Python,
    ) -> PyResult<Option<PyObject>> {
        let collection = self.get_collection(exchange)?;
        let lock = collection.lock().unwrap();

        if let Some(md) = lock.get(symbol) {
            let dict = PyDict::new_bound(py);
            dict.set_item("bid", md.bid)?;
            dict.set_item("ask", md.ask)?;
            dict.set_item("bid_qty", md.bid_qty)?;
            dict.set_item("ask_qty", md.ask_qty)?;
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
    ) -> PyResult<&Arc<std::sync::Mutex<crate::market_data::MarketDataCollection>>> {
        match exchange.to_lowercase().as_str() {
            "binance" => Ok(&self.all_data.binance),
            "coinbase" => Ok(&self.all_data.coinbase),
            "bybit" => Ok(&self.all_data.bybit),
            "kraken" => Ok(&self.all_data.kraken),
            "lighter" => Ok(&self.all_data.lighter),
            "mexc" => Ok(&self.all_data.mexc),
            _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Unknown exchange: {}",
                exchange
            ))),
        }
    }

    fn get_arc(&self) -> Arc<AllMarketData> {
        Arc::clone(&self.all_data)
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
        let mut spot: HashMap<String, Vec<String>> = HashMap::new();
        let mut perp: HashMap<String, Vec<String>> = HashMap::new();

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
    m.add_class::<PyMarketData>()?;
    m.add_class::<PyAppConfig>()?;
    m.add_class::<PyFeedManager>()?;
    Ok(())
}
