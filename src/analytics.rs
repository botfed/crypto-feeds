use crate::market_data::{AllMarketData, Exchange, MarketData};
use crate::snapshot::{AllSnapshotData, SnapshotData};
use crate::symbol_registry::SymbolId;
use std::sync::Arc;

#[derive(Debug, Clone, Copy)]
pub enum SnapshotField {
    Bid,
    Ask,
    BidQty,
    AskQty,
    Midquote,
    Spread,
    LogReturn,
}

impl SnapshotField {
    pub fn extract(&self, snap: &SnapshotData) -> f64 {
        match self {
            Self::Bid => snap.bid,
            Self::Ask => snap.ask,
            Self::BidQty => snap.bid_qty,
            Self::AskQty => snap.ask_qty,
            Self::Midquote => snap.midquote,
            Self::Spread => snap.spread,
            Self::LogReturn => snap.log_return,
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "bid" => Some(Self::Bid),
            "ask" => Some(Self::Ask),
            "bid_qty" | "bidqty" => Some(Self::BidQty),
            "ask_qty" | "askqty" => Some(Self::AskQty),
            "midquote" | "mid" => Some(Self::Midquote),
            "spread" => Some(Self::Spread),
            "log_return" | "logreturn" => Some(Self::LogReturn),
            _ => None,
        }
    }
}

pub struct Analytics {
    tick_data: Arc<AllMarketData>,
    snap_data: Arc<AllSnapshotData>,
}

impl Analytics {
    pub fn new(tick_data: Arc<AllMarketData>, snap_data: Arc<AllSnapshotData>) -> Self {
        Self {
            tick_data,
            snap_data,
        }
    }

    // -- snapshot-based helpers --

    fn read_snap_field(
        &self,
        exchange: &Exchange,
        symbol_id: SymbolId,
        field: SnapshotField,
        n: usize,
    ) -> Option<Vec<f64>> {
        let coll = self.snap_data.get_collection(exchange);
        let buf = coll.get_buffer(&symbol_id)?;
        let mut raw = vec![SnapshotData::default(); n];
        let count = buf.read_last_n(n, &mut raw);
        if count == 0 {
            return None;
        }
        let values: Vec<f64> = raw[..count]
            .iter()
            .map(|s| field.extract(s))
            .filter(|v| !v.is_nan())
            .collect();
        if values.is_empty() {
            None
        } else {
            Some(values)
        }
    }

    // -- snapshot-based analytics --

    pub fn snap_mean(
        &self,
        exchange: &Exchange,
        symbol_id: SymbolId,
        field: SnapshotField,
        n: usize,
    ) -> Option<f64> {
        let vals = self.read_snap_field(exchange, symbol_id, field, n)?;
        Some(mean(&vals))
    }

    pub fn snap_stdev(
        &self,
        exchange: &Exchange,
        symbol_id: SymbolId,
        field: SnapshotField,
        n: usize,
    ) -> Option<f64> {
        let vals = self.read_snap_field(exchange, symbol_id, field, n)?;
        stdev(&vals)
    }

    pub fn snap_median(
        &self,
        exchange: &Exchange,
        symbol_id: SymbolId,
        field: SnapshotField,
        n: usize,
    ) -> Option<f64> {
        let vals = self.read_snap_field(exchange, symbol_id, field, n)?;
        Some(median(&vals))
    }

    // -- convenience methods --

    pub fn midquote_twap(
        &self,
        exchange: &Exchange,
        symbol_id: SymbolId,
        n: usize,
    ) -> Option<f64> {
        self.snap_mean(exchange, symbol_id, SnapshotField::Midquote, n)
    }

    pub fn mean_spread(
        &self,
        exchange: &Exchange,
        symbol_id: SymbolId,
        n: usize,
    ) -> Option<f64> {
        self.snap_mean(exchange, symbol_id, SnapshotField::Spread, n)
    }

    pub fn midquote_stdev(
        &self,
        exchange: &Exchange,
        symbol_id: SymbolId,
        n: usize,
    ) -> Option<f64> {
        self.snap_stdev(exchange, symbol_id, SnapshotField::Midquote, n)
    }

    pub fn realized_vol(
        &self,
        exchange: &Exchange,
        symbol_id: SymbolId,
        n: usize,
    ) -> Option<f64> {
        self.snap_stdev(exchange, symbol_id, SnapshotField::LogReturn, n)
    }

    pub fn log_returns(
        &self,
        exchange: &Exchange,
        symbol_id: SymbolId,
        n: usize,
    ) -> Vec<f64> {
        self.read_snap_field(exchange, symbol_id, SnapshotField::LogReturn, n)
            .unwrap_or_default()
    }

    // -- tick-based analytics --

    pub fn tick_midquote_mean(
        &self,
        exchange: &Exchange,
        symbol_id: SymbolId,
        n: usize,
    ) -> Option<f64> {
        let coll = self.tick_data.get_collection(exchange);
        let buf = coll.get_buffer(&symbol_id)?;
        let mut raw = vec![MarketData::default(); n];
        let count = buf.read_last_n(n, &mut raw);
        if count == 0 {
            return None;
        }
        let mids: Vec<f64> = raw[..count]
            .iter()
            .filter_map(|md| md.midquote())
            .collect();
        if mids.is_empty() {
            None
        } else {
            Some(mean(&mids))
        }
    }

    pub fn get_latest_snapshot(
        &self,
        exchange: &Exchange,
        symbol_id: SymbolId,
    ) -> Option<SnapshotData> {
        self.snap_data.get_collection(exchange).latest(&symbol_id)
    }
}

// -- stateless math helpers --

fn mean(vals: &[f64]) -> f64 {
    vals.iter().sum::<f64>() / vals.len() as f64
}

fn stdev(vals: &[f64]) -> Option<f64> {
    if vals.len() < 2 {
        return None;
    }
    let m = mean(vals);
    let variance = vals.iter().map(|v| (v - m).powi(2)).sum::<f64>() / (vals.len() - 1) as f64;
    Some(variance.sqrt())
}

fn median(vals: &[f64]) -> f64 {
    let mut sorted = vals.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = sorted.len();
    if n % 2 == 0 {
        (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
    } else {
        sorted[n / 2]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market_data::AllMarketData;
    use crate::snapshot::{AllSnapshotData, SnapshotData};

    fn make_analytics(snaps: &[(SymbolId, Vec<SnapshotData>)]) -> Analytics {
        let tick_data = Arc::new(AllMarketData::new());
        let snap_data = Arc::new(AllSnapshotData::new(1024));

        for (sym_id, snapshots) in snaps {
            for s in snapshots {
                snap_data.binance.push(sym_id, *s);
            }
        }

        Analytics::new(tick_data, snap_data)
    }

    fn snap_with_mid(mid: f64) -> SnapshotData {
        SnapshotData {
            bid: mid - 0.5,
            ask: mid + 0.5,
            bid_qty: 1.0,
            ask_qty: 1.0,
            midquote: mid,
            spread: 1.0,
            log_return: f64::NAN,
            exchange_ts_ns: 0,
            received_ts_ns: 0,
            snap_ts_ns: 0,
        }
    }

    #[test]
    fn test_mean() {
        let vals = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        assert!((mean(&vals) - 3.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_stdev() {
        let vals = vec![2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let s = stdev(&vals).unwrap();
        // sample stdev (n-1): variance = 32/7 ≈ 4.571, stdev ≈ 2.138
        assert!((s - 2.138).abs() < 0.01);
    }

    #[test]
    fn test_median_odd() {
        let vals = vec![1.0, 3.0, 2.0];
        assert!((median(&vals) - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_median_even() {
        let vals = vec![1.0, 2.0, 3.0, 4.0];
        assert!((median(&vals) - 2.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_snap_mean_midquote() {
        let snaps: Vec<SnapshotData> = (0..10).map(|i| snap_with_mid(100.0 + i as f64)).collect();
        let analytics = make_analytics(&[(0, snaps)]);

        let twap = analytics
            .midquote_twap(&Exchange::Binance, 0, 10)
            .unwrap();
        assert!((twap - 104.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_mean_spread() {
        let snaps: Vec<SnapshotData> = (0..5).map(|_| snap_with_mid(100.0)).collect();
        let analytics = make_analytics(&[(0, snaps)]);

        let spread = analytics
            .mean_spread(&Exchange::Binance, 0, 5)
            .unwrap();
        assert!((spread - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_realized_vol() {
        let snaps: Vec<SnapshotData> = vec![
            SnapshotData {
                log_return: 0.01,
                midquote: 100.0,
                ..Default::default()
            },
            SnapshotData {
                log_return: -0.01,
                midquote: 99.0,
                ..Default::default()
            },
            SnapshotData {
                log_return: 0.02,
                midquote: 101.0,
                ..Default::default()
            },
            SnapshotData {
                log_return: -0.02,
                midquote: 99.0,
                ..Default::default()
            },
        ];
        let analytics = make_analytics(&[(0, snaps)]);
        let vol = analytics.realized_vol(&Exchange::Binance, 0, 10);
        assert!(vol.is_some());
        assert!(vol.unwrap() > 0.0);
    }

    #[test]
    fn test_log_returns() {
        let snaps: Vec<SnapshotData> = vec![
            SnapshotData {
                log_return: 0.01,
                ..Default::default()
            },
            SnapshotData {
                log_return: f64::NAN,
                ..Default::default()
            },
            SnapshotData {
                log_return: -0.01,
                ..Default::default()
            },
        ];
        let analytics = make_analytics(&[(0, snaps)]);
        let returns = analytics.log_returns(&Exchange::Binance, 0, 10);
        // NAN should be filtered out
        assert_eq!(returns.len(), 2);
    }

    #[test]
    fn test_empty_returns_none() {
        let analytics = make_analytics(&[]);
        assert!(analytics.midquote_twap(&Exchange::Binance, 0, 10).is_none());
        assert!(analytics.realized_vol(&Exchange::Binance, 0, 10).is_none());
    }

    #[test]
    fn test_tick_midquote_mean() {
        use crate::market_data::MarketData;

        let tick_data = Arc::new(AllMarketData::new());
        let snap_data = Arc::new(AllSnapshotData::new(1024));

        let sym: SymbolId = 0;
        for i in 0..5 {
            tick_data.binance.push(
                &sym,
                MarketData {
                    bid: Some(100.0 + i as f64),
                    ask: Some(102.0 + i as f64),
                    bid_qty: Some(1.0),
                    ask_qty: Some(1.0),
                    exchange_ts: None,
                    received_ts: None,
                },
            );
        }

        let analytics = Analytics::new(tick_data, snap_data);
        let mean = analytics.tick_midquote_mean(&Exchange::Binance, 0, 5).unwrap();
        // mids: 101, 102, 103, 104, 105 → but read_last_n returns most recent first
        // mean of [103, 102, 101, 104, 105] = 103.0 regardless of order
        assert!((mean - 103.0).abs() < f64::EPSILON);
    }
}
