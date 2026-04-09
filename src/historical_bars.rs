use anyhow::{Context, Result};
use chrono::{TimeDelta, Utc};
use log::{info, warn};
use std::path::Path;

/// A completed OHLCV bar.
#[derive(Debug, Clone, Copy)]
pub struct Bar {
    pub open_time_ms: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

/// Load 1-minute bars from daily CSV files for the last `warmup_days` days.
///
/// Scans `{bar_data_dir}/{symbol}USDT/YYYY-MM-DD.csv`.
/// Returns bars sorted ascending by open_time.
pub fn load_1m_bars(bar_data_dir: &Path, symbol: &str, warmup_days: u32) -> Result<Vec<Bar>> {
    let dir = bar_data_dir.join(format!("{}USDT", symbol.to_uppercase()));
    let today = Utc::now().date_naive();
    let start = today - TimeDelta::days(warmup_days as i64);

    let mut bars = Vec::new();
    let mut date = start;
    while date <= today {
        let filename = format!("{}.csv", date);
        let path = dir.join(&filename);
        if path.exists() {
            match read_csv_file(&path) {
                Ok(mut file_bars) => bars.append(&mut file_bars),
                Err(e) => warn!("skipping {}: {}", path.display(), e),
            }
        }
        date += TimeDelta::days(1);
    }

    bars.sort_by_key(|b| b.open_time_ms);
    Ok(bars)
}

fn read_csv_file(path: &Path) -> Result<Vec<Bar>> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(path)
        .with_context(|| format!("opening {}", path.display()))?;

    let mut bars = Vec::new();
    for result in rdr.records() {
        let record = result?;
        let open_time_ms: i64 = record[0].parse()?;
        let open: f64 = record[1].parse()?;
        let high: f64 = record[2].parse()?;
        let low: f64 = record[3].parse()?;
        let close: f64 = record[4].parse()?;
        let volume: f64 = record[5].parse()?;

        if open <= 0.0 || volume <= 0.0 {
            continue;
        }
        bars.push(Bar {
            open_time_ms,
            open,
            high,
            low,
            close,
            volume,
        });
    }
    Ok(bars)
}

/// Fetch 1-minute bars from Binance perpetual futures kline API.
/// Fetches from `start_ms` up to now, paginating in chunks of 1500.
pub async fn fetch_binance_1m_bars(symbol: &str, start_ms: i64) -> Result<Vec<Bar>> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()?;
    let binance_symbol = format!("{}USDT", symbol.to_uppercase());
    let now_ms = Utc::now().timestamp_millis();
    let mut all_bars = Vec::new();
    let mut cursor = start_ms;

    loop {
        if cursor >= now_ms {
            break;
        }
        let url = format!(
            "https://fapi.binance.com/fapi/v1/klines?symbol={}&interval=1m&startTime={}&limit=1500",
            binance_symbol, cursor
        );
        let resp = client.get(&url).send().await
            .with_context(|| format!("fetching klines for {}", binance_symbol))?;

        if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            continue;
        }
        if !resp.status().is_success() {
            anyhow::bail!("Binance API error {}: {}", resp.status(), resp.text().await.unwrap_or_default());
        }

        let data: Vec<Vec<serde_json::Value>> = resp.json().await
            .context("parsing Binance kline response")?;

        if data.is_empty() {
            break;
        }

        for row in &data {
            if row.len() < 6 {
                continue;
            }
            let open_time_ms = row[0].as_i64().unwrap_or(0);
            let open: f64 = row[1].as_str().unwrap_or("0").parse().unwrap_or(0.0);
            let high: f64 = row[2].as_str().unwrap_or("0").parse().unwrap_or(0.0);
            let low: f64 = row[3].as_str().unwrap_or("0").parse().unwrap_or(0.0);
            let close: f64 = row[4].as_str().unwrap_or("0").parse().unwrap_or(0.0);
            let volume: f64 = row[5].as_str().unwrap_or("0").parse().unwrap_or(0.0);

            if open > 0.0 && volume > 0.0 {
                all_bars.push(Bar { open_time_ms, open, high, low, close, volume });
            }
        }

        // Advance cursor past the last bar we received
        let last_ts = data.last().and_then(|r| r[0].as_i64()).unwrap_or(now_ms);
        cursor = last_ts + 60_000;

        if data.len() < 1500 {
            break;
        }
    }

    Ok(all_bars)
}

/// Load 1m bars from disk, then backfill any gap to now from Binance API.
pub async fn load_1m_bars_with_backfill(
    bar_data_dir: &Path,
    symbol: &str,
    warmup_days: u32,
) -> Result<Vec<Bar>> {
    let mut bars = load_1m_bars(bar_data_dir, symbol, warmup_days)?;
    let now_ms = Utc::now().timestamp_millis();

    let earliest_needed_ms = now_ms - (warmup_days as i64) * 86_400_000;
    let last_bar_ms = bars.last().map(|b| b.open_time_ms).unwrap_or(0);
    let fetch_from = (last_bar_ms + 60_000).max(earliest_needed_ms);
    let gap_minutes = (now_ms - fetch_from) / 60_000;

    if gap_minutes > 1 {
        info!(
            "{}: disk bars end {}m ago, fetching from Binance API",
            symbol, gap_minutes
        );
        match fetch_binance_1m_bars(symbol, fetch_from).await {
            Ok(mut fetched) => {
                info!("{}: fetched {} bars from Binance", symbol, fetched.len());
                bars.append(&mut fetched);
                bars.sort_by_key(|b| b.open_time_ms);
                bars.dedup_by_key(|b| b.open_time_ms);
            }
            Err(e) => {
                warn!("{}: Binance backfill failed (using stale disk data): {}", symbol, e);
            }
        }
    } else {
        info!("{}: disk bars are fresh ({}m gap)", symbol, gap_minutes);
    }

    Ok(bars)
}

/// Aggregate 1-minute bars into `target_min`-minute bars.
///
/// Groups by `open_time_ms / interval_ms`. OHLC rules:
/// open = first open, high = max high, low = min low, close = last close, volume = sum.
/// Drops incomplete final group (fewer than `target_min` bars).
pub fn aggregate_bars(bars_1m: &[Bar], target_min: usize) -> Vec<Bar> {
    if bars_1m.is_empty() || target_min == 0 {
        return Vec::new();
    }
    if target_min == 1 {
        return bars_1m.to_vec();
    }

    let interval_ms = target_min as i64 * 60_000;
    let mut result = Vec::new();
    let mut i = 0;

    while i < bars_1m.len() {
        let group_key = bars_1m[i].open_time_ms / interval_ms;
        let group_start_ms = group_key * interval_ms;

        let mut agg = Bar {
            open_time_ms: group_start_ms,
            open: bars_1m[i].open,
            high: bars_1m[i].high,
            low: bars_1m[i].low,
            close: bars_1m[i].close,
            volume: bars_1m[i].volume,
        };
        let mut count = 1usize;
        i += 1;

        while i < bars_1m.len() && bars_1m[i].open_time_ms / interval_ms == group_key {
            let b = &bars_1m[i];
            if b.high > agg.high {
                agg.high = b.high;
            }
            if b.low < agg.low {
                agg.low = b.low;
            }
            agg.close = b.close;
            agg.volume += b.volume;
            count += 1;
            i += 1;
        }

        // Only keep complete groups
        if count >= target_min {
            result.push(agg);
        }
    }

    result
}

/// Load 1m bars from CSV and aggregate to `target_min`-minute bars.
pub fn load_historical_bars(
    bar_data_dir: &Path,
    symbol: &str,
    target_min: usize,
    warmup_days: u32,
) -> Result<Vec<Bar>> {
    let bars_1m = load_1m_bars(bar_data_dir, symbol, warmup_days)?;
    log::info!(
        "loaded {} 1m bars for {} ({} days)",
        bars_1m.len(),
        symbol,
        warmup_days
    );
    let aggregated = aggregate_bars(&bars_1m, target_min);
    log::info!(
        "aggregated to {} {}m bars for {}",
        aggregated.len(),
        target_min,
        symbol
    );
    Ok(aggregated)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn data_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("data")
            .join("binance_ohlcv")
            .join("perps")
            .join("1m")
    }

    #[test]
    fn test_aggregate_bars_5m() {
        // 10 synthetic 1-minute bars
        let bars: Vec<Bar> = (0..10)
            .map(|i| Bar {
                open_time_ms: i * 60_000,
                open: 100.0 + i as f64,
                high: 105.0 + i as f64,
                low: 95.0 + i as f64,
                close: 101.0 + i as f64,
                volume: 10.0,
            })
            .collect();

        let agg = aggregate_bars(&bars, 5);
        assert_eq!(agg.len(), 2);

        // First group: bars 0..5
        assert_eq!(agg[0].open_time_ms, 0);
        assert_eq!(agg[0].open, 100.0); // first open
        assert_eq!(agg[0].high, 109.0); // max(105..110)
        assert_eq!(agg[0].low, 95.0); // min(95..100)
        assert_eq!(agg[0].close, 105.0); // last close
        assert!((agg[0].volume - 50.0).abs() < 1e-9);

        // Second group: bars 5..10
        assert_eq!(agg[1].open_time_ms, 300_000);
        assert_eq!(agg[1].open, 105.0);
    }

    #[test]
    fn test_aggregate_drops_incomplete() {
        let bars: Vec<Bar> = (0..7)
            .map(|i| Bar {
                open_time_ms: i * 60_000,
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.5,
                volume: 1.0,
            })
            .collect();

        let agg = aggregate_bars(&bars, 5);
        // 7 bars = 1 complete group (5) + 1 incomplete (2)
        assert_eq!(agg.len(), 1);
    }

    #[test]
    fn test_aggregate_target_1() {
        let bars = vec![Bar {
            open_time_ms: 0,
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.5,
            volume: 1.0,
        }];
        let agg = aggregate_bars(&bars, 1);
        assert_eq!(agg.len(), 1);
    }

    #[test]
    fn test_load_real_csv() {
        let dir = data_dir();
        if !dir.exists() {
            eprintln!("skipping test — {} not found", dir.display());
            return;
        }
        let bars = load_1m_bars(&dir, "BTC", 1).unwrap();
        if bars.is_empty() {
            eprintln!("skipping — no BTC bars found for today");
            return;
        }
        assert!(bars[0].open > 0.0);
        assert!(bars[0].high >= bars[0].low);

        let agg = aggregate_bars(&bars, 5);
        assert!(agg.len() <= bars.len() / 5 + 1);
        for bar in &agg {
            assert!(bar.high >= bar.low);
            assert!(bar.high >= bar.open);
            assert!(bar.high >= bar.close);
            assert!(bar.low <= bar.open);
            assert!(bar.low <= bar.close);
        }
    }
}
