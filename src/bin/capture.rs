use anyhow::{Context, Result};
use chrono::Utc;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::Notify;
use tokio::time::{self, MissedTickBehavior};

use arrow::array::{
    Float64Builder, Int64Builder, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crypto_feeds::app_config::{load_config, load_onchain, load_perp, load_spot, load_trades, AppConfig};
use crypto_feeds::trade_data::AllTradeData;
use crypto_feeds::fair_price::{
    DiagWriter, FairPriceConfig, FairPriceEngine, FairPriceGroupConfig, FairPriceOutputs,
    GroupMember, run_fair_price_task,
};
use crypto_feeds::market_data::{AllMarketData, Exchange, InstrumentType, MarketDataCollection};
use crypto_feeds::symbol_registry::{REGISTRY, SymbolId};
use crypto_feeds::trade_data::TradeDataCollection;

/// One entry in our sampling plan: which exchange + symbol to read each tick.
struct SampleTarget {
    exchange_name: &'static str,
    canonical: String,
    collection: Arc<MarketDataCollection>,
    symbol_id: SymbolId,
    prev_write_pos: u64,
}

fn build_targets(cfg: &AppConfig, market_data: &AllMarketData) -> Vec<SampleTarget> {
    let mut targets = Vec::new();

    for (exchange, coll) in market_data.iter() {
        let name = exchange.as_str();
        if let Some(syms) = cfg.spot.get(name) {
            for raw in syms {
                if let Some(&id) = REGISTRY.lookup(raw, &InstrumentType::Spot) {
                    let canonical = REGISTRY.get_symbol(id).unwrap_or(raw).to_string();
                    targets.push(SampleTarget {
                        exchange_name: name,
                        canonical,
                        collection: Arc::clone(coll),
                        symbol_id: id,
                        prev_write_pos: 0,
                    });
                }
            }
        }
        if let Some(syms) = cfg.perp.get(name) {
            for raw in syms {
                if let Some(&id) = REGISTRY.lookup(raw, &InstrumentType::Perp) {
                    let canonical = REGISTRY.get_symbol(id).unwrap_or(raw).to_string();
                    targets.push(SampleTarget {
                        exchange_name: name,
                        canonical,
                        collection: Arc::clone(coll),
                        symbol_id: id,
                        prev_write_pos: 0,
                    });
                }
            }
        }
    }

    // On-chain pools (aerodrome, uniswap)
    if let Some(ref onchain) = cfg.onchain {
        for (dex_name, dex_cfg, coll) in [
            ("aerodrome", &onchain.aerodrome, &market_data.aerodrome),
            ("uniswap", &onchain.uniswap, &market_data.uniswap),
        ] {
            if let Some(pools) = dex_cfg {
                for pool in &pools.validated_pools(dex_name) {
                    if let Some(&id) = REGISTRY.lookup(&pool.symbol, &InstrumentType::Spot) {
                        let canonical =
                            REGISTRY.get_symbol(id).unwrap_or(&pool.symbol).to_string();
                        targets.push(SampleTarget {
                            exchange_name: dex_name,
                            canonical,
                            collection: Arc::clone(coll),
                            symbol_id: id,
                            prev_write_pos: 0,
                        });
                    }
                }
            }
        }
    }

    targets
}

struct TradeSampleTarget {
    exchange_name: &'static str,
    canonical: String,
    collection: Arc<TradeDataCollection>,
    symbol_id: SymbolId,
    prev_write_pos: u64,
}

fn build_trade_targets(cfg: &AppConfig, trade_data: &AllTradeData) -> Vec<TradeSampleTarget> {
    let mut targets = Vec::new();
    for (exchange, coll) in trade_data.iter() {
        let name = exchange.as_str();
        if let Some(syms) = cfg.trades.get(name) {
            for raw in syms {
                if let Some(&id) = REGISTRY.lookup(raw, &InstrumentType::Perp) {
                    let canonical = REGISTRY.get_symbol(id).unwrap_or(raw).to_string();
                    targets.push(TradeSampleTarget {
                        exchange_name: name,
                        canonical,
                        collection: Arc::clone(coll),
                        symbol_id: id,
                        prev_write_pos: 0,
                    });
                }
            }
        }
    }
    targets
}

fn format_side(side: crypto_feeds::TradeSide) -> &'static str {
    match side {
        crypto_feeds::TradeSide::Buy => "buy",
        crypto_feeds::TradeSide::Sell => "sell",
        crypto_feeds::TradeSide::Unknown => "",
    }
}

fn ts_nanos(ts: Option<chrono::DateTime<Utc>>) -> i64 {
    ts.and_then(|t| t.timestamp_nanos_opt()).unwrap_or(0)
}

fn auto_discover_groups(cfg: &AppConfig) -> Vec<FairPriceGroupConfig> {
    use std::collections::HashMap;
    let fp = &cfg.fair_price;
    let model = fp.parse_model();
    let sigma_mode = fp.parse_sigma_mode();
    type Entry = (String, String, InstrumentType, Option<String>);
    let mut base_map: HashMap<String, Vec<Entry>> = HashMap::new();

    for (exchange, symbols) in &cfg.spot {
        for sym in symbols {
            if let Some(base) = sym.split('_').next() {
                base_map
                    .entry(base.to_string())
                    .or_default()
                    .push((exchange.clone(), sym.clone(), InstrumentType::Spot, None));
            }
        }
    }
    for (exchange, symbols) in &cfg.perp {
        for sym in symbols {
            if let Some(base) = sym.split('_').next() {
                base_map
                    .entry(base.to_string())
                    .or_default()
                    .push((exchange.clone(), sym.clone(), InstrumentType::Perp, None));
            }
        }
    }

    const USD_QUOTES: &[&str] = &["USD", "USDT", "USDC"];
    if let Some(ref onchain) = cfg.onchain {
        for (dex_name, dex_cfg) in [("aerodrome", &onchain.aerodrome), ("uniswap", &onchain.uniswap)] {
            if let Some(pools) = dex_cfg {
                for pool in &pools.validated_pools(dex_name) {
                    let parts: Vec<&str> = pool.symbol.split('_').collect();
                    if parts.len() == 2 {
                        let reprice = if USD_QUOTES.contains(&parts[1]) {
                            None
                        } else {
                            Some(parts[1].to_string())
                        };
                        base_map
                            .entry(parts[0].to_string())
                            .or_default()
                            .push((dex_name.to_string(), pool.symbol.clone(), InstrumentType::Spot, reprice));
                    }
                }
            }
        }
    }

    let mut groups: Vec<FairPriceGroupConfig> = Vec::new();
    let mut bases: Vec<String> = base_map.keys().cloned().collect();
    bases.sort();

    for base in bases {
        let entries = &base_map[&base];
        let mut members = Vec::new();
        for (exchange_name, symbol_str, itype, reprice) in entries {
            let exchange = match Exchange::from_str(exchange_name) {
                Some(e) => e,
                None => continue,
            };
            let symbol_id = match REGISTRY.lookup(symbol_str, itype) {
                Some(&id) => id,
                None => continue,
            };
            members.push(GroupMember {
                exchange,
                symbol_id,
                bias: 0.0,
                noise_var: 4e-8,
                gg_weight: 0.0,
                reprice_group: reprice.clone(),
                invert_reprice: false,
                vol_24h: 0.0,
                vol_adj: 1.0,
            });
        }
        if members.len() >= 2 {
            let bps = fp.bias_process_noise_bps_per_sqrt_s;
            let log_val = bps * 1e-4;
            let init_bps = fp.bias_init_uncertainty_bps;
            groups.push(FairPriceGroupConfig {
                name: base.clone(),
                members,
                sigma_mode: sigma_mode,
                model: model,
                bias_ewma_halflife_ms: fp.bias_ewma_halflife_s * 1000.0,
                spread_ewma_halflife_ms: fp.spread_ewma_halflife_s * 1000.0,
                sigma_k_floor: fp.sigma_k_floor,
                h_bias_per_ms: log_val * log_val / 1000.0,
                bias_init_p: (init_bps * 1e-4) * (init_bps * 1e-4),
                liquidity_adjustment: fp.liquidity_adjustment,
                sigma_scale: fp.sigma_scale,
            });
        }
    }
    groups
}

// ── Parquet schemas ──────────────────────────────────────────

fn bbo_schema() -> Schema {
    Schema::new(vec![
        Field::new("tick_ts_ns", DataType::Int64, false),
        Field::new("exchange", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("bid", DataType::Float64, true),
        Field::new("ask", DataType::Float64, true),
        Field::new("bid_qty", DataType::Float64, true),
        Field::new("ask_qty", DataType::Float64, true),
        Field::new("exchange_ts_ns", DataType::Int64, true),
        Field::new("received_ts_ns", DataType::Int64, true),
    ])
}

fn trade_schema() -> Schema {
    Schema::new(vec![
        Field::new("tick_ts_ns", DataType::Int64, false),
        Field::new("exchange", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("qty", DataType::Float64, false),
        Field::new("side", DataType::Utf8, false),
        Field::new("exchange_ts_ns", DataType::Int64, true),
        Field::new("received_ts_ns", DataType::Int64, true),
    ])
}

// ── Parquet row-group buffer ─────────────────────────────────

const ROW_GROUP_SIZE: usize = 65536;

struct BboBuffer {
    tick_ts: Int64Builder,
    exchange: StringBuilder,
    symbol: StringBuilder,
    bid: Float64Builder,
    ask: Float64Builder,
    bid_qty: Float64Builder,
    ask_qty: Float64Builder,
    exchange_ts: Int64Builder,
    received_ts: Int64Builder,
    len: usize,
}

impl BboBuffer {
    fn new() -> Self {
        Self {
            tick_ts: Int64Builder::with_capacity(ROW_GROUP_SIZE),
            exchange: StringBuilder::with_capacity(ROW_GROUP_SIZE, ROW_GROUP_SIZE * 10),
            symbol: StringBuilder::with_capacity(ROW_GROUP_SIZE, ROW_GROUP_SIZE * 16),
            bid: Float64Builder::with_capacity(ROW_GROUP_SIZE),
            ask: Float64Builder::with_capacity(ROW_GROUP_SIZE),
            bid_qty: Float64Builder::with_capacity(ROW_GROUP_SIZE),
            ask_qty: Float64Builder::with_capacity(ROW_GROUP_SIZE),
            exchange_ts: Int64Builder::with_capacity(ROW_GROUP_SIZE),
            received_ts: Int64Builder::with_capacity(ROW_GROUP_SIZE),
            len: 0,
        }
    }

    fn append(
        &mut self,
        ts: i64,
        exch: &str,
        sym: &str,
        md: &crypto_feeds::market_data::MarketData,
    ) {
        self.tick_ts.append_value(ts);
        self.exchange.append_value(exch);
        self.symbol.append_value(sym);
        match md.bid {
            Some(v) => self.bid.append_value(v),
            None => self.bid.append_null(),
        }
        match md.ask {
            Some(v) => self.ask.append_value(v),
            None => self.ask.append_null(),
        }
        match md.bid_qty {
            Some(v) => self.bid_qty.append_value(v),
            None => self.bid_qty.append_null(),
        }
        match md.ask_qty {
            Some(v) => self.ask_qty.append_value(v),
            None => self.ask_qty.append_null(),
        }
        self.exchange_ts.append_value(ts_nanos(md.exchange_ts));
        self.received_ts.append_value(ts_nanos(md.received_ts));
        self.len += 1;
    }

    fn flush(&mut self, writer: &mut ArrowWriter<std::fs::File>) -> Result<()> {
        if self.len == 0 {
            return Ok(());
        }
        let batch = RecordBatch::try_new(
            Arc::new(bbo_schema()),
            vec![
                Arc::new(self.tick_ts.finish()),
                Arc::new(self.exchange.finish()),
                Arc::new(self.symbol.finish()),
                Arc::new(self.bid.finish()),
                Arc::new(self.ask.finish()),
                Arc::new(self.bid_qty.finish()),
                Arc::new(self.ask_qty.finish()),
                Arc::new(self.exchange_ts.finish()),
                Arc::new(self.received_ts.finish()),
            ],
        )?;
        writer.write(&batch)?;
        self.len = 0;
        Ok(())
    }
}

struct TradeBuffer {
    tick_ts: Int64Builder,
    exchange: StringBuilder,
    symbol: StringBuilder,
    price: Float64Builder,
    qty: Float64Builder,
    side: StringBuilder,
    exchange_ts: Int64Builder,
    received_ts: Int64Builder,
    len: usize,
}

impl TradeBuffer {
    fn new() -> Self {
        Self {
            tick_ts: Int64Builder::with_capacity(ROW_GROUP_SIZE),
            exchange: StringBuilder::with_capacity(ROW_GROUP_SIZE, ROW_GROUP_SIZE * 10),
            symbol: StringBuilder::with_capacity(ROW_GROUP_SIZE, ROW_GROUP_SIZE * 16),
            price: Float64Builder::with_capacity(ROW_GROUP_SIZE),
            qty: Float64Builder::with_capacity(ROW_GROUP_SIZE),
            side: StringBuilder::with_capacity(ROW_GROUP_SIZE, ROW_GROUP_SIZE * 4),
            exchange_ts: Int64Builder::with_capacity(ROW_GROUP_SIZE),
            received_ts: Int64Builder::with_capacity(ROW_GROUP_SIZE),
            len: 0,
        }
    }

    fn append(
        &mut self,
        ts: i64,
        exch: &str,
        sym: &str,
        td: &crypto_feeds::trade_data::TradeData,
    ) {
        self.tick_ts.append_value(ts);
        self.exchange.append_value(exch);
        self.symbol.append_value(sym);
        self.price.append_value(td.price);
        self.qty.append_value(td.qty);
        self.side.append_value(format_side(td.side));
        self.exchange_ts.append_value(ts_nanos(td.exchange_ts));
        self.received_ts.append_value(ts_nanos(td.received_ts));
        self.len += 1;
    }

    fn flush(&mut self, writer: &mut ArrowWriter<std::fs::File>) -> Result<()> {
        if self.len == 0 {
            return Ok(());
        }
        let batch = RecordBatch::try_new(
            Arc::new(trade_schema()),
            vec![
                Arc::new(self.tick_ts.finish()),
                Arc::new(self.exchange.finish()),
                Arc::new(self.symbol.finish()),
                Arc::new(self.price.finish()),
                Arc::new(self.qty.finish()),
                Arc::new(self.side.finish()),
                Arc::new(self.exchange_ts.finish()),
                Arc::new(self.received_ts.finish()),
            ],
        )?;
        writer.write(&batch)?;
        self.len = 0;
        Ok(())
    }
}

// ── Args ─────────────────────────────────────────────────────

struct Args {
    config_path: String,
    output_dir: String,
    interval_ms: u64,
    with_fp: bool,
    display: bool,
    warmup_secs: u64,
}

fn parse_args() -> Args {
    let mut args = Args {
        config_path: "configs/capture.yaml".to_string(),
        output_dir: "data/capture".to_string(),
        interval_ms: 0,
        with_fp: false,
        display: false,
        warmup_secs: 5,
    };
    let argv: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < argv.len() {
        match argv[i].as_str() {
            "--config" if i + 1 < argv.len() => { args.config_path = argv[i + 1].clone(); i += 2; }
            "--output-dir" if i + 1 < argv.len() => { args.output_dir = argv[i + 1].clone(); i += 2; }
            "--interval-ms" if i + 1 < argv.len() => { args.interval_ms = argv[i + 1].parse().unwrap_or(0); i += 2; }
            "--with-fp" => { args.with_fp = true; i += 1; }
            "--display" => { args.display = true; i += 1; }
            "--warmup-secs" if i + 1 < argv.len() => { args.warmup_secs = argv[i + 1].parse().unwrap_or(5); i += 2; }
            "--help" | "-h" => {
                eprintln!("Usage: capture [--interval-ms N] [--with-fp] [--display] [--output-dir DIR] [--config PATH] [--warmup-secs N]");
                eprintln!();
                eprintln!("  --interval-ms N    Poll interval in ms (default: 0 = tick-by-tick on change)");
                eprintln!("  --with-fp          Start FP engine and write tick diagnostics");
                eprintln!("  --display          Live TUI display");
                eprintln!("  --output-dir DIR   Output directory (default: data/capture)");
                eprintln!("  --config PATH      Exchange/symbol config (default: configs/capture.yaml)");
                eprintln!("  --warmup-secs N    Wait N seconds before capturing (default: 5)");
                std::process::exit(0);
            }
            _ => { i += 1; }
        }
    }
    args
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args();

    if args.display {
        crypto_feeds::display::init_display_logger(log::LevelFilter::Info);
    } else {
        env_logger::init();
    }

    let cfg: AppConfig = load_config(&args.config_path)
        .with_context(|| format!("loading {}", args.config_path))?;

    crypto_feeds::symbol_registry::seed_extra_bases(cfg.base_assets());

    std::fs::create_dir_all(&args.output_dir)?;

    let market_data = Arc::new(AllMarketData::with_clock_correction(cfg.clock_correction.clone()));
    let shutdown = Arc::new(Notify::new());
    let mut handles = Vec::new();

    load_spot(&mut handles, &cfg, &market_data, &shutdown)?;
    load_perp(&mut handles, &cfg, &market_data, &shutdown)?;
    if let Err(e) = load_onchain(&mut handles, &cfg, &market_data, &shutdown) {
        log::warn!("Onchain feeds not started: {}", e);
    }

    let trade_data = Arc::new(AllTradeData::with_clock_correction(cfg.clock_correction.clone()));
    load_trades(&mut handles, &cfg, &trade_data, &shutdown)?;

    let mut targets = build_targets(&cfg, &market_data);
    let mut trade_targets = build_trade_targets(&cfg, &trade_data);
    eprintln!("Discovered {} BBO targets, {} trade targets", targets.len(), trade_targets.len());

    // Set up FP engine if requested
    if args.with_fp {
        let volumes = Arc::new(crypto_feeds::volume_fetcher::fetch_all_volumes(&cfg).await);
        let mut groups = auto_discover_groups(&cfg);
        for group in &mut groups {
            for m in &mut group.members {
                let reg_sym = REGISTRY.get_symbol(m.symbol_id).unwrap_or("?");
                if let Some(&vol) = volumes.get(&(m.exchange, reg_sym.to_string())) {
                    m.vol_24h = vol;
                }
                let trust = cfg.fair_price.volume_adjustments
                    .get(m.exchange.as_str()).copied().unwrap_or(1.0);
                m.vol_24h *= trust;
            }
            let max_vol = group.members.iter().map(|m| m.vol_24h).fold(0.0_f64, f64::max);
            for m in &mut group.members {
                let effective = if m.vol_24h > 0.0 { m.vol_24h } else { max_vol * 0.01 };
                m.vol_adj = if max_vol > 0.0 && effective > 0.0 {
                    (max_vol / effective).powf(cfg.fair_price.liq_adj_exponent)
                } else {
                    1.0
                };
            }
        }
        if !groups.is_empty() {
            let group_names: Vec<String> = groups.iter().map(|g| g.name.clone()).collect();
            let vol_provider = cfg.fair_price.to_vol_provider(&group_names);
            let drain_interval_ms = cfg.fair_price.drain_interval_ms;
            let fp_config = FairPriceConfig {
                interval_ms: drain_interval_ms.max(1) as u64,
                buffer_capacity: 65536,
                groups,
                vol_provider,
            };
            for g in &fp_config.groups {
                eprintln!("FP group '{}': {} members", g.name, g.members.len());
            }
            let out = Arc::new(FairPriceOutputs::new(&fp_config));
            let diag = Some(DiagWriter::new(&args.output_dir)
                .with_context(|| format!("creating DiagWriter at {}", args.output_dir))?);
            let engine = Arc::new(FairPriceEngine::new(
                Arc::clone(&market_data),
                Arc::clone(&out),
                fp_config,
                diag,
            ));
            if drain_interval_ms > 0 {
                let sd = Arc::clone(&shutdown);
                handles.push(tokio::spawn(run_fair_price_task(Arc::clone(&engine), sd)));
            }
        }
    }

    // Display
    let cfg = Arc::new(cfg);
    if args.display {
        let md = Arc::clone(&market_data);
        let sd = Arc::clone(&shutdown);
        let cfg_arc = Arc::clone(&cfg);
        handles.push(tokio::spawn(async move {
            if let Err(e) = crypto_feeds::feed_display::run_feed_display(md, cfg_arc, sd).await {
                log::error!("feed display error: {:?}", e);
            }
        }));
    }

    // Warmup
    if args.warmup_secs > 0 {
        eprintln!("Warming up for {} seconds...", args.warmup_secs);
        tokio::time::sleep(Duration::from_secs(args.warmup_secs)).await;
    }

    // ── BBO capture ──────────────────────────────────────────
    let bbo_handle = {
        let shutdown_clone = Arc::clone(&shutdown);
        let output_dir = args.output_dir.clone();
        let interval_ms = args.interval_ms;

        tokio::spawn(async move {
            let ts = Utc::now().format("%Y%m%d_%H%M%S");
            let output_path = format!("{}/bbo_{}.parquet", output_dir, ts);
            let file = std::fs::File::create(&output_path).expect("create BBO parquet file");

            let props = WriterProperties::builder()
                .set_compression(Compression::ZSTD(Default::default()))
                .set_max_row_group_size(ROW_GROUP_SIZE)
                .build();
            let mut writer = ArrowWriter::try_new(file, Arc::new(bbo_schema()), Some(props))
                .expect("create ArrowWriter");
            let mut buf = BboBuffer::new();
            let mut row_count: u64 = 0;

            let poll_ms = if interval_ms == 0 { 1 } else { interval_ms };
            let mut interval = time::interval(Duration::from_millis(poll_ms));
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            let shutdown_fut = shutdown_clone.notified();
            tokio::pin!(shutdown_fut);

            loop {
                tokio::select! {
                    _ = interval.tick() => {}
                    _ = &mut shutdown_fut => {
                        let _ = buf.flush(&mut writer);
                        writer.close().expect("close BBO parquet");
                        eprintln!("BBO capture: {} rows -> {}", row_count, output_path);
                        return;
                    }
                }

                let now_ns = Utc::now().timestamp_nanos_opt().unwrap_or(0);

                for t in targets.iter_mut() {
                    let ring = match t.collection.get_buffer(&t.symbol_id) {
                        Some(b) => b,
                        None => continue,
                    };
                    let cur_pos = ring.write_pos();
                    if cur_pos <= t.prev_write_pos {
                        continue;
                    }
                    // Drain all new ticks since last poll
                    let start = t.prev_write_pos;
                    let count = (cur_pos - start).min(1000) as usize;
                    for i in 0..count {
                        if let Some(md) = ring.read_at(start + i as u64) {
                            buf.append(now_ns, t.exchange_name, &t.canonical, &md);
                            row_count += 1;
                            if buf.len >= ROW_GROUP_SIZE {
                                buf.flush(&mut writer).expect("flush BBO row group");
                            }
                        }
                    }
                    t.prev_write_pos = cur_pos;
                }
            }
        })
    };

    // ── Trade capture ────────────────────────────────────────
    let trade_handle = if !trade_targets.is_empty() {
        let shutdown_clone = Arc::clone(&shutdown);
        let output_dir = args.output_dir.clone();
        let interval_ms = args.interval_ms;

        Some(tokio::spawn(async move {
            let ts = Utc::now().format("%Y%m%d_%H%M%S");
            let output_path = format!("{}/trades_{}.parquet", output_dir, ts);
            let file = std::fs::File::create(&output_path).expect("create trades parquet file");

            let props = WriterProperties::builder()
                .set_compression(Compression::ZSTD(Default::default()))
                .set_max_row_group_size(ROW_GROUP_SIZE)
                .build();
            let mut writer = ArrowWriter::try_new(file, Arc::new(trade_schema()), Some(props))
                .expect("create ArrowWriter");
            let mut buf = TradeBuffer::new();
            let mut row_count: u64 = 0;

            let poll_ms = if interval_ms == 0 { 1 } else { interval_ms };
            let mut interval = time::interval(Duration::from_millis(poll_ms));
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            let shutdown_fut = shutdown_clone.notified();
            tokio::pin!(shutdown_fut);

            loop {
                tokio::select! {
                    _ = interval.tick() => {}
                    _ = &mut shutdown_fut => {
                        let _ = buf.flush(&mut writer);
                        writer.close().expect("close trades parquet");
                        eprintln!("Trade capture: {} rows -> {}", row_count, output_path);
                        return;
                    }
                }

                let now_ns = Utc::now().timestamp_nanos_opt().unwrap_or(0);

                for t in trade_targets.iter_mut() {
                    let ring = match t.collection.get_buffer(&t.symbol_id) {
                        Some(b) => b,
                        None => continue,
                    };
                    let cur_pos = ring.write_pos();
                    if cur_pos <= t.prev_write_pos {
                        continue;
                    }
                    let start = t.prev_write_pos;
                    let count = (cur_pos - start).min(1000) as usize;
                    for i in 0..count {
                        if let Some(td) = ring.read_at(start + i as u64) {
                            buf.append(now_ns, t.exchange_name, &t.canonical, &td);
                            row_count += 1;
                            if buf.len >= ROW_GROUP_SIZE {
                                buf.flush(&mut writer).expect("flush trade row group");
                            }
                        }
                    }
                    t.prev_write_pos = cur_pos;
                }
            }
        }))
    } else {
        None
    };

    if args.interval_ms == 0 {
        eprintln!("Capturing tick-by-tick -> {}/", args.output_dir);
    } else {
        eprintln!("Capturing every {}ms -> {}/", args.interval_ms, args.output_dir);
    }

    signal::ctrl_c().await?;
    eprintln!("\nShutting down...");
    shutdown.notify_waiters();

    tokio::time::timeout(Duration::from_secs(5), async {
        let _ = bbo_handle.await;
        if let Some(h) = trade_handle {
            let _ = h.await;
        }
        for h in handles {
            let _ = h.await;
        }
    })
    .await?;

    Ok(())
}
