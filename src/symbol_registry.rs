use once_cell::sync::Lazy;
use rustc_hash::FxHashMap;
use serde::Deserialize;
use std::path::Path;

use crate::market_data::InstrumentType;

pub const MAX_SYMBOLS: usize = 1_000;
pub type SymbolId = usize;

#[derive(Deserialize)]
struct Config {
    base_assets: Vec<String>,
}

pub struct SymbolRegistry {
    to_symbol: [Option<String>; MAX_SYMBOLS],
    spot_to_id: FxHashMap<String, SymbolId>,
    perp_to_id: FxHashMap<String, SymbolId>,
}

impl SymbolRegistry {
    fn new() -> Self {
        Self {
            to_symbol: std::array::from_fn(|_| None),
            spot_to_id: FxHashMap::default(),
            perp_to_id: FxHashMap::default(),
        }
    }

    pub fn from_config(path: &str) -> Result<Self, String> {
        let content =
            std::fs::read_to_string(path).map_err(|e| format!("Failed to read {}: {}", path, e))?;

        let config: Config =
            serde_yaml::from_str(&content).map_err(|e| format!("Failed to parse YAML: {}", e))?;

        let mut reg = Self::new();

        let quote_currencies = ["USDT", "USDC", "USD"];
        let instrument_types = [InstrumentType::Spot, InstrumentType::Perp];

        for base in &config.base_assets {
            for quote in &quote_currencies {
                for instrument in instrument_types {
                    // Canonical format: SPOT-BTC-USDT, PERP-ETH-USDC, etc.
                    let canonical = format!("{}-{}-{}", instrument.as_str(), base, quote);
                    let id = reg.register_symbol(&canonical)?;

                    match instrument {
                        InstrumentType::Spot => {
                            reg.spot_to_id.insert(canonical, id);
                        }
                        InstrumentType::Perp => {
                            reg.perp_to_id.insert(canonical, id);
                        }
                        _ => {}
                    }

                    // Generate all alias formats
                    let aliases = generate_aliases(base, quote, &instrument);
                    for alias in aliases {
                        match instrument {
                            InstrumentType::Spot => {
                                reg.spot_to_id.insert(alias, id);
                            }
                            InstrumentType::Perp => {
                                reg.perp_to_id.insert(alias, id);
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        Ok(reg)
    }

    fn register_symbol(&mut self, canonical: &str) -> Result<SymbolId, String> {
        let count = self.to_symbol.iter().filter(|s| s.is_some()).count();

        if count >= MAX_SYMBOLS {
            return Err("Symbol registry full".to_string());
        }

        self.to_symbol[count] = Some(canonical.to_string());
        Ok(count)
    }

    pub fn lookup(&self, symbol: &str, itype: &InstrumentType) -> Option<&SymbolId> {
        match itype {
            InstrumentType::Spot => self.spot_to_id.get(symbol),
            InstrumentType::Perp => self.perp_to_id.get(symbol),
            _ => None,
        }
    }

    pub fn get_symbol(&self, id: SymbolId) -> Option<&str> {
        self.to_symbol[id].as_deref()
    }
}

fn generate_aliases(base: &str, quote: &str, instrument: &InstrumentType) -> Vec<String> {
    let pair = format!("{}{}", base, quote);
    let pair_dash = format!("{}-{}", base, quote);
    let pair_slash = format!("{}/{}", base, quote);
    let pair_underscore = format!("{}_{}", base, quote);

    match instrument {
        InstrumentType::Spot | InstrumentType::Perp => vec![
            pair.clone(),            // BTCUSDT
            pair_dash.clone(),       // BTC-USDT
            pair_slash.clone(),      // BTC/USDT
            pair_underscore.clone(), // BTC_USDT
        ],
        _ => vec![],
    }
}

// Static registry - loads on first access
pub static REGISTRY: Lazy<SymbolRegistry> = Lazy::new(|| {
    // Get path from env var or use default
    let default_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("configs/symbols.yaml");

    let path = std::env::var("SYMBOL_CONFIG")
        .unwrap_or_else(|_| default_path.to_str().unwrap().to_string());

    SymbolRegistry::from_config(&path)
        .unwrap_or_else(|e| panic!("Failed to load symbol registry from '{}': {}", path, e))
});
