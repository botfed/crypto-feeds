use once_cell::sync::Lazy;
use rustc_hash::FxHashMap;
use serde::Deserialize;
use std::collections::HashSet;
use std::path::Path;
use std::sync::Mutex;

use crate::market_data::InstrumentType;

pub const MAX_SYMBOLS: usize = 1_000;
pub type SymbolId = usize;

const QUOTE_CURRENCIES: &[&str] = &["USDT", "USDC", "USD", "ETH", "WETH"];
const INSTRUMENT_TYPES: &[InstrumentType] = &[InstrumentType::Spot, InstrumentType::Perp];

#[derive(Deserialize)]
struct Config {
    base_assets: Vec<String>,
}

pub struct SymbolRegistry {
    to_symbol: [Option<String>; MAX_SYMBOLS],
    spot_to_id: FxHashMap<String, SymbolId>,
    perp_to_id: FxHashMap<String, SymbolId>,
}

/// Extra base assets seeded by the application before first REGISTRY access.
/// Call `seed_extra_bases()` before any code touches `REGISTRY`.
static EXTRA_BASES: Mutex<Option<Vec<String>>> = Mutex::new(None);

/// Register additional base assets to be included when the registry initializes.
/// Must be called **before** the first access to `REGISTRY`.
pub fn seed_extra_bases(bases: Vec<String>) {
    if let Ok(mut guard) = EXTRA_BASES.lock() {
        *guard = Some(bases);
    }
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
        Self::from_config_with_extras(path, &[])
    }

    pub fn from_config_with_extras(path: &str, extra_bases: &[String]) -> Result<Self, String> {
        let content =
            std::fs::read_to_string(path).map_err(|e| format!("Failed to read {}: {}", path, e))?;

        let config: Config =
            serde_yaml::from_str(&content).map_err(|e| format!("Failed to parse YAML: {}", e))?;

        // Merge symbols.yaml bases with extras, deduplicated
        let mut seen = HashSet::new();
        let mut all_bases = Vec::new();
        for base in config.base_assets.iter().chain(extra_bases.iter()) {
            let upper = base.to_uppercase();
            if seen.insert(upper.clone()) {
                all_bases.push(upper);
            }
        }

        let mut reg = Self::new();
        reg.register_bases(&all_bases)?;
        Ok(reg)
    }

    fn register_bases(&mut self, bases: &[String]) -> Result<(), String> {
        for base in bases {
            for quote in QUOTE_CURRENCIES {
                for &instrument in INSTRUMENT_TYPES {
                    let canonical = format!("{}-{}-{}", instrument.as_str(), base, quote);

                    // Skip if already registered (dedup)
                    let map = match instrument {
                        InstrumentType::Spot => &self.spot_to_id,
                        InstrumentType::Perp => &self.perp_to_id,
                        _ => continue,
                    };
                    if map.contains_key(&canonical) {
                        continue;
                    }

                    let id = self.register_symbol(&canonical)?;

                    match instrument {
                        InstrumentType::Spot => {
                            self.spot_to_id.insert(canonical, id);
                        }
                        InstrumentType::Perp => {
                            self.perp_to_id.insert(canonical, id);
                        }
                        _ => {}
                    }

                    let aliases = generate_aliases(base, quote, &instrument);
                    for alias in aliases {
                        match instrument {
                            InstrumentType::Spot => {
                                self.spot_to_id.insert(alias, id);
                            }
                            InstrumentType::Perp => {
                                self.perp_to_id.insert(alias, id);
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        Ok(())
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
            pair,
            pair_dash,
            pair_slash,
            pair_underscore,
        ],
        _ => vec![],
    }
}

// Static registry - loads on first access, merging symbols.yaml + seeded extras
pub static REGISTRY: Lazy<SymbolRegistry> = Lazy::new(|| {
    let default_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("configs/symbols.yaml");

    let path = std::env::var("SYMBOL_CONFIG")
        .unwrap_or_else(|_| default_path.to_str().unwrap().to_string());

    let extra = EXTRA_BASES.lock().ok()
        .and_then(|mut guard| guard.take())
        .unwrap_or_default();

    SymbolRegistry::from_config_with_extras(&path, &extra)
        .unwrap_or_else(|e| panic!("Failed to load symbol registry from '{}': {}", path, e))
});
