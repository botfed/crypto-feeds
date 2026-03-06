use crate::mappers::symbol_mapper::SymbolMapper;
use crate::market_data::InstrumentType;
use anyhow::Result;

#[derive(Clone)]
pub struct KucoinMapper;

/// KuCoin futures uses XBT instead of BTC.
fn canonical_to_native(base: &str) -> &str {
    match base {
        "BTC" => "XBT",
        _ => base,
    }
}

fn native_to_canonical(base: &str) -> &str {
    match base {
        "XBT" => "BTC",
        _ => base,
    }
}

impl SymbolMapper for KucoinMapper {
    fn normalize(&self, native: &str, itype: InstrumentType) -> Result<String> {
        let (base, quote) = self.parse(native, itype)?;
        Ok(format!("{}_{}_{}", itype.as_str(), base, quote))
    }
    fn denormalize(&self, normalized: &str, itype: InstrumentType) -> Result<String> {
        let parts: Vec<&str> = normalized.split('_').collect();
        if parts.len() < 2 {
            anyhow::bail!("Invalid normalized symbol: {}", normalized);
        }
        let (base, quote) = if parts.len() == 3 {
            (parts[1], parts[2])
        } else {
            (parts[0], parts[1])
        };
        match itype {
            InstrumentType::Spot => Ok(format!("{}-{}", base, quote).to_uppercase()),
            InstrumentType::Perp => {
                let upper_base = base.to_uppercase();
                let native_base = canonical_to_native(&upper_base);
                Ok(format!("{}{}M", native_base, quote).to_uppercase())
            }
            _ => anyhow::bail!("Type not implemented {:?}", itype),
        }
    }
    fn parse(&self, native: &str, itype: InstrumentType) -> Result<(String, String)> {
        match itype {
            InstrumentType::Spot => {
                // KuCoin spot: "BTC-USDT"
                let parts: Vec<&str> = native.split('-').collect();
                if parts.len() == 2 {
                    Ok((parts[0].to_uppercase(), parts[1].to_uppercase()))
                } else {
                    anyhow::bail!("Could not parse KuCoin spot symbol: {}", native)
                }
            }
            InstrumentType::Perp => {
                // KuCoin perp: "XBTUSDTM" -> ("BTC", "USDT")
                let upper = native.to_uppercase();
                let without_m = upper
                    .strip_suffix('M')
                    .ok_or_else(|| anyhow::anyhow!("KuCoin perp symbol must end with M: {}", native))?;
                const QUOTES: &[&str] = &["USDT", "USDC", "USD"];
                for quote in QUOTES {
                    if let Some(base) = without_m.strip_suffix(quote) {
                        if !base.is_empty() {
                            let canonical = native_to_canonical(base);
                            return Ok((canonical.to_string(), quote.to_string()));
                        }
                    }
                }
                anyhow::bail!("Could not parse KuCoin perp symbol: {}", native)
            }
            _ => anyhow::bail!("Unsupported itype {:?}", itype),
        }
    }
    fn exchange(&self) -> &str {
        "kucoin"
    }
}
