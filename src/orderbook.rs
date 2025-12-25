use std::collections::BTreeMap;

pub struct OrderBook {
    pub bids: BTreeMap<ordered_float::OrderedFloat<f64>, f64>, // price -> size
    pub asks: BTreeMap<ordered_float::OrderedFloat<f64>, f64>,
}

impl OrderBook {
    pub fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }

    pub fn update_bids(&mut self, updates: Vec<(String, f64)>) {
        for (price, size) in updates {
            if let Ok(price_f64) = price.parse::<f64>() {
                let key = ordered_float::OrderedFloat(price_f64);
                if size == 0.0 {
                    // Size 0 means remove this level
                    self.bids.remove(&key);
                } else {
                    self.bids.insert(key, size);
                }
            }
        }
    }
    pub fn update_asks(&mut self, updates: Vec<(String, f64)>) {
        for (price, size) in updates {
            if let Ok(price_f64) = price.parse::<f64>() {
                let key = ordered_float::OrderedFloat(price_f64);
                if size == 0.0 {
                    // Size 0 means remove this level
                    self.asks.remove(&key);
                } else {
                    self.asks.insert(key, size);
                }
            }
        }
    }

    pub fn best_bid(&self) -> Option<(f64, f64)> {
        // BTreeMap is sorted, get highest bid
        self.bids
            .iter()
            .next_back()
            .map(|(price, &size)| (price.0, size))
    }

    pub fn best_ask(&self) -> Option<(f64, f64)> {
        // Get lowest ask
        self.asks
            .iter()
            .next()
            .map(|(price, &size)| (price.0, size))
    }
}
