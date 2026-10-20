use plexus_core::{ChannelId, ChannelStats};
use std::collections::HashMap;
use std::time::Instant;

pub struct Scheduler { weights: HashMap<ChannelId, f64>, min_health: f64 }

impl Default for Scheduler {
    fn default() -> Self { Self { weights: HashMap::new(), min_health: 5.0 } }
}

impl Scheduler {
    pub fn new() -> Self { Self::default() }

    pub fn rebalance(&mut self, stats: &HashMap<ChannelId, ChannelStats>, now: Instant) {
        let mut active: Vec<(ChannelId, f64)> = stats.iter()
            .filter(|(_,s)| s.is_alive(now) && s.health() >= self.min_health)
            .map(|(id,s)| (*id, s.health())).collect();
        if active.is_empty() { self.weights.clear(); return; }
        let total: f64 = active.iter().map(|(_,h)| *h).sum();
        self.weights.clear();
        for (id,h) in active.drain(..) { self.weights.insert(id, h / total); }
    }

    pub fn pick(&self) -> Option<ChannelId> {
        if self.weights.is_empty() { return None; }
        let r = rand_f64();
        let mut acc = 0.0;
        for (id,w) in &self.weights {
            acc += w;
            if r <= acc { return Some(*id); }
        }
        self.weights.keys().next().copied()
    }

    pub fn weight(&self, id: ChannelId) -> f64 { *self.weights.get(&id).unwrap_or(&0.0) }
}

fn rand_f64() -> f64 {
    use std::cell::Cell;
    thread_local! { static STATE: Cell<u64> = Cell::new(0x9E3779B97F4A7C15); }
    STATE.with(|s| {
        let mut x = s.get();
        x ^= x << 13; x ^= x >> 7; x ^= x << 17;
        s.set(x);
        (x >> 11) as f64 / (1u64 << 53) as f64
    })
}