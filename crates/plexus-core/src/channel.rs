use std::time::{Duration, Instant};

pub type ChannelId = u8;

#[derive(Debug, Clone, Default)]
pub struct ChannelStats {
    pub rtt_ms: f64,
    pub loss_rate: f64,
    pub throughput_bps: u64,
    pub last_seen: Option<Instant>,
    pub consecutive_failures: u32,
}

impl ChannelStats {
    pub fn health(&self) -> f64 {
        let rtt_penalty = (self.rtt_ms / 500.0).min(1.0);
        let loss_penalty = self.loss_rate.min(1.0);
        let fail_penalty = (self.consecutive_failures as f64 / 5.0).min(1.0);
        ((1.0 - 0.4 * rtt_penalty - 0.5 * loss_penalty - 0.6 * fail_penalty).max(0.0)) * 100.0
    }

    pub fn is_alive(&self, now: Instant) -> bool {
        match self.last_seen {
            Some(t) => now.duration_since(t) < Duration::from_secs(3),
            None => false,
        }
    }
}

#[derive(Debug)]
pub struct Channel {
    pub id: ChannelId,
    pub name: String,
    pub stats: ChannelStats,
    pub weight: f64,
}

impl Channel {
    pub fn new(id: ChannelId, name: impl Into<String>) -> Self {
        Self { id, name: name.into(), stats: ChannelStats::default(), weight: 1.0 }
    }
}