use crate::{Channel, ChannelId};
use std::collections::HashMap;

pub struct Session {
    pub id: u64,
    pub channels: HashMap<ChannelId, Channel>,
    next_seq: u32,
}

impl Session {
    pub fn new(id: u64) -> Self { Self { id, channels: HashMap::new(), next_seq: 1 } }
    pub fn add_channel(&mut self, ch: Channel) { self.channels.insert(ch.id, ch); }
    pub fn next_seq(&mut self) -> u32 {
        let s = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);
        s
    }
    pub fn best_channel(&self, now: std::time::Instant) -> Option<ChannelId> {
        self.channels.values()
            .filter(|c| c.stats.is_alive(now))
            .max_by(|a, b| a.stats.health().partial_cmp(&b.stats.health()).unwrap())
            .map(|c| c.id)
    }
}