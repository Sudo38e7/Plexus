//! Plexus core: packet format, channel abstraction, session state, handshake.

pub mod packet;
pub mod channel;
pub mod session;
pub mod handshake;

pub use packet::{Packet, PacketKind, HEADER_SIZE};
pub use channel::{Channel, ChannelId, ChannelStats};
pub use session::Session;

pub const PROTOCOL_VERSION: u8 = 1;
pub const MAX_PAYLOAD: usize = 1400;

/// FEC: K data-шардов в группе.
pub const FEC_DATA_SHARDS: usize = 10;
/// FEC: M parity-шардов на группу (переживает до M потерь).
pub const FEC_PARITY_SHARDS: usize = 3;