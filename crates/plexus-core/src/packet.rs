use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PacketHeader {
    pub version: u8,
    pub kind: PacketKind,
    pub channel_id: u8,
    pub flags: u8,
    pub seq: u32,
    pub ack: u32,
    pub payload_len: u16,
    pub fec_group: u16,
}

pub const HEADER_SIZE: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PacketKind {
    Data = 0, Ack = 1, FecParity = 2, Ping = 3, Pong = 4,
    Hello = 5, HelloAck = 6, StatsReq = 7, StatsResp = 8,
}

impl PacketKind {
    pub fn from_u8(b: u8) -> Option<Self> {
        Some(match b {
            0 => PacketKind::Data, 1 => PacketKind::Ack,
            2 => PacketKind::FecParity, 3 => PacketKind::Ping, 4 => PacketKind::Pong,
            5 => PacketKind::Hello, 6 => PacketKind::HelloAck,
            7 => PacketKind::StatsReq, 8 => PacketKind::StatsResp,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Packet { pub header: PacketHeader, pub payload: Bytes }

impl Packet {
    fn with_kind_full(kind: PacketKind, channel_id: u8, flags: u8, seq: u32, ack: u32, fec_group: u16, payload: Bytes) -> Self {
        Self { header: PacketHeader {
            version: super::PROTOCOL_VERSION, kind, channel_id, flags, seq, ack,
            payload_len: payload.len() as u16, fec_group,
        }, payload }
    }

    pub fn data(channel_id: u8, seq: u32, ack: u32, payload: Bytes) -> Self {
        Self::with_kind_full(PacketKind::Data, channel_id, 0, seq, ack, 0, payload)
    }
    pub fn data_fec(channel_id: u8, seq: u32, ack: u32, group_id: u16, shard_idx: u8, payload: Bytes) -> Self {
        Self::with_kind_full(PacketKind::Data, channel_id, shard_idx, seq, ack, group_id, payload)
    }
    pub fn fec_parity(group_id: u16, seq: u32, shard_idx: u8, payload: Bytes) -> Self {
        Self::with_kind_full(PacketKind::FecParity, 0, shard_idx, seq, 0, group_id, payload)
    }
    pub fn ack(seq: u32) -> Self { Self::with_kind_full(PacketKind::Ack, 0, 0, 0, seq, 0, Bytes::new()) }
    pub fn ping(ts_ns: u64) -> Self {
        let mut b = BytesMut::with_capacity(8); b.put_u64(ts_ns);
        Self::with_kind_full(PacketKind::Ping, 0, 0, 0, 0, 0, b.freeze())
    }
    pub fn pong(ts_ns: u64) -> Self {
        let mut b = BytesMut::with_capacity(8); b.put_u64(ts_ns);
        Self::with_kind_full(PacketKind::Pong, 0, 0, 0, 0, 0, b.freeze())
    }
    pub fn hello(client_nonce: u64, session_token: u64) -> Self {
        let mut b = BytesMut::with_capacity(16);
        b.put_u64(client_nonce);
        b.put_u64(session_token);
        Self::with_kind_full(PacketKind::Hello, 0, 0, 0, 0, 0, b.freeze())
    }
    pub fn hello_ack(server_nonce: u64, session_id: u64) -> Self {
        let mut b = BytesMut::with_capacity(16);
        b.put_u64(server_nonce); b.put_u64(session_id);
        Self::with_kind_full(PacketKind::HelloAck, 0, 0, 0, 0, 0, b.freeze())
    }
    pub fn stats_req(expected: u32) -> Self {
        let mut b = BytesMut::with_capacity(4); b.put_u32(expected);
        Self::with_kind_full(PacketKind::StatsReq, 0, 0, 0, 0, 0, b.freeze())
    }
    pub fn stats_resp(raw: u32, recovered: u32, expected: u32) -> Self {
        let mut b = BytesMut::with_capacity(12);
        b.put_u32(raw); b.put_u32(recovered); b.put_u32(expected);
        Self::with_kind_full(PacketKind::StatsResp, 0, 0, 0, 0, 0, b.freeze())
    }

    pub fn encode(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(HEADER_SIZE + self.payload.len());
        buf.put_u8(self.header.version);
        buf.put_u8(self.header.kind as u8);
        buf.put_u8(self.header.channel_id);
        buf.put_u8(self.header.flags);
        buf.put_u32(self.header.seq);
        buf.put_u32(self.header.ack);
        buf.put_u16(self.header.payload_len);
        buf.put_u16(self.header.fec_group);
        buf.put_slice(&self.payload);
        buf
    }

    pub fn decode(mut buf: Bytes) -> Option<Self> {
        if buf.len() < HEADER_SIZE { return None; }
        let version = buf.get_u8();
        if version != super::PROTOCOL_VERSION { return None; }
        let kind = PacketKind::from_u8(buf.get_u8())?;
        let channel_id = buf.get_u8();
        let flags = buf.get_u8();
        let seq = buf.get_u32();
        let ack = buf.get_u32();
        let payload_len = buf.get_u16();
        let fec_group = buf.get_u16();
        if buf.len() < payload_len as usize { return None; }
        let payload = buf.split_to(payload_len as usize);
        Some(Self {
            header: PacketHeader { version, kind, channel_id, flags, seq, ack, payload_len, fec_group },
            payload,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn roundtrip() {
        let p = Packet::data(1, 42, 41, Bytes::from_static(b"hi"));
        let d = Packet::decode(p.encode().freeze()).unwrap();
        assert_eq!(d.header.seq, 42);
    }
    #[test]
    fn hello_with_token() {
        let p = Packet::hello(0xAAAA, 0xBBBB);
        assert_eq!(p.payload.len(), 16);
    }
}