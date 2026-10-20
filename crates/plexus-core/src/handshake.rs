use bytes::{Buf, BufMut, Bytes, BytesMut};

pub fn compute_session_id(client_nonce: u64, server_nonce: u64) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for v in [client_nonce, server_nonce] {
        h ^= v;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

pub fn parse_client_nonce_and_token(payload: &Bytes) -> Option<(u64, u64)> {
    if payload.len() < 16 { return None; }
    let mut b = payload.clone();
    let nonce = b.get_u64();
    let token = b.get_u64();
    Some((nonce, token))
}

pub fn parse_hello_ack(payload: &Bytes) -> Option<(u64, u64)> {
    if payload.len() < 16 { return None; }
    let mut b = payload.clone();
    let sn = b.get_u64();
    let sid = b.get_u64();
    Some((sn, sid))
}

pub fn parse_timestamp(payload: &Bytes) -> Option<u64> {
    if payload.len() < 8 { return None; }
    let mut b = payload.clone();
    Some(b.get_u64())
}

pub fn parse_stats_req(payload: &Bytes) -> Option<u32> {
    if payload.len() < 4 { return None; }
    let mut b = payload.clone();
    Some(b.get_u32())
}

pub fn parse_stats_resp(payload: &Bytes) -> Option<(u32, u32, u32)> {
    if payload.len() < 12 { return None; }
    let mut b = payload.clone();
    Some((b.get_u32(), b.get_u32(), b.get_u32()))
}

pub fn encode_u64(v: u64) -> Bytes {
    let mut b = BytesMut::with_capacity(8);
    b.put_u64(v);
    b.freeze()
}