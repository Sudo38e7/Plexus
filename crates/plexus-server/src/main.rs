use anyhow::Result;
use bytes::Bytes;
use plexus_core::handshake::{
    compute_session_id, parse_client_nonce_and_token, parse_stats_req, parse_timestamp,
};
use plexus_core::{Packet, PacketKind, FEC_DATA_SHARDS, FEC_PARITY_SHARDS};
use plexus_fec::FecCodec;
use socket2::{Domain, Protocol, Socket, Type};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Instant;
use tokio::net::UdpSocket;

struct FecGroup {
    shards: Vec<Option<Vec<u8>>>,
    data_count: usize,
}
impl FecGroup {
    fn new(k: usize, m: usize) -> Self { Self { shards: vec![None; k + m], data_count: 0 } }
    fn insert(&mut self, idx: usize, payload: Vec<u8>, k: usize) -> bool {
        if idx >= self.shards.len() || self.shards[idx].is_some() { return false; }
        self.shards[idx] = Some(payload);
        if idx < k { self.data_count += 1; }
        true
    }
}

#[derive(Default)]
struct ChannelState {
    groups: HashMap<u16, FecGroup>,
    non_fec_data: u32,
}

#[derive(Default)]
struct SessionState {
    channels: HashMap<SocketAddr, ChannelState>,
}

fn create_udp_socket(addr: SocketAddr) -> Result<UdpSocket> {
    let domain = if addr.is_ipv4() { Domain::IPV4 } else { Domain::IPV6 };
    let sock = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
    sock.set_recv_buffer_size(64 * 1024 * 1024)?;
    sock.set_send_buffer_size(64 * 1024 * 1024)?;
    sock.set_nonblocking(true)?;
    sock.bind(&addr.into())?;
    let std_sock: std::net::UdpSocket = sock.into();
    Ok(UdpSocket::from_std(std_sock)?)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let bind_str = std::env::args().nth(1).unwrap_or_else(|| "0.0.0.0:51820".into());
    let addr: SocketAddr = bind_str.parse()?;
    let sock = create_udp_socket(addr)?;
    tracing::info!("Plexus server on {} | FEC K={} M={}", addr, FEC_DATA_SHARDS, FEC_PARITY_SHARDS);

    let codec = FecCodec::new(FEC_DATA_SHARDS, FEC_PARITY_SHARDS)?;
    let k = FEC_DATA_SHARDS;
    let m = FEC_PARITY_SHARDS;

    let mut sessions: HashMap<u64, SessionState> = HashMap::new();
    let mut addr_to_session: HashMap<SocketAddr, u64> = HashMap::new();

    let mut buf = vec![0u8; 65535];
    let mut last_report = Instant::now();
    let mut total_pkts: u64 = 0;
    let mut total_bytes: u64 = 0;

    loop {
        let (n, peer) = sock.recv_from(&mut buf).await?;
        total_pkts += 1;
        total_bytes += n as u64;
        let raw = Bytes::copy_from_slice(&buf[..n]);
        let Some(pkt) = Packet::decode(raw) else { continue };

        match pkt.header.kind {
            PacketKind::Hello => {
                let Some((client_nonce, session_token)) = parse_client_nonce_and_token(&pkt.payload) else { continue };
                let server_nonce: u64 = rand::random();
                let sid = compute_session_id(client_nonce, server_nonce);
                addr_to_session.insert(peer, session_token);
                sessions.entry(session_token).or_default();  // новый token → свежая сессия
                sock.send_to(&Packet::hello_ack(server_nonce, sid).encode(), peer).await?;
                tracing::info!("HELLO from {} -> token {:#018x} | sid {:#018x}", peer, session_token, sid);
            }
            PacketKind::Ping => {
                if let Some(ts) = parse_timestamp(&pkt.payload) {
                    sock.send_to(&Packet::pong(ts).encode(), peer).await?;
                }
            }
            PacketKind::Data | PacketKind::FecParity => {
                let Some(&token) = addr_to_session.get(&peer) else { continue };
                let sess = sessions.entry(token).or_default();
                let ch = sess.channels.entry(peer).or_default();
                if pkt.header.fec_group == 0 {
                    if pkt.header.kind == PacketKind::Data { ch.non_fec_data += 1; }
                } else {
                    let g = ch.groups.entry(pkt.header.fec_group).or_insert_with(|| FecGroup::new(k, m));
                    g.insert(pkt.header.flags as usize, pkt.payload.to_vec(), k);
                }
            }
            PacketKind::StatsReq => {
                let expected = parse_stats_req(&pkt.payload).unwrap_or(0);
                let Some(&token) = addr_to_session.get(&peer) else { continue };
                let sess = sessions.entry(token).or_default();
                let mut raw_received: u32 = 0;
                let mut recovered: u32 = 0;
                let channel_count = sess.channels.len();
                // НЕ очищаем state — ретраи безопасны
                for (_a, ch) in sess.channels.iter() {
                    raw_received += ch.non_fec_data;
                    for (_gid, g) in ch.groups.iter() {
                        if g.data_count == k {
                            raw_received += k as u32;
                        } else {
                            let missing = k - g.data_count;
                            // пробуем восстановить на клоне, чтобы не портить состояние
                            let mut shards_copy = g.shards.clone();
                            if missing <= m && codec.reconstruct(&mut shards_copy) {
                                raw_received += g.data_count as u32;
                                recovered += missing as u32;
                            } else {
                                raw_received += g.data_count as u32;
                            }
                        }
                    }
                }
                sock.send_to(&Packet::stats_resp(raw_received, recovered, expected).encode(), peer).await?;
                tracing::info!(
                    "STATS from {} (token {:#018x}): channels={} raw={} recovered={} expected={}",
                    peer, token, channel_count, raw_received, recovered, expected
                );
            }
            _ => {}
        }

        if last_report.elapsed().as_secs() >= 5 {
            tracing::info!("stats: pkts={}, bytes={}, sessions={}", total_pkts, total_bytes, sessions.len());
            total_pkts = 0; total_bytes = 0;
            last_report = Instant::now();
        }
    }
}