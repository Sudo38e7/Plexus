use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use clap::{Parser, Subcommand};
use plexus_core::handshake::{compute_session_id, parse_hello_ack, parse_stats_resp, parse_timestamp};
use plexus_core::{Packet, PacketKind, FEC_DATA_SHARDS, FEC_PARITY_SHARDS};
use plexus_fec::FecCodec;
use socket2::{Domain, Protocol, Socket, Type};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::UdpSocket;

#[derive(Parser)]
#[command(name = "plexus", version, about = "Plexus network aggregator")]
struct Cli { #[command(subcommand)] command: Commands }

#[derive(Subcommand)]
enum Commands {
    Handshake { #[arg(long, default_value = "127.0.0.1:51820")] server: String },
    Ping {
        #[arg(long, default_value = "127.0.0.1:51820")] server: String,
        #[arg(long, default_value_t = 10)] count: u32,
        #[arg(long, default_value_t = 500)] timeout_ms: u64,
    },
    Bench {
        #[arg(long)] servers: String,
        #[arg(long, default_value_t = 5000)] count: u32,
        #[arg(long, default_value_t = 1400)] size: usize,
        #[arg(long, default_value_t = false)] fec: bool,
        #[arg(long)] rate_mbps: Option<u64>,
        #[arg(long)] weights: Option<String>,
    },
    /// Live TUI dashboard. Press Q to quit.
    Tui {
        #[arg(long, default_value_t = 6)] channels: u32,
        #[arg(long, default_value_t = 20)] rate_mbps: u64,
        #[arg(long, default_value_t = 0.05)] loss: f64,
        #[arg(long, default_value_t = 20)] delay_ms: u64,
    },
    /// Full self-contained lab test.
    Lab {
        #[arg(long, default_value_t = 6)] channels: u32,
        #[arg(long, default_value_t = 20)] rate_mbps: u64,
        #[arg(long, default_value_t = 0.05)] loss: f64,
        #[arg(long, default_value_t = 20)] delay_ms: u64,
        #[arg(long, default_value_t = 12000)] count: u32,
        #[arg(long, default_value_t = 1400)] size: usize,
        #[arg(long, default_value_t = false)] fec: bool,
        #[arg(long, default_value_t = false)] no_cleanup: bool,
    },
    /// Real-time network adapters monitor. Press Q to quit.
    Monitor {
        #[arg(long, default_value_t = 500)] interval_ms: u64,
    },
}

fn now_ns() -> u64 { SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64 }

fn create_udp_socket() -> Result<UdpSocket> {
    let sock = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    sock.set_recv_buffer_size(4 * 1024 * 1024)?;
    sock.set_send_buffer_size(4 * 1024 * 1024)?;
    sock.set_nonblocking(true)?;
    let addr: SocketAddr = "0.0.0.0:0".parse()?;
    sock.bind(&addr.into())?;
    let std_sock: std::net::UdpSocket = sock.into();
    Ok(UdpSocket::from_std(std_sock)?)
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    if !matches!(cli.command, Commands::Tui { .. } | Commands::Monitor { .. }) {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .init();
    }
    match cli.command {
        Commands::Handshake { server } => handshake(&server).await,
        Commands::Ping { server, count, timeout_ms } => ping(&server, count, timeout_ms).await,
        Commands::Bench { servers, count, size, fec, rate_mbps, weights } =>
            bench(&servers, count, size, fec, rate_mbps, weights.as_deref()).await,
        Commands::Lab { channels, rate_mbps, loss, delay_ms, count, size, fec, no_cleanup } =>
            lab(channels, rate_mbps, loss, delay_ms, count, size, fec, no_cleanup).await,
        Commands::Tui { channels, rate_mbps, loss, delay_ms } =>
            tui_mode(channels, rate_mbps, loss, delay_ms).await,
        Commands::Monitor { interval_ms } =>
            monitor_mode(interval_ms).await,
    }
}

async fn do_handshake(sock: &UdpSocket, session_token: u64) -> Result<()> {
    let client_nonce: u64 = rand::random();
    let hello = Packet::hello(client_nonce, session_token).encode().freeze();
    let mut buf = [0u8; 1024];
    for attempt in 1..=8u32 {
        sock.send(&hello).await.with_context(|| format!("hello send (attempt {})", attempt))?;
        let deadline = Instant::now() + Duration::from_millis(500);
        while Instant::now() < deadline {
            let rem = deadline.saturating_duration_since(Instant::now());
            match tokio::time::timeout(rem, sock.recv(&mut buf)).await {
                Ok(Ok(n)) => {
                    if let Some(p) = Packet::decode(Bytes::copy_from_slice(&buf[..n])) {
                        if p.header.kind == PacketKind::HelloAck {
                            let (sn, sid) = parse_hello_ack(&p.payload)
                                .ok_or_else(|| anyhow::anyhow!("bad HelloAck payload"))?;
                            anyhow::ensure!(compute_session_id(client_nonce, sn) == sid, "session mismatch");
                            return Ok(());
                        }
                    }
                }
                _ => break,
            }
        }
    }
    anyhow::bail!("handshake failed after 8 attempts")
}

async fn handshake(server: &str) -> Result<()> {
    let peer: SocketAddr = server.parse()?;
    let sock = create_udp_socket()?;
    sock.connect(peer).await?;
    do_handshake(&sock, rand::random()).await?;
    println!("HELLO OK");
    Ok(())
}

async fn ping(server: &str, count: u32, timeout_ms: u64) -> Result<()> {
    let peer: SocketAddr = server.parse()?;
    let sock = create_udp_socket()?;
    sock.connect(peer).await?;
    let mut rtts = Vec::new();
    for i in 0..count {
        let ts = now_ns();
        let start = Instant::now();
        sock.send(&Packet::ping(ts).encode()).await?;
        let mut buf = [0u8; 1024];
        let deadline = Instant::now() + Duration::from_millis(timeout_ms);
        loop {
            let rem = deadline.saturating_duration_since(Instant::now());
            if rem.is_zero() { eprintln!("[{:>2}] timeout", i+1); break; }
            match tokio::time::timeout(rem, sock.recv(&mut buf)).await {
                Ok(Ok(n)) => {
                    if let Some(d) = Packet::decode(Bytes::copy_from_slice(&buf[..n])) {
                        if d.header.kind == PacketKind::Pong {
                            if let Some(echo) = parse_timestamp(&d.payload) {
                                if echo == ts {
                                    let rtt = start.elapsed().as_secs_f64() * 1000.0;
                                    rtts.push(rtt);
                                    println!("[{:>2}] rtt = {:.3} ms", i+1, rtt);
                                    break;
                                }
                            }
                        }
                    }
                }
                _ => { eprintln!("[{:>2}] timeout", i+1); break; }
            }
        }
    }
    if !rtts.is_empty() {
        let min = rtts.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = rtts.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let avg: f64 = rtts.iter().sum::<f64>() / rtts.len() as f64;
        println!("\n--- rtt summary ---");
        println!("sent {} / got {}: min={:.3} avg={:.3} max={:.3} ms", count, rtts.len(), min, avg, max);
    }
    Ok(())
}

async fn send_paced(
    sock: &UdpSocket, encoded: BytesMut, count: &mut u64,
    rate_bps: Option<u64>, bits_per_pkt: u64, start: Instant,
) -> Result<()> {
    *count += 1;
    if let Some(bps) = rate_bps {
        let target_ns = (*count).saturating_mul(bits_per_pkt).saturating_mul(1_000_000_000) / bps;
        let target = start + Duration::from_nanos(target_ns);
        let now = Instant::now();
        if target > now { tokio::time::sleep(target - now).await; }
    }
    sock.send(&encoded).await?;
    Ok(())
}

fn distribute_weights(total: usize, weights: &[f64]) -> Vec<usize> {
    let sum: f64 = weights.iter().sum();
    if sum == 0.0 { return vec![total / weights.len(); weights.len()]; }
    let mut alloc: Vec<usize> = weights.iter().map(|w| ((*w / sum) * total as f64) as usize).collect();
    let assigned: usize = alloc.iter().sum();
    let mut remainder = total - assigned;
    let mut i = 0;
    while remainder > 0 { alloc[i % weights.len()] += 1; remainder -= 1; i += 1; }
    alloc
}

async fn bench(
    servers: &str, count: u32, size: usize, fec: bool,
    rate_mbps: Option<u64>, weights: Option<&str>,
) -> Result<()> {
    let server_list: Vec<&str> = servers.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();
    if server_list.is_empty() { anyhow::bail!("no servers provided"); }
    let n_channels = server_list.len();
    let weights_vec: Vec<f64> = match weights {
        Some(w) => w.split(',').map(|x| x.trim().parse::<f64>().unwrap_or(1.0)).collect(),
        None => vec![1.0; n_channels],
    };
    if weights_vec.len() != n_channels { anyhow::bail!("weights count must match servers count"); }

    let session_token: u64 = rand::random();
    let mut channels: Vec<Arc<UdpSocket>> = Vec::new();
    for s in &server_list {
        let peer: SocketAddr = s.parse()?;
        let sock = create_udp_socket()?;
        sock.connect(peer).await?;
        do_handshake(&sock, session_token).await?;
        channels.push(Arc::new(sock));
    }

    let k = FEC_DATA_SHARDS;
    let m = FEC_PARITY_SHARDS;
    let effective_count = if fec { ((count as usize) / k) * k } else { count as usize };
    if effective_count == 0 { anyhow::bail!("count too small"); }
    let total_groups = if fec { effective_count / k } else { effective_count };
    let per_channel_alloc = distribute_weights(total_groups, &weights_vec);

    println!(
        "bench: {} data-units x {} bytes | channels={} {:?} | fec={} (K={} M={}) | rate={:?} Mbps/ch",
        effective_count, size, n_channels, per_channel_alloc, fec, k, m, rate_mbps
    );

    let codec = if fec { Some(Arc::new(FecCodec::new(k, m)?)) } else { None };
    let payload_template = vec![0xABu8; size];
    let bits_per_pkt = (size + 16) as u64 * 8;
    let rate_bps = rate_mbps.map(|r| r * 1_000_000u64);
    let start = Instant::now();

    let mut handles = Vec::new();
    for (ch_idx, sock) in channels.iter().enumerate() {
        let sock = Arc::clone(sock);
        let payload = payload_template.clone();
        let codec = codec.as_ref().map(Arc::clone);
        let units = per_channel_alloc[ch_idx];
        let handle: tokio::task::JoinHandle<Result<()>> = tokio::spawn(async move {
            let start = Instant::now();
            let mut pkt_count: u64 = 0;
            if let Some(codec) = codec {
                let mut seq: u32 = (ch_idx as u32) << 24;
                let mut group_id: u16 = ((ch_idx + 1) as u16) * 1000;
                for _ in 0..units {
                    let mut data_shards = Vec::with_capacity(k);
                    for _ in 0..k { data_shards.push(payload.clone()); }
                    let parity = codec.encode(&data_shards)?;
                    for (i, shard) in data_shards.iter().enumerate() {
                        let encoded = Packet::data_fec(ch_idx as u8 + 1, seq, 0, group_id, i as u8, Bytes::from(shard.clone())).encode();
                        send_paced(&sock, encoded, &mut pkt_count, rate_bps, bits_per_pkt, start).await?;
                        seq += 1;
                    }
                    for (j, shard) in parity.iter().enumerate() {
                        let encoded = Packet::fec_parity(group_id, seq, (k + j) as u8, Bytes::from(shard.clone())).encode();
                        send_paced(&sock, encoded, &mut pkt_count, rate_bps, bits_per_pkt, start).await?;
                        seq += 1;
                    }
                    group_id = group_id.wrapping_add(1);
                    if group_id == 0 { group_id = 1; }
                }
            } else {
                for i in 0..units as u32 {
                    let encoded = Packet::data(ch_idx as u8 + 1, i, 0, Bytes::copy_from_slice(&payload)).encode();
                    send_paced(&sock, encoded, &mut pkt_count, rate_bps, bits_per_pkt, start).await?;
                }
            }
            Ok(())
        });
        handles.push(handle);
    }
    for h in handles { h.await??; }
    let send_elapsed = start.elapsed().as_secs_f64();

    tokio::time::sleep(Duration::from_millis(700)).await;

    let expected = effective_count as u32;
    let stats_sock = &channels[0];
    let mut stats_pkt = None;
    let overall_deadline = Instant::now() + Duration::from_secs(15);
    let mut buf = [0u8; 65535];
    while Instant::now() < overall_deadline {
        stats_sock.send(&Packet::stats_req(expected).encode()).await?;
        if let Ok(Ok(n)) = tokio::time::timeout(Duration::from_millis(400), stats_sock.recv(&mut buf)).await {
            if let Some(p) = Packet::decode(Bytes::copy_from_slice(&buf[..n])) {
                if p.header.kind == PacketKind::StatsResp { stats_pkt = Some(p); break; }
            }
        }
    }
    let resp = stats_pkt.context("no stats_resp")?;
    let (raw, recovered, expected) = parse_stats_resp(&resp.payload)
        .ok_or_else(|| anyhow::anyhow!("bad stats_resp payload"))?;
    let total_elapsed = start.elapsed().as_secs_f64();

    let wire_packets = if fec {
        (effective_count as f64 / k as f64) * (k + m) as f64
    } else {
        effective_count as f64
    };
    let wire_bytes = wire_packets * (size + 16) as f64;
    let delivered = raw + recovered;
    let useful_bytes = delivered as f64 * size as f64;
    let wire_mbps = wire_bytes * 8.0 / send_elapsed / 1e6;
    let useful_mbps = useful_bytes * 8.0 / send_elapsed / 1e6;
    let delivery_mbps = useful_bytes * 8.0 / total_elapsed / 1e6;
    let pct = if expected > 0 { delivered as f64 / expected as f64 * 100.0 } else { 0.0 };

    println!("\n--- bench result (channels={}, fec={}, rate={:?} Mbps/ch) ---", n_channels, fec, rate_mbps);
    println!("send elapsed        = {:.3} s", send_elapsed);
    println!("total elapsed       = {:.3} s", total_elapsed);
    println!("throughput (wire)   = {:.2} Mbit/s (during send)", wire_mbps);
    println!("throughput (useful) = {:.2} Mbit/s   <-- app-level (during send)", useful_mbps);
    println!("throughput delivery = {:.2} Mbit/s (end-to-end)", delivery_mbps);
    println!("expected            = {}", expected);
    println!("raw received        = {}", raw);
    println!("recovered via FEC   = {}", recovered);
    println!("delivered           = {}/{} = {:.2}%", delivered, expected, pct);
    Ok(())
}

#[cfg(windows)]
fn cleanup_stale() {
    for name in &["plexus-server.exe", "plexus-lossy-proxy.exe"] {
        let _ = std::process::Command::new("taskkill")
            .args(["/F", "/IM", name])
            .output();
    }
}

#[cfg(not(windows))]
fn cleanup_stale() {
    for pat in &["plexus-server", "plexus-lossy-proxy"] {
        let _ = std::process::Command::new("pkill")
            .args(["-f", pat])
            .output();
    }
}

async fn spawn_children(
    channels: u32, rate_mbps: u64, loss: f64, delay_ms: u64,
) -> Result<(Vec<tokio::process::Child>, Vec<u32>)> {
    let exe = std::env::current_exe()?;
    let exe_dir = exe.parent().context("no exe parent")?.to_path_buf();
    let sfx = std::env::consts::EXE_SUFFIX;
    let server_exe = exe_dir.join(format!("plexus-server{}", sfx));
    let proxy_exe = exe_dir.join(format!("plexus-lossy-proxy{}", sfx));
    anyhow::ensure!(server_exe.exists(), "server binary not found: {:?}", server_exe);
    anyhow::ensure!(proxy_exe.exists(), "proxy binary not found: {:?}", proxy_exe);

    println!("[lab] starting server on 127.0.0.1:51820 ...");
    let mut srv_cmd = tokio::process::Command::new(&server_exe);
    srv_cmd.arg("0.0.0.0:51820")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true);
    let server_child = srv_cmd.spawn().context("failed to spawn server")?;
    let mut children = vec![server_child];
    tokio::time::sleep(Duration::from_millis(700)).await;

    let mut ports = Vec::new();
    for i in 0..channels {
        let port = 51821u32 + i;
        println!("[lab] starting proxy on 127.0.0.1:{} ...", port);
        let mut cmd = tokio::process::Command::new(&proxy_exe);
        cmd.arg("--listen").arg(format!("0.0.0.0:{}", port))
            .arg("--target").arg("127.0.0.1:51820")
            .arg("--loss").arg(loss.to_string())
            .arg("--delay-ms").arg(delay_ms.to_string())
            .arg("--rate-mbps").arg(rate_mbps.to_string())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true);
        let c = cmd.spawn().with_context(|| format!("spawn proxy {}", port))?;
        children.push(c);
        ports.push(port);
        tokio::time::sleep(Duration::from_millis(80)).await;
    }
    tokio::time::sleep(Duration::from_millis(1200)).await;
    Ok((children, ports))
}

async fn lab(
    channels: u32, rate_mbps: u64, loss: f64, delay_ms: u64,
    count: u32, size: usize, fec: bool, no_cleanup: bool,
) -> Result<()> {
    if !no_cleanup {
        println!("[cleanup] killing stale plexus processes...");
        cleanup_stale();
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    let (mut children, ports) = spawn_children(channels, rate_mbps, loss, delay_ms).await?;
    let servers_str = ports.iter().map(|p| format!("127.0.0.1:{}", p)).collect::<Vec<_>>().join(",");
    println!();
    println!("=== Lab ready ===");
    println!("  server:  127.0.0.1:51820");
    println!("  proxies: {} (each {} Mbps, {:.1}% loss, {} ms delay)", servers_str, rate_mbps, loss * 100.0, delay_ms);
    println!();
    let result = bench(&servers_str, count, size, fec, Some(rate_mbps), None).await;
    println!();
    println!("[lab] shutting down ...");
    for c in children.iter_mut() { let _ = c.start_kill(); }
    for c in children.iter_mut() { let _ = c.wait().await; }
    result
}

// ============================================================================
// TUI mode
// ============================================================================

use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Alignment, Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Paragraph, Sparkline},
    Terminal,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

async fn tui_mode(channels: u32, rate_mbps: u64, loss: f64, delay_ms: u64) -> Result<()> {
    cleanup_stale();
    tokio::time::sleep(Duration::from_millis(400)).await;
    let (_children, ports) = spawn_children(channels, rate_mbps, loss, delay_ms).await?;

    let session_token: u64 = rand::random();
    let mut channel_sockets: Vec<Arc<UdpSocket>> = Vec::new();
    for &port in &ports {
        let peer: SocketAddr = format!("127.0.0.1:{}", port).parse()?;
        let sock = create_udp_socket()?;
        sock.connect(peer).await?;
        do_handshake(&sock, session_token).await?;
        channel_sockets.push(Arc::new(sock));
    }

    let per_channel_bytes: Vec<Arc<AtomicU64>> = (0..channels)
        .map(|_| Arc::new(AtomicU64::new(0)))
        .collect();
    let total_bytes = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let payload_template = vec![0xABu8; 1400];
    let bits_per_pkt = 1400u64 * 8;
    let rate_bps = rate_mbps * 1_000_000;

    let mut workers = Vec::new();
    for (i, sock) in channel_sockets.iter().enumerate() {
        let sock = Arc::clone(sock);
        let payload = payload_template.clone();
        let counter = Arc::clone(&per_channel_bytes[i]);
        let total = Arc::clone(&total_bytes);
        let stop = Arc::clone(&stop);

        let handle = tokio::spawn(async move {
            let start = Instant::now();
            let mut pkt_count: u64 = 0;
            while !stop.load(Ordering::Relaxed) {
                pkt_count += 1;
                if rate_bps > 0 {
                    let target_ns = pkt_count.saturating_mul(bits_per_pkt).saturating_mul(1_000_000_000) / rate_bps;
                    let target = start + Duration::from_nanos(target_ns);
                    let now = Instant::now();
                    if target > now { tokio::time::sleep(target - now).await; }
                }
                let p = Packet::data(i as u8 + 1, pkt_count as u32, 0, Bytes::copy_from_slice(&payload));
                if sock.send(&p.encode()).await.is_ok() {
                    counter.fetch_add(1400, Ordering::Relaxed);
                    total.fetch_add(1400, Ordering::Relaxed);
                }
            }
        });
        workers.push(handle);
    }

    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let start = Instant::now();
    let mut last_bytes = 0u64;
    let mut last_tick = Instant::now();
    let mut speed_history: Vec<u64> = vec![0; 60];
    let mut current_speed: f64 = 0.0;
    let mut per_channel_speed: Vec<f64> = vec![0.0; channels as usize];
    let mut last_per_channel: Vec<u64> = vec![0; channels as usize];
    let tick_rate = Duration::from_millis(200);

    let res: Result<()> = loop {
        terminal.draw(|f| {
            let size = f.area();
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(3),
                    Constraint::Length(1),
                    Constraint::Length(2),
                    Constraint::Min(8),
                    Constraint::Length(2),
                    Constraint::Length(4),
                    Constraint::Length(2),
                ])
                .split(size);

            let speed_text = format!(" {:.1} Mbit/s ", current_speed);
            let speed_para = Paragraph::new(speed_text)
                .style(Style::default().fg(Color::Green).add_modifier(Modifier::BOLD))
                .alignment(Alignment::Center);
            f.render_widget(speed_para, chunks[0]);

            let uptime = start.elapsed().as_secs();
            let uptime_str = format!("Uptime: {:02}:{:02}:{:02}   Channels: {}/{}   Total: {:.1} MiB",
                uptime / 3600, (uptime % 3600) / 60, uptime % 60,
                channels, channels,
                total_bytes.load(Ordering::Relaxed) as f64 / 1024.0 / 1024.0);
            f.render_widget(
                Paragraph::new(uptime_str).alignment(Alignment::Center).style(Style::default().fg(Color::Gray)),
                chunks[1],
            );

            f.render_widget(
                Paragraph::new("--- Channel breakdown ---------------------").style(Style::default().fg(Color::DarkGray)),
                chunks[2],
            );

            let mut lines: Vec<Line> = Vec::new();
            for i in 0..channels as usize {
                let mbps = per_channel_speed[i];
                let bar_len = ((mbps / rate_mbps as f64) * 25.0).min(25.0) as usize;
                let bar = format!("[ {:<25} ]", "=".repeat(bar_len));
                lines.push(Line::from(vec![
                    Span::styled(format!("[{:>2}] ", i + 1), Style::default().fg(Color::Cyan)),
                    Span::styled(bar, Style::default().fg(Color::Green)),
                    Span::raw(format!("  {:.1} Mbit/s", mbps)),
                ]));
            }
            f.render_widget(Paragraph::new(lines), chunks[3]);

            f.render_widget(
                Paragraph::new("--- Throughput (last 60 samples) -------------").style(Style::default().fg(Color::DarkGray)),
                chunks[4],
            );

            let max_speed = speed_history.iter().max().copied().unwrap_or(1).max(10);
            let spark = Sparkline::default()
                .data(&speed_history)
                .max(max_speed)
                .style(Style::default().fg(Color::Green));
            f.render_widget(spark, chunks[5]);

            f.render_widget(
                Paragraph::new("Press Q to quit").alignment(Alignment::Center).style(Style::default().fg(Color::DarkGray)),
                chunks[6],
            );
        })?;

        if last_tick.elapsed() >= tick_rate {
            let elapsed = last_tick.elapsed().as_secs_f64();
            let cur = total_bytes.load(Ordering::Relaxed);
            let delta = cur.saturating_sub(last_bytes);
            current_speed = delta as f64 * 8.0 / elapsed / 1e6;
            last_bytes = cur;

            for i in 0..channels as usize {
                let c = per_channel_bytes[i].load(Ordering::Relaxed);
                let d = c.saturating_sub(last_per_channel[i]);
                per_channel_speed[i] = d as f64 * 8.0 / elapsed / 1e6;
                last_per_channel[i] = c;
            }

            speed_history.remove(0);
            speed_history.push(current_speed as u64);
            last_tick = Instant::now();
        }

        if event::poll(Duration::from_millis(50))? {
            if let Event::Key(key) = event::read()? {
                if key.code == KeyCode::Char('q') || key.code == KeyCode::Esc {
                    break Ok(());
                }
            }
        }
    };

    stop.store(true, Ordering::Relaxed);
    for w in workers { let _ = w.await; }

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen, DisableMouseCapture)?;
    terminal.show_cursor()?;

    res
}

// ============================================================================
// Monitor mode
// ============================================================================

use ratatui::widgets::BorderType;
use sysinfo::Networks;

async fn monitor_mode(interval_ms: u64) -> Result<()> {
    use crossterm::event::{self, Event, KeyCode};
    use ratatui::{
        backend::CrosstermBackend,
        layout::{Alignment, Constraint, Direction, Layout},
        style::{Color, Modifier, Style},
        widgets::{Block, Borders, Cell, Paragraph, Row, Table},
        Terminal,
    };
    use std::collections::HashMap;

    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut networks = Networks::new_with_refreshed_list();
    let mut prev_recv: HashMap<String, u64> = HashMap::new();
    let mut prev_sent: HashMap<String, u64> = HashMap::new();
    let mut speeds: HashMap<String, (f64, f64)> = HashMap::new();
    let mut last_tick = Instant::now();
    let tick_rate = Duration::from_millis(interval_ms);

    let res: Result<()> = loop {
        terminal.draw(|f| {
            let size = f.area();
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(3),
                    Constraint::Min(5),
                    Constraint::Length(2),
                ])
                .split(size);

            let total_rx: f64 = speeds.values().map(|(rx, _)| rx).sum();
            let total_tx: f64 = speeds.values().map(|(_, tx)| tx).sum();
            let title = format!(" Plexus Network Monitor | Rx: {:.2} Mbit/s | Tx: {:.2} Mbit/s ", total_rx, total_tx);
            let title_para = Paragraph::new(title)
                .style(Style::default().fg(Color::Green).add_modifier(Modifier::BOLD))
                .alignment(Alignment::Center);
            f.render_widget(title_para, chunks[0]);

            // Use English headers to avoid Windows console encoding issues
            let header_cells = ["Interface", "Status", "Rx (Mbit/s)", "Tx (Mbit/s)"]
                .iter().map(|h| Cell::from(*h).style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)));
            let header = Row::new(header_cells).height(1).bottom_margin(1);

            let mut rows: Vec<Row> = Vec::new();
            let mut sorted: Vec<_> = speeds.iter().collect();
            sorted.sort_by(|a, b| b.1.0.partial_cmp(&a.1.0).unwrap_or(std::cmp::Ordering::Equal));

            for (name, (rx, tx)) in sorted {
                if name.contains("Loopback") || (name.contains("lo") && speeds.len() > 1) {
                    continue;
                }
                let status = if *rx > 0.01 || *tx > 0.01 { "UP" } else { "idle" };
                let rx_str = format!("{:.2}", rx);
                let tx_str = format!("{:.2}", tx);
                let row = Row::new(vec![
                    Cell::from(name.clone()),
                    Cell::from(status),
                    Cell::from(rx_str).style(Style::default().fg(Color::Green)),
                    Cell::from(tx_str).style(Style::default().fg(Color::Cyan)),
                ]);
                rows.push(row);
            }

            let table = Table::new(rows, [Constraint::Percentage(40), Constraint::Percentage(15), Constraint::Percentage(25), Constraint::Percentage(20)])
                .header(header)
                .block(Block::default().borders(Borders::ALL).border_type(BorderType::Plain).title("Network Adapters"));
            f.render_widget(table, chunks[1]);

            let footer = Paragraph::new("Press Q to quit")
                .alignment(Alignment::Center)
                .style(Style::default().fg(Color::DarkGray));
            f.render_widget(footer, chunks[2]);
        })?;

        if last_tick.elapsed() >= tick_rate {
            networks.refresh();
            let elapsed = last_tick.elapsed().as_secs_f64();
            speeds.clear();
            for (name, data) in &networks {
                let rx = data.total_received();
                let tx = data.total_transmitted();
                let prev_rx = prev_recv.get(name).copied().unwrap_or(rx);
                let prev_tx = prev_sent.get(name).copied().unwrap_or(tx);
                let d_rx = rx.saturating_sub(prev_rx);
                let d_tx = tx.saturating_sub(prev_tx);
                let rx_mbps = d_rx as f64 * 8.0 / elapsed / 1e6;
                let tx_mbps = d_tx as f64 * 8.0 / elapsed / 1e6;
                prev_recv.insert(name.clone(), rx);
                prev_sent.insert(name.clone(), tx);
                speeds.insert(name.clone(), (rx_mbps, tx_mbps));
            }
            last_tick = Instant::now();
        }

        if event::poll(Duration::from_millis(50))? {
            if let Event::Key(key) = event::read()? {
                if key.code == KeyCode::Char('q') || key.code == KeyCode::Esc {
                    break Ok(());
                }
            }
        }
    };

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    res
}