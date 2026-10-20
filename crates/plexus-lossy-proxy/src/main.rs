//! UDP-прокси с потерями, задержкой и rate-limit (token bucket).

use anyhow::Result;
use clap::Parser;
use rand::Rng;
use socket2::{Domain, Protocol, Socket, Type};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;

#[derive(Parser, Debug)]
#[command(name = "plexus-lossy-proxy", about = "Эмулятор плохого канала")]
struct Args {
    #[arg(long, default_value = "0.0.0.0:51821")] listen: String,
    #[arg(long, default_value = "127.0.0.1:51820")] target: String,
    #[arg(long, default_value_t = 0.05)] loss: f64,
    #[arg(long, default_value_t = 0)] delay_ms: u64,
    #[arg(long, default_value_t = 0)] rate_mbps: u64,
}

fn create_udp_socket(addr: SocketAddr) -> Result<UdpSocket> {
    let domain = if addr.is_ipv4() { Domain::IPV4 } else { Domain::IPV6 };
    let sock = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
    sock.set_recv_buffer_size(4 * 1024 * 1024)?;
    sock.set_send_buffer_size(4 * 1024 * 1024)?;
    sock.set_nonblocking(true)?;
    sock.bind(&addr.into())?;
    let std_sock: std::net::UdpSocket = sock.into();
    Ok(UdpSocket::from_std(std_sock)?)
}

struct RateLimiter {
    rate_bps: u64,
    tokens: AtomicU64,
    last_refill: AtomicU64,
    start: Instant,
}

impl RateLimiter {
    fn new(rate_mbps: u64) -> Self {
        let rate_bps = rate_mbps * 1_000_000;
        Self {
            rate_bps,
            tokens: AtomicU64::new(rate_bps / 10),
            last_refill: AtomicU64::new(0),
            start: Instant::now(),
        }
    }
    fn now_us(&self) -> u64 { self.start.elapsed().as_micros() as u64 }

    async fn acquire(&self, bytes: u64) {
        if self.rate_bps == 0 { return; }
        let need = bytes * 8;
        loop {
            let now = self.now_us();
            let last = self.last_refill.load(Ordering::Relaxed);
            let elapsed = now.saturating_sub(last);
            let refill = (self.rate_bps as u128 * elapsed as u128 / 1_000_000) as u64;
            if refill > 0 {
                if self.last_refill.compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
                    let cur = self.tokens.load(Ordering::Relaxed);
                    let cap = self.rate_bps / 10;
                    self.tokens.store((cur + refill).min(cap), Ordering::Relaxed);
                }
            }
            let cur = self.tokens.load(Ordering::Relaxed);
            if cur >= need {
                if self.tokens.compare_exchange(cur, cur - need, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
                    return;
                }
            } else {
                let missing = need - cur;
                let wait_us = (missing as u128 * 1_000_000 / self.rate_bps as u128) as u64;
                tokio::time::sleep(Duration::from_micros(wait_us.max(50))).await;
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let args = Args::parse();
    let listen: SocketAddr = args.listen.parse()?;
    let target: SocketAddr = args.target.parse()?;
    let front = Arc::new(create_udp_socket(listen)?);
    let back = Arc::new(create_udp_socket("0.0.0.0:0".parse()?)?);
    back.connect(target).await?;
    tracing::info!(
        "lossy proxy {} -> {} | loss={:.2}% delay={}ms rate={}Mbps",
        listen, target, args.loss * 100.0, args.delay_ms, args.rate_mbps
    );

    let limiter = Arc::new(RateLimiter::new(args.rate_mbps));
    let loss = args.loss;
    let delay = args.delay_ms;
    let mut client_addr: Option<SocketAddr> = None;
    let mut buf_up = vec![0u8; 65535];
    let mut buf_down = vec![0u8; 65535];

    loop {
        tokio::select! {
            r = front.recv_from(&mut buf_up) => {
                let (n, from) = r?;
                client_addr = Some(from);
                if rand::thread_rng().gen::<f64>() < loss { continue; }
                let data = buf_up[..n].to_vec();
                let back = back.clone();
                let limiter = limiter.clone();
                tokio::spawn(async move {
                    if delay > 0 { tokio::time::sleep(Duration::from_millis(delay)).await; }
                    limiter.acquire(data.len() as u64).await;
                    let _ = back.send(&data).await;
                });
            }
            r = back.recv(&mut buf_down) => {
                let n = r?;
                if let Some(addr) = client_addr {
                    if rand::thread_rng().gen::<f64>() < loss { continue; }
                    let data = buf_down[..n].to_vec();
                    let front = front.clone();
                    let limiter = limiter.clone();
                    tokio::spawn(async move {
                        if delay > 0 { tokio::time::sleep(Duration::from_millis(delay)).await; }
                        limiter.acquire(data.len() as u64).await;
                        let _ = front.send_to(&data, addr).await;
                    });
                }
            }
        }
    }
}