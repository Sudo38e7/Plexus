# 🚀 Plexus

<p align="center">
  <b>Multipath network aggregator, custom UDP protocol, and real-time system monitor written in Rust.</b>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/language-Rust-orange.svg" alt="Rust">
  <img src="https://img.shields.io/badge/platform-Windows%20%7C%20Linux%20%7C%20macOS-blue.svg" alt="Platform">
  <img src="https://img.shields.io/badge/license-MIT-green.svg" alt="License">
</p>

## 🤔 Overview

Plexus is an experimental networking project built from scratch in Rust. It demonstrates how to aggregate multiple network channels, handle packet loss with Forward Error Correction (FEC), and monitor system network adapters in real-time.

It is divided into two main components:
1. **The Engine:** A custom UDP transport protocol with Reed-Solomon FEC and a built-in network emulator.
2. **The Monitor:** A live system network monitor (similar to `iftop` or `btop`) written entirely in Rust.

## ✨ Features

- **Custom UDP Protocol:** 16-byte header, optimized for high-throughput data transfer.
- **Forward Error Correction (FEC):** Uses Reed-Solomon erasure coding (K=10, M=3). Reconstructs lost packets without waiting for TCP retransmissions.
- **Multi-Channel Aggregation:** Distributes traffic across multiple sockets/channels using a weighted scheduler.
- **Live System Monitor:** A beautiful TUI (Terminal User Interface) showing real-time download/upload speeds for every network adapter on your machine.
- **Self-Contained Lab:** Includes an emulator that can simulate packet loss, delay, and bandwidth limits for reproducible benchmarks.

## 📊 Benchmarks

Tested on a local loopback with simulated loss (5%) and delay (20ms):

| Channels | Per-channel Rate | Useful Throughput | Delivery (No FEC) | Delivery (With FEC) |
| :--- | :--- | :--- | :--- | :--- |
| 6 | 20 Mbps | 111 Mbit/s | 94.9% | **99.9%** |
| 12 | 50 Mbps | **599 Mbit/s** | 94.8% | **99.9%** |

*Note: Speeds above 1 Gbit/s are achieved via memory loopback; actual throughput depends on physical network limits.*

## 🛠 Build & Install

Ensure you have [Rust installed](https://rustup.rs/), then run:

```bash
# Clone the repository
git clone https://github.com/Sudo38e7/Plexus.git
cd Plexus

# Build the optimized release binary
cargo build --release
```

## 🚀 Usage

The compiled binary is located at `./target/release/plexus`.

**1. Live Network Monitor**
Watch your real network interfaces (Wi-Fi, Ethernet, VPNs) in real-time. Press `Q` to quit.
```bash
./plexus monitor
```

**2. Live TUI Dashboard**
Run the internal simulation with 6 channels to see the aggregation engine in action.
```bash
./plexus tui --channels 6 --rate-mbps 20
```

**3. Benchmark Lab**
Run a self-contained test with 12 channels, 50 Mbps each, and 5% packet loss, using FEC.
```bash
./plexus lab --channels 12 --rate-mbps 50 --loss 0.05 --fec
```

## �
 Architecture

The workspace is split into several crates:

- `plexus-core`: Packet structure, handshake, and session state.
-  plexus-fec`: Reed-Solomon erasure coding wrapper.
- `plexus-scheduler`: Weighted channel distribution logic.
- `plexus-cli``: The main client binary containing the TUI and Monitor.
- `plexus-server`: UDP server for receiving and reconstructing packets.
- `plexus-lossy-proxy`: Network emulator (loss, delay, rate limiting).

## 📄 License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.