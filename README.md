# rnet — a modular P2P networking stack in Rust

`rnet` is an experimental peer-to-peer networking library, built from scratch to understand how a full p2p stack fits together: raw transports, secure channel upgrades, stream multiplexing, protocol negotiation, and application-level protocols on top.

The layering follows libp2p's model but is implemented independently, with the async plumbing written out explicitly rather than hidden behind a framework.

## Demo: ping and floodsub multiplexed over one connection

Two logical streams (`ping` and `floodsub`) running concurrently over a single TCP connection between two nodes.

https://github.com/user-attachments/assets/506826b9-cfb6-4947-90b0-d5c21dabd555

## What's implemented

| Layer | Crate | What it does |
|---|---|---|
| Identity | `identity` | RSA keypairs, `PeerId` (base58 of SHA-256 of the public key), `Multiaddr` parsing, the core traits every layer implements, and the `GlobalEvent` types |
| Transport | `transport` | `tcp` and `udp`. UDP is connection-oriented on top of a single socket: a 1-byte flag (`Connect` / `Disconnect` / `General`) routes datagrams to per-peer connections |
| Security | `security` | X25519 ephemeral Diffie-Hellman key exchange, then ChaCha20-Poly1305 on every frame |
| Negotiation | `multistream` | Multiselect handshake plus an `identify` exchange so both sides learn the remote `PeerInfo` (peer id + listen address) |
| Muxer | `muxer` | `mplex`: varint header carrying `stream_id` and a flag (`NewStream`, `Message`, `CloseStream`, …). Also runs the optional UDP liveliness ping on stream 0 |
| Swarm | `swarm` | Accept/dial loop, runs the upgrade pipeline on every connection, keeps the peerstore and the map of active connections, routes new streams to protocol handlers |
| Node | `node` | Public facade. Starts the swarm, registers protocols, exposes `connect` / `new_stream` / floodsub / ping APIs over an internal mpsc channel |
| Protocols | `protocols/ping`, `protocols/floodsub` | `ping`: RTT measurement, optionally with RLNC-encoded payloads. `floodsub`: topic pub/sub with per-peer subscription tracking, flooding with loop prevention, and a last-seen dedup cache |
| Schema | `schema` | `.proto` definitions for floodsub/gossipsub, compiled by `build.rs` via `prost` |
| Facade | `p2p` | Re-exports every crate above as one dependency |

### Connection upgrade pipeline

Every connection, inbound or outbound, goes through the same sequence in [swarm/src/inner.rs](crates/swarm/src/inner.rs):

```
raw stream (tcp | udp)
  → security   : X25519 DH → ChaCha20-Poly1305 channel
  → identify   : multiselect + PeerInfo exchange
  → muxer      : mplex, one connection → many streams
  → peerstore  : remember the peer, keep a write handle to its connection
```

After that, `new_stream(peer, [protocol])` opens an mplex stream, negotiates the protocol name (e.g. `rnet/ping/0.0.1`), and hands the stream to the registered handler.

### Events

Protocols don't print or call back into application code. Every protocol event (`PingEvent` with RTTs, `FloodsubEvent` for publish / subscribe / unsubscribe) is `bincode`-serialised and sent on a single `global_event_rx` channel that `NodeInner::new` returns. The application drains that channel and decides what to do.

### RLNC over ping

With `--rlnc`, the ping payload is split into fixed-size chunks and sent as random linear combinations over GF(256). The receiver reconstructs once it has enough linearly independent packets, so lost datagrams (relevant over UDP) don't require retransmission. See [examples/rlnc](examples/rlnc/README.md) for a standalone walkthrough of the encode/decode math.

## Getting started

Requirements: Rust 1.90+ (a `rust-toolchain.toml` pins stable) and `protoc` for the schema crate.

```sh
# terminal 1
cargo run --bin floodsub --release

# terminal 2
cargo run --bin floodsub --release
# then in the CLI:  connect <multiaddr of node 1>
```

The `floodsub` binary is the main interactive example. It runs a node with both `ping` and `floodsub` enabled and exposes a CLI:

```
connect <maddr>          connect with a new peer
ping <maddr> <count>     exchange pings with a peer
fsub <maddr>             open a floodsub stream with a peer
join <topic>             subscribe to a topic
leave <topic>            unsubscribe from a topic
publish <topic> <msg>    publish a message
topics / fpeers / gpeers / mesh / local
```

Flags:

| Flag | Effect |
|---|---|
| `--udp` | Listen and dial over UDP instead of TCP |
| `--ping-check` | Send a liveliness frame every 5 s on each connection (UDP only) |
| `--rlnc` | Encode ping payloads with RLNC |

Other examples under [examples/](examples/): `tcp` (raw transport, no upgrades), `udp` (raw UDP transport), `rlnc` (encoding/decoding only, no networking). Each has its own README.

## Using the library

```rust
use node::{inner::NodeInner, protocol::InnerProtocolOpt};
use identity::multiaddr::Multiaddr;

let mut listen_addr = Multiaddr::new("ip4/127.0.0.1/tcp/0")?;

let (node, mut events) = NodeInner::new(
    &mut listen_addr,
    vec![InnerProtocolOpt::Floodsub, InnerProtocolOpt::Ping { enable_rlnc: false }],
    None,   // hex-encoded RSA key, or None to generate one
    false,  // UDP liveliness check
).await?;

// listen_addr now has the real port and /p2p/<peer-id> appended
node.connect(&remote).await?;
node.new_stream(&remote.to_string(), vec!["rnet/ping/0.0.1".into()]).await?;

while let Some(bytes) = events.recv().await {
    let event: identity::events::GlobalEvent = bincode::deserialize(&bytes)?;
    // handle it
}
```

## Development

```sh
make lint   # cargo fmt + clippy with -D warnings
make test   # cargo test --all --all-features
```

CI runs the same three steps (fmt check, clippy, test) on every push and PR.

## Status and limitations

This is a learning project, not a production library.

- Floodsub trusts every peer: no message signing, scoring, or mesh optimisation (see [floodsub README](crates/protocols/floodsub/README.md)).
- The security handshake authenticates nothing — DH is unauthenticated, so it protects against passive observers only.
- Error handling is mostly `unwrap()`; a misbehaving peer can bring down the task handling it.
- Only `mplex` and one security option exist; the negotiation code selects them without real fallback.

Per-crate READMEs may lag behind the code; this file and the source are the reference.
