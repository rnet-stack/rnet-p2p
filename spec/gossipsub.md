This specification outlines the architecture, data structures, and execution flows required to implement the Gossipsub within the existing rnet-p2p stack. It builds on the channel driven actor model established in the Floodsub implementation while introducing, 
- mesh routing
- control messages
- periodic maintenance

## Architecture

Here we will forward and broadcast full-messages to subset of peers (in mesh) and use control messages  (IHAVE/IWANT) for the rest of the peers in the same topic.

Will use the same `IProtocolHandler` trail and muxed-stream per-peer model, with an extension of handling periodic heartbeat ticks.

Instead of `Arc<Mutex<Store>>`, the Gossipsub store, will remain entirely inside the central actor loop (`handle_internal_api`) to prevent lock contention during heavy network traffic.

### Gossipsub Configuration Parameters

The protocol relies on predefined thresholds to manage the mesh degree per topic:

- D: Target mesh degree (default: 6)
- D_low: Lower bound for mesh degree (default: 4)
- D_high: Upper bound for mesh degree (default: 12)
- D_out: Outbound connection quota (default: 2)
- heartbeat_interval: Tick duration (default: 1 second)
- history_length: Size of the message cache window (default: 5)
- history_gossip: Number of history windows to include in IHAVE messages (default: 3)

### Gossipsub-Store

The actor state must track peers categorized by their relationship to topics:

- mesh: `HashMap<TopicId, HashSet<PeerId>>`: Peers receiving full messages for subscribed topics.

- fanout: `HashMap<TopicId, HashSet<PeerId>>`: Peers receiving full messages for topics we publish to, but are not subscribed to.

- known_peers: `HashMap<TopicId, HashSet<PeerId>>`: All peers in the swarm that subscribe to a topic.

- connected_streams: `HashMap<PeerId, Sender<Rpc>>`: Active channels to peer stream writers.

- mcache: `MessageCache`: The sliding window cache serving `IWANT` requests and deduplication.

### Internal API commands

```rust
pub enum GossipCommand {
    Subscribe(String),
    Unsubscribe(String),
    Publish(Message),
    IncomingRpc(PeerId, Rpc),
    PeerConnected(PeerId, Sender<Rpc>),
    PeerDisconnected(PeerId),
    HeartbeatTick,
}
```

### Message Cache

Gossipsub requires a sliding-window message cache, not just a time-to-live deduplicator. This serves two purposes: deduplication and answering `IWANT` requests.

- Structure: Implement as a circular buffer or a VecDeque of HashMaps (`VecDeque<HashMap<MessageId, Message>>`).

- Capacity: Length is strictly history_length (e.g., 5).

- Insertion: New messages are inserted into the map at index 0.

- Shifting: On every heartbeat tick, a new empty map is pushed to index 0, and the oldest map (index 5) is dropped.

- Gossip Generation: When emitting `IHAVE` messages, only message IDs from the first `history_gossip` (e.g., 3) maps are included.

### Heartbeat Engine

The heartbeat is the core driver of Gossipsub. A dedicated Tokio task will emit a `GossipCommand::HeartbeatTick` to the central actor every second.

Upon receiving the tick, the central actor performs the following sequential operations:

- Shift Cache: Shift the `MessageCache` window forward by one.

- Mesh Maintenance (Per Subscribed Topic):

    - If mesh.len() < D_low: Select random peers from known_peers (excluding current mesh), add them to mesh, and queue GRAFT messages to them.

    - If mesh.len() > D_high: Select random peers in the mesh to drop until the size equals D. Remove them from mesh and queue PRUNE messages.

- Fanout Maintenance (Per Fanout Topic):

    - If a fanout topic has not been published to in the last TTL (usually 60 seconds), drop it from the fanout map entirely.

    - If fanout.len() < D: Select peers from known_peers to populate it.

- Emit Gossip:

    - For each topic, select a random subset of peers from known_peers that are not in the mesh.

    - Construct an IHAVE control message containing the Message IDs from the first history_gossip windows of the message cache.

    - Queue these IHAVE messages for delivery.

### Control Message Processing

The Rpc Protobuf definition must be expanded to handle the ControlMessage payload (IHAVE, IWANT, GRAFT, PRUNE). Piggybacking is mandatory: control messages are bundled into the same RPC packets as standard data publishers.

Ingress Flow (Receiving Rpc):

When `IncomingRpc` hits the central actor:

- Process Subscriptions: Update `known_peers` based on peer sub/unsub flags.

- Process Data Messages:

    - Check `mcache` for duplicates using a cryptographic hash or strict node-counter MessageId. If seen, drop.

    - Insert into mcache[0].

    - Route payload to local subscription MPSC channels.

    - Forward the message to all peers in mesh (excluding the sender).

- Process Control Messages:

    - GRAFT: Add the sender to the mesh for the requested topic. If we aren't subscribed to that topic, respond with PRUNE.

    - PRUNE: Remove the sender from the mesh.

    - IHAVE: Check the provided Message IDs against the mcache. If an ID is missing, queue an IWANT message back to the sender.

    - IWANT: Look up the requested Message IDs in the full mcache. For every message found, queue it as a full data message back to the sender.

### Publishing flow

When the local node publishes a message via GossipCommand::Publish:

- Assign a monotonically increasing sequence number (e.g., AtomicU64) to prevent timestamp collisions.

- Calculate the MessageId.

- Insert into mcache[0].

- Check if we are subscribed to the topic.

    - If Subscribed: Forward the full message to all peers in mesh[topic].

    - If Not Subscribed: Forward the full message to all peers in fanout[topic]. If fanout[topic] is empty, populate it with up to D peers from known_peers, then forward.