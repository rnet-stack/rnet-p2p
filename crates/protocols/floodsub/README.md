# FloodSub Gossip Protocol

This module implements a basic FloodSub-style publish–subscribe gossip protocol on top of multiplexed streams. 

It enables peers to:

- subscribe and unsubscribe from topics
- publish messages to topics
- forward messages to other interested peers
- deliver messages to local application subscribers

The design prioritizes simplicity and correctness over efficiency or mesh optimization. Message dissemination follows a naïve flooding strategy with loop prevention. See the demonstration and example at [floodsub-example](../../examples/floodsub/README.md).

## Overview

The FloodSub layer sits above the stream multiplexer and below application-level subscription APIs.

It is responsible for:

- tracking connected peers and their topic subscriptions
- handling incoming FloodSub RPC messages
- maintaining a last-seen cache to prevent message loops
- forwarding publish messages to eligible peers
- dispatching received messages to local topic subscribers

Each peer maintains a local view of topic membership based solely on received subscription updates. There is no global mesh, no scoring, and no control plane beyond subscription propagation.

## Core components

- **`Floodsub`**: The main protocol driver.

  - It owns:

    - local peer identity (PeerInfo)
    - an async control channel (floodsub_mpsc_tx) for internal API events
    - a shared store tracking peers, topics, and subscriptions
    - a last-seen message cache for deduplication

  - `FloodSub` is designed to be shared (`Arc`) and accessed concurrently.

- **`FloosubStore`**: An in-memory state store protected by a mutex.

  - It maintains:

    - peer_topics: mapping of topic → peers subscribed to that topic
    - peers: mapping of peer ID → outbound message channel
    - subscribed_topic_api: mapping of locally subscribed topics → application channels

  - This store represents the node’s entire view of the FloodSub network. There is no persistence or reconciliation logic.

- **`LastSeenCache`**: A deduplication cache used to prevent message rebroadcast loops.

    - Messages are identified by:
        - origin peer ID
        - sequence number (interpreted as a UNIX timestamp)

    - Each entry records the time the message was first seen locally. A background task periodically cleans up expired entries.

### Design limitations (explicit)

This implementation deliberately omits:
- mesh optimization (as in GossipSub)
- peer scoring or reputation
- message signing or validaion
- retransmission or acknowledgements
- ordered or reliable delivery guarantees
- protection against malicious peers

The module assumes cooperative peers and low to moderate network sizes.

### Intended use

This FloodSub implementation is suitable for:
- experimentation
- learning protocol mechanics
- small trusted networks
- prototyping higher-level pubsub APIs

It is not suitable for adversarial or large-scale production environments without significant extension.


global_event_tx: UNSUBSCRIBE + SUBSCRIBE + RECV_MSG