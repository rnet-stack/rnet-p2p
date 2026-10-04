use std::collections::{HashMap, HashSet};

use tokio::sync::mpsc::Sender;

use crate::{
    msg_schema::PubsubMsg,
    pubsub::{PeerID, TopicID},
};

#[derive(Debug)]
pub struct PubsubStore {
    subscribed: HashSet<TopicID>,
    mesh: HashMap<TopicID, HashSet<PeerID>>, // these are the peers we maintain, for active full-msg exchanges
    topic_peers: HashMap<TopicID, HashSet<PeerID>>, // topicIDs are mapped to all the peers, we know are subscribed to it
    peer_channels: HashMap<PeerID, Sender<PubsubMsg>>,
    // TODO:
    // fanout: HashMap<TopicID, HashSet<PeerID>> Peers in a topic, we publish to, while not presently being subscribed to the topic
    // mcache: MessageCache
}

impl PubsubStore {
    pub fn new() -> PubsubStore {
        PubsubStore {
            subscribed: HashSet::new(),
            mesh: HashMap::new(),
            topic_peers: HashMap::new(),
            peer_channels: HashMap::new(),
        }
    }
}
