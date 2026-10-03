use std::collections::{HashMap, HashSet};

use tokio::sync::mpsc::Sender;

use crate::{
    msg_schema::PubsubMsg,
    pubsub::{PeerID, TopicID},
};

#[derive(Debug)]
pub struct PubsubStore {
    mesh: HashMap<TopicID, HashSet<PeerID>>,
    known_peers: HashMap<TopicID, HashSet<PeerID>>,
    peers: HashMap<PeerID, Sender<PubsubMsg>>,
    // TODO:
    // fanout
    // mcache
}

impl PubsubStore {
    pub fn new() -> PubsubStore {
        PubsubStore {
            mesh: HashMap::new(),
            known_peers: HashMap::new(),
            peers: HashMap::new(),
        }
    }
}
