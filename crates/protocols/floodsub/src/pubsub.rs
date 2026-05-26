use std::{
    collections::{hash_map::Entry, HashMap, HashSet, VecDeque},
    sync::Arc,
};

use anyhow::Result;
use async_trait::async_trait;
use identity::{
    events::{FloodsubEvent, FloodsubMsgType, GlobalEvent},
    traits::muxer::IMuxedStream,
};
use identity::{peer::PeerInfo, traits::core::IProtocolHandler};
use prost::Message as ProstMessage;
use schema::floodsub::{rpc::SubOpts, Message, Rpc};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    Mutex,
};
use tracing::{error, warn};

use crate::{
    cache::{cleanup_msg_cache, get_seqno, seqno_to_unix_tx},
    subscription::{
        build_floodsub_api_frame, process_floodsub_api_frame, SubAPIMpscFlag, SubscriptionAPI,
    },
};

pub type LastSeenCache = HashMap<MessageKey, u64>;
pub type FloodsubPayload = (Option<String>, Option<Vec<String>>, Option<Vec<u8>>);
pub type SubscrbedTopicApi = HashMap<String, Sender<(Vec<u8>, Vec<u8>)>>;

#[derive(Debug, Clone)]
pub struct FloosubStore {
    peer_topics: HashMap<String, Vec<String>>,
    peers: HashMap<String, Sender<Vec<u8>>>,
    subscribed_topic_api: SubscrbedTopicApi,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct MessageKey {
    from: Vec<u8>,
    seqno: u64,
}

#[derive(Debug, Clone)]
pub struct FloodSub {
    pub local_peer_info: PeerInfo,
    pub floodsub_mpsc_tx: Sender<Vec<u8>>,
    last_seen_cache: Arc<Mutex<LastSeenCache>>,
    pub floodsub_store: Arc<Mutex<FloosubStore>>,
    pub global_event_tx: Sender<Vec<u8>>,
}

#[async_trait]
impl IProtocolHandler for FloodSub {
    async fn stream_handler(
        &self,
        mut stream: Box<dyn IMuxedStream + Send + Sync + 'static>,
    ) -> Result<()> {
        let peer_id = stream.get_peer_id();
        let (floodsub_peer_mpsc_tx, mut floodsub_peer_mpsc_rx) = mpsc::channel::<Vec<u8>>(100);
        let mut notification = VecDeque::<Vec<u8>>::new();

        // We received a new peer here, so send a hello packet too,
        // to notify them of our subscribed topics
        self.handle_new_peer(floodsub_peer_mpsc_tx, peer_id.clone())
            .await
            .unwrap();

        loop {
            tokio::select! {

                incoming = stream.read() => {
                    match incoming {
                        Ok(incoming) => {
                            let rpc = Rpc::decode(&incoming[..]).expect("Decoding failed");
                            self.handle_incoming(rpc, peer_id.clone()).await.unwrap();
                        }
                        Err(_) => {
                            break;
                        }
                    }
                }

                Some(event) = floodsub_peer_mpsc_rx.recv() => {
                    notification.push_back(event);
                }

                _ = async {}, if !notification.is_empty() => {
                    let data = notification.pop_front().unwrap();
                    if let Err(e) = stream.write(&data).await {
                        error!("Error while writing in stream of peer: {}, {}", peer_id, e);
                        break;
                    }
                }
            }
        }

        let frame =
            build_floodsub_api_frame(SubAPIMpscFlag::DeadPeers, None, Some(vec![peer_id]), None);
        self.floodsub_mpsc_tx.send(frame).await.unwrap();

        Ok(())
    }
}

impl FloodSub {
    pub async fn new(local_peer: &PeerInfo, global_event_tx: Sender<Vec<u8>>) -> Result<Arc<Self>> {
        let (floodsub_mpsc_tx, floodsub_mpsc_rx) = mpsc::channel::<Vec<u8>>(300);
        let last_seen_cache = Arc::new(Mutex::new(HashMap::new()));
        let local_peer_info = local_peer.clone();

        let floodsub = Arc::new(FloodSub {
            local_peer_info,
            floodsub_mpsc_tx: floodsub_mpsc_tx.clone(),
            last_seen_cache: last_seen_cache.clone(),
            floodsub_store: Arc::new(Mutex::new(FloosubStore {
                peer_topics: HashMap::new(),
                peers: HashMap::new(),
                subscribed_topic_api: HashMap::new(),
            })),
            global_event_tx,
        });

        tokio::spawn(async move {
            cleanup_msg_cache(last_seen_cache).await;
        });

        let spawn_floodsub = floodsub.clone();
        tokio::spawn(async move {
            spawn_floodsub.handle_internal_api(floodsub_mpsc_rx).await;
        });

        Ok(floodsub)
    }

    pub async fn handle_internal_api(&self, mut floodsub_mpsc_rx: Receiver<Vec<u8>>) {
        let mut notification = VecDeque::<Vec<u8>>::new();

        loop {
            tokio::select! {
                Some(event) = floodsub_mpsc_rx.recv() => {
                    notification.push_back(event);
                }

                _ = async {}, if !notification.is_empty() => {
                    let frame = notification.pop_front().unwrap();

                    let (flag, (_, opt_vec_pld, opt_data)): (SubAPIMpscFlag, FloodsubPayload) = process_floodsub_api_frame(frame).unwrap();
                    match flag {
                        SubAPIMpscFlag::DeadPeers => {
                            self.handle_dead_peers(opt_vec_pld.unwrap())
                                .await.unwrap();
                        }
                        SubAPIMpscFlag::Unsubscribe => {
                            self.unsubscribe(opt_vec_pld.unwrap()).await.unwrap();
                        }
                        SubAPIMpscFlag::Publish => {
                            let timestamp = get_seqno();

                            let pub_msg = Message {
                                from: Some(self.local_peer_info.clone().peer_id.as_bytes().to_vec()),
                                data: opt_data,
                                seqno: Some(timestamp),
                                topic_ids: opt_vec_pld.unwrap(),
                            };

                            self.publish(self.local_peer_info.clone().peer_id, pub_msg).await.unwrap();
                        }
                    }
                }
            }
        }
    }

    pub async fn handle_incoming(&self, rpc: Rpc, peer_id: String) -> Result<()> {
        let subsribed_topics = self.get_subscribed_topics().await.unwrap_or(vec![]);

        if !rpc.publish.is_empty() {
            for msg in rpc.publish {
                // is msg not in subscribed topics: continue
                for topic in msg.topic_ids.iter() {
                    if subsribed_topics.contains(topic) {
                        self.consume_pubsub_msg(peer_id.clone(), msg.clone())
                            .await
                            .unwrap();
                        break;
                    }
                }
            }
        }

        if !rpc.subscriptions.is_empty() {
            for msg in rpc.subscriptions {
                self.handle_subopts(peer_id.clone(), msg).await.unwrap();
            }
        }
        Ok(())
    }

    pub async fn write_msg(&self, peer_id: String, rpc_msg: Rpc) -> Result<()> {
        let floodsub_peer_mpsc_tx = {
            let floodsub_store = self.floodsub_store.lock().await;
            floodsub_store.peers.get(&peer_id).unwrap().clone()
        };

        let mut buf = Vec::new();
        rpc_msg.encode(&mut buf).expect("Encoding failed");

        floodsub_peer_mpsc_tx.send(buf).await.unwrap();
        Ok(())
    }

    pub async fn subscribe(&self, topic_id: String) -> Result<()> {
        if self
            .get_subscribed_topics()
            .await
            .unwrap_or_else(Vec::new)
            .contains(&topic_id)
        {
            warn!("Already subscribed to the topic: {}", topic_id);
            return Ok(());
        }

        // Send out a global event, for subscribing to a new topic
        let floosub_event = GlobalEvent::Floodsub(FloodsubEvent {
            msg_type: FloodsubMsgType::Subscribe,
            source: None,
            msg: None,
            topic: topic_id.clone(),
        });

        self.global_event_tx
            .send(bincode::serialize(&floosub_event).unwrap())
            .await
            .unwrap();
        // ---------------------------------------------------------

        let (topic_mpsc_tx, topic_mpsc_rx) = mpsc::channel::<(Vec<u8>, Vec<u8>)>(100);
        let mut sub_api = SubscriptionAPI::new(
            topic_id.clone(),
            self.floodsub_mpsc_tx.clone(),
            topic_mpsc_rx,
            self.global_event_tx.clone(),
        );

        {
            let mut store = self.floodsub_store.lock().await;
            store
                .subscribed_topic_api
                .insert(topic_id.clone(), topic_mpsc_tx);
        }

        let mut sub_rpc = Rpc::default();
        sub_rpc.subscriptions.push(SubOpts {
            subscribe: Some(true),
            topic_id: Some(topic_id),
        });

        self.message_all_peers(sub_rpc).await.unwrap();
        tokio::spawn(async move {
            sub_api.receive_loop().await;
        });

        Ok(())
    }

    pub async fn message_all_peers(&self, rpc: Rpc) -> Result<()> {
        let mut rpc_bytes = Vec::new();
        rpc.encode(&mut rpc_bytes).expect("Encoding failed");

        let floodsub_store = self.floodsub_store.lock().await;
        for stream in floodsub_store.peers.values() {
            stream.send(rpc_bytes.clone()).await.unwrap();
        }

        Ok(())
    }

    pub async fn unsubscribe(&self, topics: Vec<String>) -> Result<()> {
        for topic_id in topics {
            if !self
                .get_subscribed_topics()
                .await
                .unwrap_or(vec![])
                .contains(&topic_id)
            {
                warn!("Not subscribed to the topic: {}", topic_id);
                return Ok(());
            }

            // Send out a global event, for unsubscribing to a new topic
            let floosub_event = GlobalEvent::Floodsub(FloodsubEvent {
                msg_type: FloodsubMsgType::Unsubscribe,
                source: None,
                msg: None,
                topic: topic_id.clone(),
            });

            self.global_event_tx
                .send(bincode::serialize(&floosub_event).unwrap())
                .await
                .unwrap();
            // ---------------------------------------------------------

            {
                let mut store = self.floodsub_store.lock().await;
                store.subscribed_topic_api.remove_entry(&topic_id);
            }

            let mut unsub_rpc = Rpc::default();
            unsub_rpc.subscriptions.push(SubOpts {
                subscribe: Some(false),
                topic_id: Some(topic_id),
            });

            self.message_all_peers(unsub_rpc).await.unwrap();
        }
        Ok(())
    }

    pub async fn publish(&self, msg_forwarder_id: String, publish_msg: Message) -> Result<()> {
        let peers = self
            .get_peers_to_send(
                publish_msg.topic_ids.clone(),
                msg_forwarder_id,
                String::from_utf8_lossy(publish_msg.from()).to_string(),
            )
            .await
            .unwrap_or_else(HashSet::new);

        let mut rpc_msg = Rpc::default();
        rpc_msg.publish.push(publish_msg.clone());

        for peer_id in peers {
            self.write_msg(peer_id, rpc_msg.clone()).await.unwrap();
        }

        Ok(())
    }

    pub async fn consume_pubsub_msg(
        &self,
        msg_forwarder_id: String,
        publish_msg: Message,
    ) -> Result<()> {
        // - if peer blacklisted :- msg_forwarder + origin_peer
        // - reject msgs originated from ourselves
        // - if message seen then return, else insert in the cache and proceed
        // - notify subscriptions
        // - publish msg

        let origin = String::from_utf8_lossy(publish_msg.from()).to_string();
        let subscribed_topics = self.get_subscribed_topics().await.unwrap();

        if origin == self.local_peer_info.peer_id {
            return Ok(());
        }

        if self.is_msg_seen(&publish_msg).await {
            return Ok(());
        }

        for topic_id in publish_msg.topic_ids.iter() {
            if subscribed_topics.contains(topic_id) {
                self.notify_subscription(topic_id, publish_msg.clone())
                    .await
                    .unwrap();
            }
        }

        self.publish(msg_forwarder_id, publish_msg).await.unwrap();

        Ok(())
    }

    pub async fn notify_subscription(&self, topic_id: &String, pubsub_msg: Message) -> Result<()> {
        let topic_mpsc_tx = {
            let store = self.floodsub_store.lock().await;
            let mpsc_tx = store.subscribed_topic_api.get(topic_id).unwrap().clone();

            mpsc_tx
        };

        topic_mpsc_tx
            .send((pubsub_msg.data().to_vec(), pubsub_msg.from().to_vec()))
            .await
            .unwrap();

        Ok(())
    }

    pub async fn is_msg_seen(&self, publish_msg: &Message) -> bool {
        let origin_id: Vec<u8> = publish_msg.from().to_vec();
        let seqno_int: u64 = seqno_to_unix_tx(publish_msg.seqno()).unwrap();

        let key = MessageKey {
            from: origin_id,
            seqno: seqno_int,
        };

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut cache = self.last_seen_cache.lock().await;

        if let Entry::Vacant(e) = cache.entry(key) {
            e.insert(now);
            false
        } else {
            true
        }
    }

    pub async fn handle_subopts(&self, origin_id: String, sub_msg: SubOpts) -> Result<()> {
        let mut floodsub_store = self.floodsub_store.lock().await;
        let topic_id = sub_msg.topic_id.clone().unwrap();

        if sub_msg.subscribe() {
            // debug!("[{}]: subscribed [{}]", origin_id, topic_id);

            floodsub_store
                .peer_topics
                .entry(topic_id)
                .and_modify(|peers| {
                    if !peers.contains(&origin_id) {
                        peers.push(origin_id.clone());
                    }
                })
                .or_insert_with(|| vec![origin_id]);
        } else {
            // debug!("[{}]: unsubscribed [{}]", origin_id, topic_id);
            if let Some(peers) = floodsub_store.peer_topics.get_mut(&topic_id) {
                peers.retain(|x| x != &origin_id);
            }
        }

        Ok(())
    }

    pub async fn handle_new_peer(
        &self,
        floodsub_peer_mpsc_tx: Sender<Vec<u8>>,
        peer_id: String,
    ) -> Result<()> {
        {
            let mut floodsub_store = self.floodsub_store.lock().await;
            floodsub_store
                .peers
                .insert(peer_id.clone(), floodsub_peer_mpsc_tx.clone());
        }

        // send in the hello-packet
        match self.get_hello_packet().await {
            None => {}
            Some(rpc) => {
                let mut buf = Vec::new();
                rpc.encode(&mut buf).expect("Encoding failed");

                floodsub_peer_mpsc_tx.send(buf).await.unwrap();
            }
        }

        Ok(())
    }

    pub async fn handle_dead_peers(&self, peer_ids: Vec<String>) -> Result<()> {
        let mut store = self.floodsub_store.lock().await;

        for peer_id in peer_ids {
            if store.peers.remove(&peer_id).is_none() {
                return Ok(());
            }

            for peers in store.peer_topics.values_mut() {
                peers.retain(|peer| peer != &peer_id);
            }
        }

        Ok(())
    }

    async fn get_peers_to_send(
        &self,
        topic_ids: Vec<String>,
        msg_forwarder: String,
        origin: String,
    ) -> Option<HashSet<String>> {
        let floodsub_store = self.floodsub_store.lock().await;
        let known = [msg_forwarder, origin];
        let mut peers_to_send = HashSet::new();

        for topic in topic_ids {
            match floodsub_store.peer_topics.get(&topic) {
                Some(peers) => {
                    for peer in peers {
                        if !known.contains(peer) {
                            peers_to_send.insert(peer.clone());
                        }
                    }
                }
                None => continue,
            }
        }

        if !peers_to_send.is_empty() {
            return Some(peers_to_send);
        }

        None
    }

    pub async fn get_subscribed_topics(&self) -> Option<Vec<String>> {
        let topics: Vec<String> = {
            let store = self.floodsub_store.lock().await;
            store
                .subscribed_topic_api
                .keys()
                .cloned()
                .collect::<Vec<_>>()
        };

        if !topics.is_empty() {
            return Some(topics);
        }

        None
    }

    pub async fn get_connected_peers(&self) -> Option<Vec<String>> {
        let peers: Vec<String> = {
            let store = self.floodsub_store.lock().await;
            store.peers.keys().cloned().collect()
        };

        if !peers.is_empty() {
            return Some(peers);
        }

        None
    }

    pub async fn get_floodsub_mesh(&self) -> HashMap<String, Vec<String>> {
        let peer_topics = {
            let store = self.floodsub_store.lock().await;
            store.peer_topics.clone()
        };

        peer_topics
    }

    pub fn get_local(&self) -> Result<PeerInfo> {
        Ok(self.local_peer_info.clone())
    }

    pub async fn get_hello_packet(&self) -> Option<Rpc> {
        let mut rpc = Rpc::default();
        match self.get_subscribed_topics().await {
            Some(topics) => {
                for topic_id in topics {
                    rpc.subscriptions.push(SubOpts {
                        subscribe: Some(true),
                        topic_id: Some(topic_id),
                    });
                }
                Some(rpc)
            }
            None => None,
        }
    }
}
