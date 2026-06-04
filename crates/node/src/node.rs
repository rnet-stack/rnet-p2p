use async_trait::async_trait;
use floodsub::{
    pubsub::FloodSub,
    subscription::{build_floodsub_api_frame, SubAPIMpscFlag},
};
use identity::{
    keys::rsa::RsaKeyPair,
    multiaddr::Multiaddr,
    peer::{PeerData, PeerInfo},
    traits::protocols::{INodeFloodsubAPI, INodePingAPI},
};

use ping::handler::Ping;
use swarm::inner::ProtocolHanldler;
use tokio::sync::{mpsc::Sender, Mutex};

use anyhow::{Error, Result};
use identity::traits::core::INode;
use std::{collections::HashMap, result::Result::Ok, sync::Arc, time::Duration};
use tracing::{info, warn};

use crate::{
    headers::{build_host_frame, HostMpscTxFlag},
    protocol::InnerProtocolOpt,
};

const FLOODSUB: &str = "rnet/floodsub/0.0.1";
const PING: &str = "rnet/ping/0.0.1";

pub struct Node {
    pub local_peer_info: PeerInfo,
    pub key_pair: RsaKeyPair,
    pub mpsc_tx: Sender<Vec<u8>>,
    pub handlers: Arc<Mutex<HashMap<String, ProtocolHanldler>>>,
    pub global_event_tx: Sender<Vec<u8>>,
    pub peerstore: Arc<Mutex<PeerData>>,

    // Protocols
    pub floodsub: Arc<Mutex<Option<Arc<FloodSub>>>>,
    pub ping: Arc<Mutex<Option<Arc<Ping>>>>,
}

impl Node {
    pub fn new(
        mpsc_tx: Sender<Vec<u8>>,
        key_pair: RsaKeyPair,
        handlers: Arc<Mutex<HashMap<String, ProtocolHanldler>>>,
        peerstore: Arc<Mutex<PeerData>>,
        local_peer_info: PeerInfo,
        global_event_tx: Sender<Vec<u8>>,
    ) -> Self {
        Node {
            local_peer_info,
            key_pair,
            mpsc_tx,
            handlers,
            global_event_tx,
            peerstore,

            floodsub: Arc::new(Mutex::new(None)),
            ping: Arc::new(Mutex::new(None)),
        }
    }
}

#[async_trait]
impl INode for Node {
    async fn connect(&self, maddr: &Multiaddr) -> Result<()> {
        let frame = build_host_frame(HostMpscTxFlag::Connect, maddr.to_string(), None);
        self.write(frame).await.unwrap();
        Ok(())
    }

    async fn new_stream(&self, maddr: &str, protocols: Vec<String>) -> Result<()> {
        let frame = build_host_frame(
            HostMpscTxFlag::NewStream,
            maddr.to_string(),
            Some(protocols),
        );
        self.write(frame).await.unwrap();
        Ok(())
    }

    async fn on_disconnect(&self, peer_id: &str) -> Result<()> {
        let frame = build_host_frame(HostMpscTxFlag::Disconnect, peer_id.to_string(), None);
        self.write(frame).await.unwrap();
        Ok(())
    }

    async fn write(&self, notification: Vec<u8>) -> Result<()> {
        if let Err(e) = self.mpsc_tx.send(notification).await {
            return Err(Error::msg(format!("mpsc-receiver dropped: {}", e)));
        }

        Ok(())
    }
}

impl Node {
    pub fn get_local(&self) -> PeerInfo {
        self.local_peer_info.clone()
    }

    pub async fn get_addr(&self, peer_id: &String) -> Multiaddr {
        let peerstore = self.peerstore.lock().await;
        let peerinfo = peerstore.peer_store.get(peer_id).expect("Peer not found");

        Multiaddr::new(&peerinfo.listen_addr).unwrap()
    }

    pub async fn get_peers(&self) -> Vec<Multiaddr> {
        let peerstore = self.peerstore.lock().await;

        peerstore
            .peer_store
            .values()
            .filter_map(|peerinfo| Multiaddr::new(&peerinfo.listen_addr).ok())
            .collect()
    }

    pub async fn set_stream_handler(
        &self,
        protocol: &str,
        handler: ProtocolHanldler,
    ) -> Result<()> {
        let mut handler_registry = self.handlers.lock().await;
        handler_registry.insert(protocol.to_string(), handler);

        Ok(())
    }

    pub async fn initiate_protocol(
        &self,
        local_peer: &PeerInfo,
        protocol_opt: InnerProtocolOpt,
    ) -> Result<()> {
        match protocol_opt {
            InnerProtocolOpt::Floodsub => {
                let mut floodsub_guard = self.floodsub.lock().await;

                match &*floodsub_guard {
                    Some(_) => {
                        warn!("Floodsub already running!!")
                    }
                    None => {
                        let floodsub = FloodSub::new(local_peer, self.global_event_tx.clone())
                            .await
                            .unwrap();
                        self.set_stream_handler(FLOODSUB, Box::new(floodsub.clone()))
                            .await
                            .unwrap();

                        *floodsub_guard = Some(floodsub);
                        info!("Floodsub running!!");
                    }
                }
            }

            InnerProtocolOpt::Ping => {
                let mut ping_guard = self.ping.lock().await;

                match &*ping_guard {
                    Some(_) => {
                        warn!("Ping already running!!");
                    }
                    None => {
                        let ping = Arc::new(Ping::new(None, self.global_event_tx.clone()));
                        self.set_stream_handler(PING, Box::new(ping.clone()))
                            .await
                            .unwrap();

                        *ping_guard = Some(ping);
                        info!("Ping running!!");
                    }
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl INodeFloodsubAPI for Node {
    async fn floodsub_subscribe(&self, topic: String) -> Result<()> {
        match self.floodsub_is_running().await {
            false => warn!("Floodsub not running!!"),
            true => {
                let floodsub_guard = self.floodsub.lock().await;
                let floodsub = floodsub_guard.as_ref().unwrap();

                floodsub.subscribe(topic).await.unwrap();
            }
        }

        Ok(())
    }

    async fn floodsub_unsubscribe(&self, topics: Vec<String>) -> Result<()> {
        match self.floodsub_is_running().await {
            false => warn!("Floodsub not running!!"),
            true => {
                let floodsub_guard = self.floodsub.lock().await;
                let floodsub = floodsub_guard.as_ref().unwrap();

                floodsub.unsubscribe(topics).await.unwrap();
            }
        }

        Ok(())
    }

    async fn floodsub_publish(&self, topic: String, msg: Vec<u8>) -> Result<()> {
        match self.floodsub_is_running().await {
            false => warn!("Floodsub not running!!"),
            true => {
                let floodsub_guard = self.floodsub.lock().await;
                let floodsub = floodsub_guard.as_ref().unwrap();

                let frame = build_floodsub_api_frame(
                    SubAPIMpscFlag::Publish,
                    None,
                    Some(vec![topic]),
                    Some(msg),
                );

                floodsub.floodsub_mpsc_tx.send(frame).await.unwrap();
            }
        }

        // Some jitter after each publish
        tokio::time::sleep(Duration::from_millis(400)).await;

        Ok(())
    }

    async fn floodsub_is_running(&self) -> bool {
        let floodsub_guard = self.floodsub.lock().await;

        match &*floodsub_guard {
            Some(_) => {
                return true;
            }
            None => {
                return false;
            }
        }
    }

    async fn floodsub_topics(&self) -> Option<Vec<String>> {
        match self.floodsub_is_running().await {
            false => warn!("Floodsub not running!!"),
            true => {
                let floodsub_guard = self.floodsub.lock().await;
                let floodsub = floodsub_guard.as_ref().unwrap();

                return floodsub.get_subscribed_topics().await;
            }
        }

        None
    }

    async fn floodsub_peers(&self) -> Option<Vec<String>> {
        match self.floodsub_is_running().await {
            false => warn!("Floodsub not running!!"),
            true => {
                let floodsub_guard = self.floodsub.lock().await;
                let floodsub = floodsub_guard.as_ref().unwrap();

                return floodsub.get_connected_peers().await;
            }
        }

        None
    }

    async fn floodsub_mesh(&self) -> Option<HashMap<String, Vec<String>>> {
        match self.floodsub_is_running().await {
            false => warn!("Floodsub not running!!"),
            true => {
                let floodsub_guard = self.floodsub.lock().await;
                let floodsub = floodsub_guard.as_ref().unwrap();

                let mesh = floodsub.get_floodsub_mesh().await;
                let mut mapped_mesh = HashMap::new();

                for (topic, peers) in mesh {
                    let mut peer_addrs = Vec::new();

                    for peer in peers {
                        peer_addrs.push(self.get_addr(&peer).await.to_string());
                    }

                    mapped_mesh.insert(topic, peer_addrs);
                }

                return Some(mapped_mesh);
            }
        }

        None
    }
}

#[async_trait]
impl INodePingAPI for Node {
    async fn ping(&self, count: Option<u32>, maddr: &str) -> Result<()> {
        match self.ping_is_running().await {
            false => warn!("Ping not running!!"),
            true => {
                let ping_guard = self.ping.lock().await;
                let ping = ping_guard.as_ref().unwrap();

                {
                    let mut ping_count = ping.count.lock().await;
                    *ping_count = count.unwrap_or(0);
                }

                self.new_stream(maddr, vec![PING.to_string()])
                    .await
                    .unwrap();
            }
        }

        Ok(())
    }

    async fn ping_is_running(&self) -> bool {
        let ping = self.ping.lock().await;

        match &*ping {
            Some(_) => {
                return true;
            }
            None => {
                return false;
            }
        }
    }
}
