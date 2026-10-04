use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use identity::{
    peer::PeerInfo,
    traits::{core::IProtocolHandler, muxer::IMuxedStream},
};
use std::result::Result::Ok;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing::error;

use crate::{msg_schema::PubsubMsg, store::PubsubStore};

pub type TopicID = String;
pub type PeerID = String;
pub type MessageID = [u8; 32];

pub enum PubsubCommands {
    NewPeer(Sender<Vec<u8>>, PeerID),
    Incoming(PubsubMsg, PeerID),
    Subscribe(TopicID),
    Unsubscribe(TopicID),
    Publish(Vec<u8>, TopicID),
    Heartbeat,
    DeadPeer(PeerID),
    // TODO: add pubsub-store commands
}

#[derive(Debug)]
pub struct Pubsub {
    pub local_peer_info: PeerInfo,
    pub api_tx: Sender<PubsubCommands>,
    pub global_tx: Sender<Vec<u8>>,
}

#[async_trait]
impl IProtocolHandler for Pubsub {
    async fn stream_handler(
        &self,
        mut stream: Box<dyn IMuxedStream + Send + Sync + 'static>,
    ) -> Result<()> {
        let remote_peer_id = stream.get_peer_id();
        let (stream_tx, mut stream_rx) = mpsc::channel::<Vec<u8>>(100);

        self.api_tx
            .send(PubsubCommands::NewPeer(stream_tx, remote_peer_id.clone()))
            .await
            .unwrap();

        // TODO: maybe add a 100 ms, time-gap here??

        loop {
            tokio::select! {
                incoming = stream.read() => {
                    match incoming {
                        Ok(incoming) => {
                            // TODO: deserialize the incoming: Vec<u8> to pubsub-msg
                            // then send it off to handle-incoming

                            let msg: PubsubMsg = bincode::deserialize(&incoming).unwrap();
                            self.api_tx.send(PubsubCommands::Incoming(msg, stream.get_peer_id())).await.unwrap();
                        }
                        Err(_) => break
                    }
                }

                Some(data) = stream_rx.recv() => {
                    if let Err(e) = stream.write(&data).await {
                        error!("Error while writing in stream of peer: {}, {}", remote_peer_id, e);
                        break;
                    }
                }
            }
        }

        // TODO: send pubsub-command DEAD-PEER to internal-api
        self.api_tx
            .send(PubsubCommands::DeadPeer(remote_peer_id))
            .await
            .unwrap();

        Ok(())
    }
}

impl Pubsub {
    pub async fn new(local_peer_info: PeerInfo, global_tx: Sender<Vec<u8>>) -> Result<Arc<Self>> {
        let (api_tx, api_rx) = mpsc::channel::<PubsubCommands>(300);
        let pubsub = Arc::new(Pubsub {
            local_peer_info,
            api_tx,
            global_tx,
        });

        // TODO: spawn a task to periodically initiate hearbeat-sequence
        // TODO: spawn the handle-internal-api

        let api_pubsub = pubsub.clone();
        tokio::spawn(async move {
            api_pubsub.handle_internal_api(api_rx).await;
        });

        Ok(pubsub)
    }

    pub async fn handle_internal_api(&self, mut api_rx: Receiver<PubsubCommands>) {
        let mut pubsub_store = PubsubStore::new();

        loop {
            if let Some(msg) = api_rx.recv().await {
                match msg {
                    PubsubCommands::NewPeer(stream_tx, peer_id) => {}
                    PubsubCommands::Incoming(msg, peer_id) => {}
                    PubsubCommands::Subscribe(topic_id) => {}
                    PubsubCommands::Unsubscribe(topic_id) => {}
                    PubsubCommands::Publish(msg_bytes, topic_id) => {}
                    PubsubCommands::Heartbeat => {}
                    PubsubCommands::DeadPeer(peer_id) => {} // todo: add pubsub-store commands
                }
            }
        }
    }
}
