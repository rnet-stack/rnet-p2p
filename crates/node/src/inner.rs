use identity::{
    keys::rsa::RsaKeyPair,
    multiaddr::{Multiaddr, Protocol},
    peer::PeerInfo,
    traits::core::{IProtocolHandler, ISwarm},
};

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};
use swarm::{inner::SwarmInner, swarm::Swarm};
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    Mutex,
};

use anyhow::Result;
use identity::peer::PeerData;
use std::result::Result::Ok;
use tracing::{debug, info};

use crate::{
    headers::{process_host_frame, HostMpscTxFlag},
    node::Node,
    protocol::InnerProtocolOpt,
};

// TODO: After sometime:
// Only one endpoint to connect with the whole rnet node: HostMpscTx
// Set protocols which are going to be active in host-initiation
// All the internal function calls will be event driven from HostMpscTx,
// and received from global_event_rx
// Integrate Swarm with BasicHost
// Fix the dependency tree

// Happy exams !!
pub type ProtocolHanldler = Box<Arc<dyn IProtocolHandler + Send + Sync + 'static>>;

pub struct NodeInner {
    pub key_pair: RsaKeyPair,
    pub peerstore: Arc<Mutex<PeerData>>,
    pub handlers: Arc<Mutex<HashMap<String, ProtocolHanldler>>>,

    pub node_mpsc_rx: Receiver<Vec<u8>>,
    pub node_mpsc_tx: Arc<Node>,

    pub swarm_mpsc_tx: Arc<Swarm>,
    pub global_event_tx: Sender<Vec<u8>>,
}

/// new
/// get_peer_id
/// get_addrs
/// get_pubkey
/// get_privkey
/// get_connected_peers
/// set_stream_handler
/// remove_stream_handler
/// disconnect
/// get_live_peers
/// is_peer_connected
impl NodeInner {
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(
        listen_addr: &mut Multiaddr,
        protocol_opt: Vec<InnerProtocolOpt>,
        key_pair: Option<String>,
    ) -> Result<(Arc<Node>, Receiver<Vec<u8>>)> {
        // generate rsa-keypair
        // handlers
        // create mpsc channels
        // create/initiate swarm

        // keypair/handlers generate
        let keypair = match key_pair {
            Some(hex) => RsaKeyPair::from_hex(hex).unwrap(),
            None => {
                debug!("Generating RSA keypair");
                RsaKeyPair::generate().unwrap()
            }
        };

        let peer_id = keypair.peer_id();
        listen_addr.push_proto(Protocol::P2P(peer_id.clone()));

        let handlers = Arc::new(Mutex::new(HashMap::new()));

        // mpsc channels
        let (global_event_tx, global_event_rx) = mpsc::channel::<Vec<u8>>(100);
        let (mpsc_tx, node_mpsc_rx) = mpsc::channel::<Vec<u8>>(100);

        // extract transport opt
        let transport_opt = match listen_addr.value_for_protocol("tcp") {
            Some(_) => "tcp",
            None => "udp",
        };

        // swarm/local_peer_info
        let (swarm_mpsc_tx, peerstore, local_peer_info) = SwarmInner::new(
            transport_opt,
            listen_addr,
            peer_id.clone(),
            handlers.clone(),
            global_event_tx.clone(),
        )
        .await
        .unwrap();

        // Node client interface
        let node_mpsc_tx = Arc::new(Node::new(
            mpsc_tx,
            keypair.clone(),
            handlers.clone(),
            peerstore.clone(),
            local_peer_info.clone(),
            global_event_tx.clone(),
        ));

        info!("Node listening on: {}", listen_addr.to_string());

        let node_inner = NodeInner {
            key_pair: keypair,
            peerstore,
            handlers,
            node_mpsc_rx,
            node_mpsc_tx: node_mpsc_tx.clone(),
            swarm_mpsc_tx,
            global_event_tx,
        };

        node_inner
            ._execute_protocols(protocol_opt, &local_peer_info)
            .await
            .unwrap();

        tokio::spawn(async move {
            node_inner.run().await.unwrap();
        });

        Ok((node_mpsc_tx, global_event_rx))
    }

    // migrate to swarm
    pub async fn run(mut self) -> Result<()> {
        let mut notification = VecDeque::<Vec<u8>>::new();

        loop {
            tokio::select! {
                Some(event) = self.node_mpsc_rx.recv() => {
                    notification.push_back(event);
                }

                _ = async {}, if !notification.is_empty() => {
                    let frame = notification.pop_front().unwrap();

                    // Process the data
                    let (flag, (str_pld, opt_vec_pld)): (HostMpscTxFlag, (String, Option<Vec<String>>)) = process_host_frame(frame).unwrap();
                    match flag {

                        HostMpscTxFlag::Connect => {
                            let maddr = Multiaddr::new(&str_pld).unwrap();
                            self.swarm_mpsc_tx.connect(&maddr).await.unwrap();
                        },

                        HostMpscTxFlag::NewStream => {
                            let maadr = Multiaddr::new(&str_pld).unwrap().to_string();
                            let protocols = opt_vec_pld.unwrap();
                            self.swarm_mpsc_tx.new_stream(&maadr, protocols).await.unwrap();
                        },

                        HostMpscTxFlag::Disconnect => {
                            let peer_id = &str_pld;
                            self.swarm_mpsc_tx.on_disconnect(peer_id).await.unwrap();
                        },
                    }
                }
            }
        }

        // TODO: ERROR HANDLING
    }

    async fn _execute_protocols(
        &self,
        protocol_opt: Vec<InnerProtocolOpt>,
        local_peer: &PeerInfo,
    ) -> Result<()> {
        // This happens in impl Node
        for opt in protocol_opt {
            match opt {
                InnerProtocolOpt::Floodsub => {
                    self.node_mpsc_tx
                        .initiate_protocol(local_peer, opt)
                        .await
                        .unwrap();
                }
                InnerProtocolOpt::Ping { .. } => {
                    self.node_mpsc_tx
                        .initiate_protocol(local_peer, opt)
                        .await
                        .unwrap();
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_headers() {
        let maddr = Multiaddr::new("ip4/127.0.0.1/tcp/0")
            .unwrap()
            .to_string()
            .into_bytes();

        let string = String::from_utf8(maddr).unwrap();
        println!("{}", string);
    }
}
