use std::{collections::HashMap, sync::Arc};

use crate::{
    headers::{process_host_frame, SwarmMpscTxFlag},
    swarm::Swarm,
    upgrader::ConnUpgrader,
};
use anyhow::Result;
use identity::{
    multiaddr::Multiaddr,
    peer::{PeerData, PeerInfo},
    traits::{
        core::{IMultistream, IProtocolHandler, IRawConnection, IReadWriteClose, ISwarm},
        security::ISecuredConn,
    },
};
use multistream::multiselect::Multiselect;
use muxer::{
    conn::MuxedConn,
    mplex::headers::{build_frame, MuxedStreamFlag},
};
use security::conn::SecureConn;
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    Mutex,
};
use tracing::{info, warn};
use transport::{raw_conn::RawConnection, transport::Transport};

type ConnectionMap = HashMap<String, Sender<Vec<u8>>>;
pub type ProtocolHanldler = Box<Arc<dyn IProtocolHandler + Send + Sync + 'static>>;

const INTERNAL: [u8; 16] = *b"internal-payload";

pub struct SwarmInner {
    pub transport: Transport,
    pub upgrader: ConnUpgrader,
    pub connections: Arc<Mutex<ConnectionMap>>,
    pub peerstore: Arc<Mutex<PeerData>>,
    pub multiselect: Multiselect,
    handlers: Arc<Mutex<HashMap<String, ProtocolHanldler>>>,
    pub swarm_mpsc_tx: Arc<dyn ISwarm + Send + Sync + 'static>,
    pub global_event_tx: Sender<Vec<u8>>,
    /// liveliness check over udp transport
    pub ping_check_opt: bool,
}

// get_peer_id
// set_stream_handler
// get_connections
// get_total_connections
// dial_peer
// upgrade_outbound
// upgrade_inbound
// new_stream
// listen
// close
// close_peer
// add_conn
// notifications/notifiee all
impl SwarmInner {
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(
        transport_opt: &str,
        listen_addr: &mut Multiaddr,
        peer_id: String,
        handlers: Arc<Mutex<HashMap<String, ProtocolHanldler>>>,
        global_event_tx: Sender<Vec<u8>>,
        ping_check_opt: bool,
    ) -> Result<(Arc<Swarm>, Arc<Mutex<PeerData>>, PeerInfo)> {
        // create transport
        // update the actual listen ip
        // create peerstore
        // create swarm-interfaces
        // create swarm-inner
        // return

        let (transport, listen_ip) = Transport::new(transport_opt, listen_addr).await.unwrap();

        // extract transport opt
        let transport_opt = match listen_addr.value_for_protocol("tcp") {
            Some(_) => "tcp",
            None => "udp",
        };

        // update the listen addr, with actual random port
        let parts: Vec<&str> = listen_ip.split(':').collect();
        listen_addr
            .replace_value_for_protocol(transport_opt, parts[1])
            .unwrap();

        // Create the peerstore
        let local_peer_info = PeerInfo {
            peer_id,
            listen_addr: listen_addr.clone().to_string(),
        };

        let peerstore = Arc::new(Mutex::new(PeerData {
            peer_info: local_peer_info.clone(),
            peer_store: HashMap::new(),
        }));

        // Create swarm client interfaces
        let (mpsc_tx, swarm_mpsc_rx) = mpsc::channel::<Vec<u8>>(100);
        let swarm_mpsc_tx = Arc::new(Swarm {
            swarm_mpsc_tx: mpsc_tx,
        });

        let swarm_inner = Arc::new(SwarmInner {
            transport,
            upgrader: ConnUpgrader::new(),
            connections: Arc::new(Mutex::new(HashMap::new())),
            peerstore: peerstore.clone(),
            multiselect: Multiselect {},
            handlers,
            swarm_mpsc_tx: swarm_mpsc_tx.clone(),
            global_event_tx,
            ping_check_opt,
        });

        tokio::spawn(async move {
            swarm_inner.initiate(swarm_mpsc_rx).await.unwrap();
        });

        Ok((swarm_mpsc_tx, peerstore, local_peer_info))
    }

    pub async fn liveliness_checkup(&self) -> Result<()> {
        todo!();
    }

    pub async fn initiate(&self, mut swarm_mpsc_rx: Receiver<Vec<u8>>) -> Result<()> {
        loop {
            tokio::select! {
                Ok((stream, _addr)) = self.transport.accept() => {
                    self.conn_handler(stream, false).await.unwrap();
                }

                Some(frame) = swarm_mpsc_rx.recv() => {
                    let (flag, (str_pld, opt_vec_pld)): (SwarmMpscTxFlag, (String, Option<Vec<String>>)) = process_host_frame(frame).unwrap();
                    match flag {

                        SwarmMpscTxFlag::Connect => {
                            let maddr = Multiaddr::new(&str_pld).unwrap();
                            self.connect(&maddr).await.unwrap();
                        },

                        SwarmMpscTxFlag::NewStream => {
                            let maadr = Multiaddr::new(&str_pld).unwrap();
                            let protocols = opt_vec_pld.unwrap();
                            self.new_stream(&maadr, protocols).await;
                        },

                        SwarmMpscTxFlag::Disconnect => {
                            let peer_id = &str_pld;
                            self.on_disconnect(peer_id).await.unwrap();
                        },
                    }
                }
            }
        }
    }

    pub async fn conn_handler(
        &self,
        stream: Box<dyn IReadWriteClose + Send + Sync + 'static>,
        is_initiator: bool,
    ) -> Result<()> {
        // security-update
        // identify
        // muxer
        // protocol

        let local = {
            let data = self.peerstore.lock().await;
            data.peer_info.clone()
        };

        // ---- SECURED + IDENTIFIED + MUXED + PEER-STORE----
        let secured_conn = self.update_security(stream, is_initiator).await.unwrap();
        let (raw_conn, remote_peer) = self
            .identify(&local, secured_conn, is_initiator)
            .await
            .unwrap();

        let (muxed_conn, muxed_mpsc_tx) = self
            .update_muxer(raw_conn, is_initiator, remote_peer.clone())
            .await
            .unwrap();

        self.update_peerstore(remote_peer.clone(), muxed_mpsc_tx)
            .await
            .unwrap();
        // ---------------------------------------------------

        info!("New peer connected: {}", remote_peer.peer_id);
        let swarm_mpsc_tx = self.swarm_mpsc_tx.clone();

        tokio::spawn(async move {
            muxed_conn
                .conn_handler(&remote_peer.peer_id, swarm_mpsc_tx)
                .await
                .unwrap();
        });

        Ok(())
    }

    async fn connect(&self, addr: &Multiaddr) -> Result<()> {
        let remote_peer_id = addr.value_for_protocol("p2p").unwrap();

        match self.is_peer_connected(&remote_peer_id).await {
            true => {
                warn!("Peer already connected: {}", remote_peer_id);
                return Ok(());
            }
            false => {
                let stream = self.transport.dial(addr).await.unwrap();
                self.conn_handler(stream, true).await.unwrap();
            }
        }

        Ok(())
    }

    async fn new_stream(&self, maddr: &Multiaddr, protocols: Vec<String>) {
        let peer_id = maddr.value_for_protocol("p2p").unwrap();

        match self.is_peer_connected(&peer_id).await {
            true => {
                warn!("Peer already connected: {}", peer_id);
            }
            false => {
                warn!("Making a connection first: {peer_id}");
                self.connect(maddr).await.unwrap();
            }
        }

        // fetch the muxed_conn instance
        let mut connections = self.connections.lock().await;
        let muxed_mpsc_tx = connections
            .get_mut(&peer_id)
            .ok_or_else(|| anyhow::anyhow!("No such peer"))
            .expect("Make a connection first")
            .clone();

        // send the new_stream header
        let mut new_stream_frame =
            build_frame(0, MuxedStreamFlag::NewStream, protocols[0].as_bytes());
        new_stream_frame.splice(0..0, INTERNAL);
        muxed_mpsc_tx.send(new_stream_frame).await.unwrap();
    }

    async fn update_security(
        &self,
        stream: Box<dyn IReadWriteClose + Send + Sync + 'static>,
        is_initiator: bool,
    ) -> Result<SecureConn> {
        Ok(self
            .upgrader
            .update_security(stream, is_initiator)
            .await
            .unwrap())
    }

    async fn update_muxer<W>(
        &self,
        stream: W,
        is_initiator: bool,
        remote_peer: PeerInfo,
    ) -> Result<(MuxedConn, Sender<Vec<u8>>)>
    where
        W: IRawConnection + Send + Sync + 'static,
    {
        self.upgrader
            .update_muxer(
                stream,
                is_initiator,
                remote_peer,
                self.handlers.clone(),
                self.global_event_tx.clone(),
                self.ping_check_opt,
            )
            .await
    }

    async fn identify<X>(
        &self,
        local_peer: &PeerInfo,
        stream: X,
        is_initiator: bool,
    ) -> Result<(RawConnection<X>, PeerInfo)>
    where
        X: ISecuredConn + Send + 'static,
    {
        let raw_conn = self
            .multiselect
            .handshake(local_peer, stream, is_initiator)
            .await
            .unwrap();

        let remote = raw_conn.peer_info();

        Ok((raw_conn, remote))
    }

    async fn update_peerstore(
        &self,
        remote_peer: PeerInfo,
        muxed_mpsc_tx: Sender<Vec<u8>>,
    ) -> Result<()> {
        let mut peerstore = self.peerstore.lock().await;
        peerstore
            .peer_store
            .insert(remote_peer.peer_id.clone(), remote_peer.clone());

        let mut connections = self.connections.lock().await;
        connections.insert(remote_peer.peer_id.clone(), muxed_mpsc_tx.clone());

        Ok(())
    }

    async fn on_disconnect(&self, peer_id: &str) -> Result<()> {
        let mut peerstore = self.peerstore.lock().await;
        let remote_peer_info = peerstore.peer_store.remove(peer_id).unwrap();
        let remote_listen_addr = Multiaddr::new(&remote_peer_info.listen_addr).unwrap();
        let transport_protocol = remote_listen_addr.value_for_protocol("udp");

        let mut connections = self.connections.lock().await;

        // TODO: this is necessary only when on UDP
        if transport_protocol.is_some() {
            let muxed_mpsc_tx = connections.get_mut(peer_id).unwrap().clone();
            let mut disconnect_frame = build_frame(0, MuxedStreamFlag::Disconnected, b"");
            disconnect_frame.splice(0..0, INTERNAL);
            muxed_mpsc_tx.send(disconnect_frame).await.unwrap();
        }

        connections.remove(peer_id);
        warn!("Peer disconnected: {}", peer_id);

        Ok(())
    }

    async fn is_peer_connected(&self, peer_id: &str) -> bool {
        let connections = self.connections.lock().await;
        connections.contains_key(peer_id)
    }
}
