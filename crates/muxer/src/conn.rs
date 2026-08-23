use std::{collections::HashMap, sync::Arc};

use anyhow::{Error, Result};
use identity::multiaddr::Multiaddr;
use identity::peer::PeerInfo;
use identity::traits::core::ISwarm;
use identity::traits::{core::IRawConnection, muxer::IMuxedConn};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::Mutex;
use tokio::time::timeout;
use tracing::debug;

use std::result::Result::Ok;
use std::time::Duration;

use crate::mplex::conn::MplexConn;
use crate::mplex::headers::{build_frame, MuxedStreamFlag};
use crate::upgrader::ProtocolHanldler;

const INTERNAL: [u8; 16] = *b"internal-payload";

// mplex-conn: IMuxedConn
// handle_incoming
// conn_handler
// write

// mplex-stream: IMuxedStream
// write
// read
// server_handshake
// client_handshake
pub struct MuxedConn {
    conn: Box<dyn IMuxedConn>,
    pub is_initiator: bool,
    pub remote_peer: PeerInfo,
    pub muxed_mpsc_tx: Sender<Vec<u8>>,
    pub global_event_tx: Sender<Vec<u8>>,
}

// TODO: in future if we want a unified conn-handler in MuxedConn itself,
// then `MuxedConn[raw_conn] + mplex/yamux mpsc channels` in which the routing
// will be from MuxedConn, but the decision making will be in internal
// multiplexing router i.e mplex / yamux
impl MuxedConn {
    #[allow(clippy::too_many_arguments)]
    pub async fn new<W>(
        protocol: &str,
        raw_conn: W,
        is_initiator: bool,
        remote_peer: PeerInfo,
        handlers: Arc<Mutex<HashMap<String, ProtocolHanldler>>>,
        muxed_mpsc_rx: Receiver<Vec<u8>>,
        muxed_mpsc_tx: Sender<Vec<u8>>,
        global_event_tx: Sender<Vec<u8>>,
    ) -> Result<Self>
    where
        W: IRawConnection + Send + Sync + 'static,
    {
        let (ping_check_tx, ping_check_rx) = mpsc::channel::<Vec<u8>>(100);

        let muxed_conn = match protocol {
            "mplex" => {
                let mplex_conn = MplexConn::new(
                    raw_conn,
                    remote_peer.clone(),
                    handlers,
                    muxed_mpsc_tx.clone(),
                    muxed_mpsc_rx,
                    global_event_tx.clone(),
                    ping_check_tx,
                );

                Ok(MuxedConn {
                    conn: Box::new(mplex_conn),
                    is_initiator,
                    remote_peer: remote_peer.clone(),
                    muxed_mpsc_tx: muxed_mpsc_tx.clone(),
                    global_event_tx,
                })
            }
            _ => Err(Error::msg("protocol not found")),
        };

        // TODO: this is supposed to happen only for UDP
        tokio::spawn(async move {
            ping_check(ping_check_rx, muxed_mpsc_tx, is_initiator, remote_peer)
                .await
                .unwrap();
        });

        muxed_conn
    }

    pub async fn conn_handler(
        mut self,
        peer_id: &str,
        swarm_mpsc_tx: Arc<dyn ISwarm + Send + Sync + 'static>,
    ) -> Result<()> {
        let peer_id = peer_id.to_string();
        tokio::spawn(async move {
            self.conn
                .conn_handler(&peer_id, swarm_mpsc_tx)
                .await
                .unwrap();
        });

        Ok(())
    }
}

/// Only required for UDP connections
pub async fn ping_check(
    mut ping_check_rx: Receiver<Vec<u8>>,
    write_req_tx: Sender<Vec<u8>>,
    is_initiator: bool,
    remote_peer: PeerInfo,
) -> Result<()> {
    let remote_multiaddr = Multiaddr::new(&remote_peer.listen_addr).unwrap();
    if remote_multiaddr.value_for_protocol("tcp").is_some() {
        return Ok(());
    }
    debug!("Liveliness check initiating: {}", remote_peer.peer_id);

    let ping_payload = build_frame(0, MuxedStreamFlag::Liveliness, b"liveliness");
    loop {
        tokio::time::sleep(Duration::from_secs(5)).await;
        // debug!("Liveliness check: {}", remote_peer.peer_id);

        match is_initiator {
            true => {
                let exchange = async {
                    write_req_tx.send(ping_payload.clone()).await.unwrap();
                    ping_check_rx.recv().await.unwrap();
                };

                match timeout(Duration::from_secs(2), exchange).await {
                    Ok(()) => continue,
                    Err(_) => break,
                };
            }
            false => {
                let exchange = async {
                    ping_check_rx.recv().await.unwrap();
                    write_req_tx.send(ping_payload.clone()).await.unwrap();
                };

                match timeout(Duration::from_secs(2), exchange).await {
                    Ok(()) => continue,
                    Err(_) => break,
                };
            }
        };
    }

    // Send out the diconnection notification
    let mut disconnect_payload = build_frame(0, MuxedStreamFlag::Disconnected, b"disconnected");
    disconnect_payload.splice(0..0, INTERNAL);

    write_req_tx.send(disconnect_payload).await.unwrap();

    Ok(())
}
