use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Error, Result};
use identity::peer::PeerInfo;
use identity::traits::core::IRawConnection;
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::Mutex;

use crate::conn::MuxedConn;
use crate::mplex::conn::MPLEX;
use crate::upgrader::ProtocolHanldler;

pub const MULTISELECT_CONNECT: &str = "mutilselect/0.0.1";

pub struct MuxerTransport {
    pub muxer_opts: Vec<String>,
}

impl Default for MuxerTransport {
    fn default() -> Self {
        Self::new()
    }
}

/// mux_conn
/// handshake_inbound
/// handshake_outbound
/// try_select
/// select_transport
impl MuxerTransport {
    pub fn new() -> Self {
        let muxer_opts = vec![String::from("mplex")];

        MuxerTransport { muxer_opts }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn mux_conn<T>(
        &self,
        stream: T,
        is_initiator: bool,
        remote_peer: PeerInfo,
        handlers: Arc<Mutex<HashMap<String, ProtocolHanldler>>>,
        global_event_tx: Sender<Vec<u8>>,
        ping_check_opt: bool,
    ) -> Result<(MuxedConn, Sender<Vec<u8>>)>
    where
        T: IRawConnection + Send + Sync + 'static,
    {
        Ok(self
            .handshake(
                stream,
                is_initiator,
                remote_peer,
                handlers,
                global_event_tx,
                ping_check_opt,
            )
            .await
            .unwrap())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn handshake<T>(
        &self,
        mut stream: T,
        is_initiator: bool,
        remote_peer: PeerInfo,
        handlers: Arc<Mutex<HashMap<String, ProtocolHanldler>>>,
        global_event_tx: Sender<Vec<u8>>,
        ping_check_opt: bool,
    ) -> Result<(MuxedConn, Sender<Vec<u8>>)>
    where
        T: IRawConnection + Send + Sync + 'static,
    {
        // Select on the protocol to use
        // TODO: Do this properly: muxer-opts priority

        self.try_select(&mut stream, MULTISELECT_CONNECT, is_initiator)
            .await
            .unwrap();

        self.try_select(&mut stream, MPLEX, is_initiator)
            .await
            .unwrap();

        let (muxed_mpsc_tx, muxed_mpsc_rx) = mpsc::channel::<Vec<u8>>(100);

        let muxed_conn = MuxedConn::new(
            "mplex",
            stream,
            is_initiator,
            remote_peer,
            handlers,
            muxed_mpsc_rx,
            muxed_mpsc_tx.clone(),
            global_event_tx,
            ping_check_opt,
        )
        .await
        .unwrap();

        Ok((muxed_conn, muxed_mpsc_tx))
    }

    async fn try_select<T>(&self, stream: &mut T, proto: &str, is_initiator: bool) -> Result<()>
    where
        T: IRawConnection,
    {
        match is_initiator {
            true => {
                let proto_bytes = bincode::serialize(&proto)?;
                stream.write(&proto_bytes).await?;

                let msg_bytes = stream.read().await.unwrap();
                let received: String = bincode::deserialize(&msg_bytes)?;

                if received.as_str() == proto {
                    return Ok(());
                }
            }

            false => {
                let msg_bytes = stream.read().await.unwrap();
                let received: String = bincode::deserialize(&msg_bytes)?;

                if received.as_str() == proto {
                    let proto_bytes = bincode::serialize(&proto)?;
                    stream.write(&proto_bytes).await.unwrap();
                    return Ok(());
                }
            }
        }

        Err(Error::msg("Negotiation failed"))
    }

    pub fn select_transport(&self) -> Result<()> {
        Ok(())
    }
}
