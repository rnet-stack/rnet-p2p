use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use identity::peer::PeerInfo;
use identity::traits::core::{IProtocolHandler, IRawConnection};
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;

use crate::conn::MuxedConn;
use crate::transport::MuxerTransport;

pub type ProtocolHanldler = Box<Arc<dyn IProtocolHandler + Send + Sync + 'static>>;

pub struct MuxerUpgrader {
    transport: MuxerTransport,
}

impl Default for MuxerUpgrader {
    fn default() -> Self {
        Self::new()
    }
}

impl MuxerUpgrader {
    pub fn new() -> Self {
        MuxerUpgrader {
            transport: MuxerTransport::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn update<T>(
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
            .transport
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
}
