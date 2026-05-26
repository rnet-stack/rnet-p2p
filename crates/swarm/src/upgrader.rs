use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use identity::peer::PeerInfo;
use identity::traits::core::{IRawConnection, IReadWriteClose};
use muxer::conn::MuxedConn;
use muxer::upgrader::MuxerUpgrader;
use security::{conn::SecureConn, upgrader::SecurityUpgrader};
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;

use crate::inner::ProtocolHanldler;

pub struct ConnUpgrader {
    sec_upgrader: SecurityUpgrader,
    mux_upgrader: MuxerUpgrader,
}

impl Default for ConnUpgrader {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnUpgrader {
    pub fn new() -> Self {
        ConnUpgrader {
            sec_upgrader: SecurityUpgrader::new(),
            mux_upgrader: MuxerUpgrader::new(),
        }
    }

    pub async fn update_security(
        &self,
        stream: Box<dyn IReadWriteClose + Send + Sync + 'static>,
        is_initiator: bool,
    ) -> Result<SecureConn> {
        Ok(self
            .sec_upgrader
            .update(stream, is_initiator)
            .await
            .unwrap())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn update_muxer<T>(
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
            .mux_upgrader
            .update(
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
