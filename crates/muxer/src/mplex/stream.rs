use std::{collections::HashMap, sync::Arc};

use anyhow::{Error, Ok, Result};
use async_trait::async_trait;
use identity::peer::PeerInfo;
use identity::traits::muxer::IMuxedStream;
use tokio::sync::{
    mpsc::{Receiver, Sender},
    Mutex,
};

use crate::{
    mplex::{
        conn::INTERNAL,
        headers::{build_frame, MuxedStreamFlag},
    },
    upgrader::ProtocolHanldler,
};

pub struct MplexStream {
    muxed_conn_mpsc_tx: Sender<Vec<u8>>,
    muxed_stream_mpsc_rx: Receiver<Vec<u8>>,
    pub stream_id: u32,
    pub is_initiator: bool,
    pub remote_peer_info: PeerInfo,
    handlers: Arc<Mutex<HashMap<String, ProtocolHanldler>>>,
    pub global_event_tx: Sender<Vec<u8>>,
}

impl MplexStream {
    pub fn new(
        muxed_conn_mpsc_tx: Sender<Vec<u8>>,
        muxed_stream_mpsc_rx: Receiver<Vec<u8>>,
        stream_id: u32,
        is_initiator: bool,
        remote_peer_info: PeerInfo,
        handlers: Arc<Mutex<HashMap<String, ProtocolHanldler>>>,
        global_event_tx: Sender<Vec<u8>>,
    ) -> MplexStream {
        MplexStream {
            muxed_conn_mpsc_tx,
            muxed_stream_mpsc_rx,
            stream_id,
            is_initiator,
            remote_peer_info,
            handlers,
            global_event_tx,
        }
    }

    pub fn peer_info(&self) -> PeerInfo {
        self.remote_peer_info.clone()
    }
}

#[async_trait]
impl IMuxedStream for MplexStream {
    async fn write(&self, msg: &Vec<u8>) -> Result<()> {
        let frame = build_frame(self.stream_id, MuxedStreamFlag::Message, msg);
        self.muxed_conn_mpsc_tx.send(frame).await?;

        Ok(())
    }

    async fn read(&mut self) -> Result<Vec<u8>> {
        match self.muxed_stream_mpsc_rx.recv().await {
            Some(payload) => return Ok(payload),
            None => return Err(Error::msg("mpsc receiver down")),
        }
    }

    async fn close(&self) -> Result<()> {
        let mut frame = build_frame(self.stream_id, MuxedStreamFlag::CloseStream, &vec![]);
        frame.splice(0..0, INTERNAL);

        self.muxed_conn_mpsc_tx.send(frame).await?;

        Ok(())
    }

    async fn handle_conn(mut self, proto: Option<Vec<u8>>) -> Result<()> {
        match self.negotiate(proto).await {
            Some(protocol) => {
                let handler = {
                    let handler_registry = self.handlers.lock().await;
                    handler_registry
                        .get(&protocol)
                        .cloned()
                        .ok_or_else(|| Error::msg("Protocol not found"))
                        .unwrap()
                };

                handler.stream_handler(Box::new(self)).await.unwrap();
            }
            None => return Err(Error::msg("Protocol negotiation failed")),
        }

        Ok(())
    }

    async fn negotiate(&mut self, proto: Option<Vec<u8>>) -> Option<String> {
        match self.is_initiator {
            true => {
                let protocol = proto.unwrap();
                let frame = build_frame(self.stream_id, MuxedStreamFlag::HandshakeReq, &protocol);
                self.muxed_conn_mpsc_tx.send(frame).await.unwrap();

                let res = self.read().await.unwrap();
                if protocol != res {
                    return None;
                }

                let protocol = String::from_utf8(protocol).unwrap();
                return Some(protocol);
            }
            false => {
                let payload = self.read().await.unwrap();
                let protocol = String::from_utf8(payload.clone()).unwrap();

                let protocols: Vec<String> = {
                    let handler_registry = self.handlers.lock().await;
                    handler_registry.keys().cloned().collect()
                };

                if !protocols.contains(&protocol) {
                    return None;
                }

                let frame = build_frame(self.stream_id, MuxedStreamFlag::HandshakeRes, &payload);
                self.muxed_conn_mpsc_tx.send(frame).await.unwrap();

                return Some(protocol);
            }
        }
    }

    fn is_initiator(&self) -> bool {
        self.is_initiator
    }

    fn get_peer_id(&self) -> String {
        self.remote_peer_info.peer_id.clone()
    }
}
