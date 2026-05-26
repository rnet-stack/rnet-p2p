use anyhow::{Error, Result};
use async_trait::async_trait;
use identity::peer::PeerInfo;
use identity::traits::core::ISwarm;
use identity::traits::muxer::IMuxedStream;
use identity::traits::{core::IRawConnection, muxer::IMuxedConn};
use tokio::sync::Mutex;
use tracing::warn;

use std::future::Future;
use std::pin::Pin;
use std::result::Result::Ok;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::mplex::headers::{build_frame, process_header, MuxedStreamFlag};
use crate::mplex::stream::MplexStream;
use crate::upgrader::ProtocolHanldler;

pub type AsyncHandler = Arc<
    dyn Fn(
            Box<dyn IMuxedStream + Send + Sync + 'static>,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>
        + Send
        + Sync,
>;

pub const INTERNAL: [u8; 16] = *b"internal-payload";
pub const MPLEX: &str = "rnet/mplex/0.0.1";

pub struct MplexConn<T>
where
    T: IRawConnection,
{
    pub raw_conn: T,
    pub remote_peer_info: PeerInfo,
    pub streams: HashMap<u32, Sender<Vec<u8>>>,
    _stream_counter: u32,
    handlers: Arc<Mutex<HashMap<String, ProtocolHanldler>>>,
    pub mpsc_tx: Sender<Vec<u8>>,
    pub mpsc_rx: Receiver<Vec<u8>>,
    pub global_event_tx: Sender<Vec<u8>>,
    pub ping_check_tx: Sender<Vec<u8>>,
}

impl<T> MplexConn<T>
where
    T: IRawConnection,
{
    pub fn new(
        raw_conn: T,
        remote_peer: PeerInfo,
        handlers: Arc<Mutex<HashMap<String, ProtocolHanldler>>>,
        mpsc_tx: Sender<Vec<u8>>,
        mpsc_rx: Receiver<Vec<u8>>,
        global_event_tx: Sender<Vec<u8>>,
        ping_check_tx: Sender<Vec<u8>>,
    ) -> MplexConn<T> {
        MplexConn {
            raw_conn,
            remote_peer_info: remote_peer,
            streams: HashMap::new(),
            _stream_counter: 0,
            handlers,
            mpsc_tx,
            mpsc_rx,
            global_event_tx,
            ping_check_tx,
        }
    }
}

#[async_trait]
impl<T> IMuxedConn for MplexConn<T>
where
    T: IRawConnection + Send + Sync,
{
    async fn handle_incoming(&mut self, frames: Vec<u8>) -> Result<bool> {
        // Process frames, to see the following:
        // - create a new muxed-stream as per the headers
        // - see which stream the payload belongs to, as per the headers

        // Extract the stream_id, header adn payload from the received frame
        let (stream_id, flag, _, payload_len) = process_header(&frames)?;
        let (_, remaining) = unsigned_varint::decode::u32(frames.as_slice()).unwrap();
        let (_, payload_after_len) = unsigned_varint::decode::usize(remaining).unwrap();
        let payload_extracted = payload_after_len[..payload_len].to_vec();

        match flag {
            MuxedStreamFlag::NewStream => {
                // Create a new stream
                let (muxed_stream_mpsc_tx, muxed_stream_mpsc_rx) = mpsc::channel::<Vec<u8>>(100);

                let mut is_initiator = false;
                if stream_id == 0 {
                    is_initiator = true;
                }

                match is_initiator {
                    // TODO: Do somthing to merge these 2 match cases
                    false => {
                        self._stream_counter = stream_id;
                        let stream = MplexStream::new(
                            self.mpsc_tx.clone(),
                            muxed_stream_mpsc_rx,
                            self._stream_counter,
                            is_initiator,
                            self.remote_peer_info.clone(),
                            self.handlers.clone(),
                            self.global_event_tx.clone(),
                        );

                        self.streams
                            .insert(self._stream_counter, muxed_stream_mpsc_tx);

                        // SERVER HANDSHAKE PROCEDURE
                        tokio::spawn(async move {
                            stream.handle_conn(None).await.unwrap();
                        });
                    }
                    true => {
                        self._stream_counter += 1;
                        let stream = MplexStream::new(
                            self.mpsc_tx.clone(),
                            muxed_stream_mpsc_rx,
                            self._stream_counter,
                            is_initiator,
                            self.remote_peer_info.clone(),
                            self.handlers.clone(),
                            self.global_event_tx.clone(),
                        );
                        self.streams
                            .insert(self._stream_counter, muxed_stream_mpsc_tx);

                        // INITIATOR-FRAME -> SERVER
                        let initiator_frame = build_frame(
                            self._stream_counter,
                            MuxedStreamFlag::NewStream,
                            b"".as_ref(),
                        );
                        self.send(initiator_frame).await.unwrap();

                        // CLIENT HANDSHAKE PROCEDURE
                        tokio::spawn(async move {
                            stream.handle_conn(Some(payload_extracted)).await.unwrap();
                        });
                    }
                };
            }

            MuxedStreamFlag::Message => match self.streams.get_mut(&stream_id) {
                Some(muxed_stream_mpsc_tx) => {
                    muxed_stream_mpsc_tx.send(payload_extracted).await.unwrap()
                }

                None => {
                    warn!("stream-id: {} - not found", stream_id)
                }
            },

            MuxedStreamFlag::CloseStream => {
                self.streams.remove_entry(&stream_id);

                warn!("stream-id: {} - got closed and removed", stream_id);
            }

            MuxedStreamFlag::HandshakeRes => match self.streams.get_mut(&stream_id) {
                Some(muxed_stream_mpsc_tx) => {
                    muxed_stream_mpsc_tx.send(payload_extracted).await.unwrap()
                }

                None => {
                    warn!("stream-id: {} - not found", stream_id)
                }
            },

            MuxedStreamFlag::HandshakeReq => match self.streams.get_mut(&stream_id) {
                Some(muxed_stream_mpsc_tx) => {
                    muxed_stream_mpsc_tx.send(payload_extracted).await.unwrap()
                }

                None => {
                    warn!("stream-id: {} - not found", stream_id)
                }
            },

            MuxedStreamFlag::Disconnected => {
                // TODO: This case happens only for UDP, but can be done for TCP
                // in future for healthy shudown
                warn!("Peer Disconnected: {}", self.remote_peer_info.peer_id);
                self.raw_conn.close().await.unwrap();
                return Ok(false);
            }

            MuxedStreamFlag::Liveliness => {
                self.ping_check_tx.send(payload_extracted).await.unwrap();
            }
        }

        Ok(true)
    }

    async fn conn_handler(
        &mut self,
        peer_id: &str,
        swarm_mpsc_tx: Arc<dyn ISwarm + Send + Sync>,
    ) -> Result<()> {
        loop {
            tokio::select! {
                Some(data) = self.mpsc_rx.recv() => {
                    if data.starts_with(&INTERNAL) {
                        match self.handle_incoming(data[INTERNAL.len()..].to_vec()).await.unwrap() {
                            true => continue,
                            false => return Ok(()),
                        }
                    }
                    self.write(&data).await.unwrap();
                }

                frame = self.raw_conn.read() => {
                    match frame {
                        Ok(frames) => {
                            self.handle_incoming(frames).await.unwrap();
                        }
                        Err(_) => {break},
                    }
                }
            }
        }

        swarm_mpsc_tx.on_disconnect(peer_id).await.unwrap();

        Ok(())
    }

    async fn send(&self, msg: Vec<u8>) -> Result<()> {
        if let Err(e) = self.mpsc_tx.send(msg).await {
            return Err(Error::msg(format!("mpsc-receiver dropped: {}", e)));
        }

        Ok(())
    }

    async fn write(&mut self, msg: &Vec<u8>) -> Result<()> {
        self.raw_conn.write(msg).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_and_parse_frame() {
        let stream_id = 42u32;
        let flag = MuxedStreamFlag::Message;
        let payload = b"HelloWorld!".to_vec();

        let frame = build_frame(stream_id, flag.clone(), &payload);

        let (parsed_stream_id, parsed_flag, _, payload_len) = process_header(&frame).unwrap();

        assert_eq!(parsed_stream_id, stream_id);
        assert_eq!(parsed_flag.tag(), flag.tag(), "flag mismatch");
        assert_eq!(payload_len, payload.len(), "payload_len mismatch");

        let (_, remaining) = unsigned_varint::decode::u32(frame.as_slice()).unwrap();

        let (decoded_payload_len, payload_after_len) =
            unsigned_varint::decode::usize(remaining).unwrap();

        assert_eq!(decoded_payload_len, payload_len);
        let payload_extracted = payload_after_len[..payload_len].to_vec();

        assert_eq!(payload_extracted, payload, "payload mismatch");
    }
}
