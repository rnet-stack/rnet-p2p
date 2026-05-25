use anyhow::Result;
use identity::events::{FloodsubEvent, FloodsubMsgType, GlobalEvent};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::debug;

use crate::pubsub::FloodsubPayload;

#[derive(Debug)]
pub struct SubscriptionAPI {
    pub topic_id: String,
    floodsub_mpsc_tx: Sender<Vec<u8>>,
    topic_mpsc_rx: Receiver<(Vec<u8>, Vec<u8>)>,
    global_event_tx: Sender<Vec<u8>>,
}

impl SubscriptionAPI {
    pub fn new(
        topic_id: String,
        floodsub_mpsc_tx: Sender<Vec<u8>>,
        topic_mpsc_rx: Receiver<(Vec<u8>, Vec<u8>)>,
        global_event_tx: Sender<Vec<u8>>,
    ) -> Self {
        SubscriptionAPI {
            topic_id,
            floodsub_mpsc_tx,
            topic_mpsc_rx,
            global_event_tx,
        }
    }

    pub async fn unsubscribe(&self) -> Result<()> {
        let frame = build_floodsub_api_frame(
            SubAPIMpscFlag::Unsubscribe,
            None,
            Some(vec![self.topic_id.clone()]),
            None,
        );

        self.floodsub_mpsc_tx.send(frame).await.unwrap();
        Ok(())
    }

    pub async fn publish(&self, msg: Vec<u8>) -> Result<()> {
        let frame = build_floodsub_api_frame(
            SubAPIMpscFlag::Publish,
            None,
            Some(vec![self.topic_id.clone()]),
            Some(msg),
        );

        self.floodsub_mpsc_tx.send(frame).await.unwrap();
        Ok(())
    }

    pub async fn recv(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.topic_mpsc_rx.recv().await
    }

    pub async fn receive_loop(&mut self) {
        let topic = self.topic_id.clone();

        // TODO: send global event from here
        loop {
            match self.recv().await {
                Some((payload, source_peer)) => {
                    // let msg = String::from_utf8_lossy(&payload).to_string();
                    let source_peer_id = String::from_utf8_lossy(&source_peer).to_string();
                    // let fsub_msg = format!("[{topic}] - [{source_peer_id}]: {msg}");

                    let floosub_event = GlobalEvent::Floodsub(FloodsubEvent {
                        msg_type: FloodsubMsgType::Publish,
                        source: Some(source_peer_id),
                        msg: Some(payload),
                        topic: topic.clone(),
                    });

                    self.global_event_tx
                        .send(bincode::serialize(&floosub_event).unwrap())
                        .await
                        .unwrap();
                }
                None => {
                    debug!("[{}]: Receiver loop ended", topic);
                    break;
                }
            }
        }
    }
}

pub enum SubAPIMpscFlag {
    Unsubscribe,
    DeadPeers,
    Publish,
}

impl SubAPIMpscFlag {
    pub fn tag(&self) -> u8 {
        match *self {
            Self::Unsubscribe => 1,
            Self::DeadPeers => 2,
            Self::Publish => 3,
        }
    }

    pub fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::Unsubscribe),
            2 => Some(Self::DeadPeers),
            3 => Some(Self::Publish),
            _ => None,
        }
    }
}

pub fn build_floodsub_api_frame(
    flag: SubAPIMpscFlag,
    payload_string: Option<String>,
    opt_vec: Option<Vec<String>>,
    opt_data: Option<Vec<u8>>,
) -> Vec<u8> {
    let mut buf = Vec::new();

    buf.push(flag.tag());

    let binding = payload_string.unwrap_or("none".to_string());
    let s_bytes = binding.as_bytes();

    buf.extend_from_slice(&(s_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(s_bytes);

    // opt_vec
    match opt_vec {
        None => {
            buf.push(0);
        }
        Some(v) => {
            buf.push(1);
            buf.extend_from_slice(&(v.len() as u32).to_le_bytes());

            for item in v {
                let b = item.as_bytes();
                buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                buf.extend_from_slice(b);
            }
        }
    }

    // opt_data
    match opt_data {
        None => {
            buf.push(0);
        }
        Some(data) => {
            buf.push(1);
            buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
            buf.extend_from_slice(&data);
        }
    }

    buf
}

pub fn process_floodsub_api_frame(payload: Vec<u8>) -> Result<(SubAPIMpscFlag, FloodsubPayload)> {
    let mut idx = 0;

    let tag = payload[idx];
    idx += 1;

    let flag = SubAPIMpscFlag::from_tag(tag)
        .ok_or_else(|| "invalid tag".to_string())
        .unwrap();

    // STRING-PAYLOAD
    let s_len = u32::from_le_bytes(payload[idx..idx + 4].try_into().unwrap()) as usize;
    idx += 4;

    let s = String::from_utf8(payload[idx..idx + s_len].to_vec()).unwrap();
    idx += s_len;

    let s_payload = if s != "none" { Some(s) } else { None };

    // OPTIONAL VEC-PAYLOAD
    let has_vec = payload[idx];
    idx += 1;

    let opt_vec = if has_vec == 0 {
        None
    } else {
        let count = u32::from_le_bytes(payload[idx..idx + 4].try_into().unwrap()) as usize;
        idx += 4;

        let mut v = Vec::with_capacity(count);
        for _ in 0..count {
            let len = u32::from_le_bytes(payload[idx..idx + 4].try_into().unwrap()) as usize;
            idx += 4;

            let item = String::from_utf8(payload[idx..idx + len].to_vec()).unwrap();
            idx += len;

            v.push(item);
        }

        Some(v)
    };

    // Optional data payload
    let has_data = payload[idx];
    idx += 1;

    let opt_data = if has_data == 0 {
        None
    } else {
        let data_len = u32::from_le_bytes(payload[idx..idx + 4].try_into().unwrap()) as usize;
        idx += 4;

        let data = payload[idx..idx + data_len].to_vec();
        Some(data)
    };

    Ok((flag, (s_payload, opt_vec, opt_data)))
}
