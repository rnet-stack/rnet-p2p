use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FloodsubEvent {
    pub msg_type: FloodsubMsgType,
    pub source: Option<String>,
    pub msg: Option<Vec<u8>>,
    pub topic: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum FloodsubMsgType {
    Publish,
    Subscribe,
    Unsubscribe,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PingEvent {
    pub remote: String,
    pub rtts: Vec<u128>,
    pub count: u128,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum GlobalEvent {
    Floodsub(FloodsubEvent),
    Ping(PingEvent),
}
