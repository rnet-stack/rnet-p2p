use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub trait INodeFloodsubAPI {
    async fn floodsub_is_running(&self) -> bool;
    async fn floodsub_subscribe(&self, topic: String) -> Result<()>;
    async fn floodsub_unsubscribe(&self, topics: Vec<String>) -> Result<()>;
    async fn floodsub_publish(&self, topic: String, msg: Vec<u8>) -> Result<()>;
    async fn floodsub_topics(&self) -> Option<Vec<String>>;
    async fn floodsub_peers(&self) -> Option<Vec<String>>;
    async fn floodsub_mesh(&self) -> Option<HashMap<String, Vec<String>>>;
}

#[async_trait]
pub trait INodePingAPI {
    async fn ping_is_running(&self) -> bool;
    async fn ping(&self, count: Option<u32>, maddr: &str) -> Result<()>;
}
