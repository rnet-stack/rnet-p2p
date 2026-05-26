use std::{sync::Arc, time::Instant};

use anyhow::Result;
use async_trait::async_trait;

use crate::traits::core::ISwarm;

use std::sync::LazyLock;
pub static INSTANT: LazyLock<Instant> = LazyLock::new(Instant::now);

#[async_trait]
pub trait IMuxedConn: Send + Sync {
    async fn handle_incoming(&mut self, frames: Vec<u8>) -> Result<bool>;
    async fn conn_handler(
        &mut self,
        peer_id: &str,
        host_mpsc_tx: Arc<dyn ISwarm + Send + Sync>,
    ) -> Result<()>;
    async fn send(&self, msg: Vec<u8>) -> Result<()>;
    #[allow(clippy::ptr_arg)]
    async fn write(&mut self, msg: &Vec<u8>) -> Result<()>;
}

#[async_trait]
pub trait IMuxedStream {
    #[allow(clippy::ptr_arg)]
    async fn write(&self, msg: &Vec<u8>) -> Result<()>;
    async fn read(&mut self) -> Result<Vec<u8>>;
    async fn close(&self) -> Result<()>;
    async fn negotiate(&mut self, protocol: Option<Vec<u8>>) -> Option<String>;
    async fn handle_conn(mut self, protocol: Option<Vec<u8>>) -> Result<()>;
    fn is_initiator(&self) -> bool;
    fn get_peer_id(&self) -> String;
}
