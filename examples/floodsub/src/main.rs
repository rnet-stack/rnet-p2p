mod cli;

use std::time::Duration;

use anyhow::Result;
use identity::multiaddr::Multiaddr;
use node::{inner::NodeInner, protocol::InnerProtocolOpt};
use tokio::sync::mpsc::Receiver;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::cli::cli_loop;

// #[tokio::main]
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("trace"))
        .without_time()
        .with_target(false)
        .compact()
        .init();

    let mut listen_addr = Multiaddr::new("ip4/127.0.0.1/udp/0").unwrap();
    // let mut listen_addr = Multiaddr::new("ip4/127.0.0.1/tcp/0").unwrap();
    let (host_mpsc_tx, _global_rx) = NodeInner::new(
        &mut listen_addr,
        vec![InnerProtocolOpt::Floodsub, InnerProtocolOpt::Ping],
    )
    .await
    .unwrap();

    tokio::spawn(async move {
        global_notification_receiver(_global_rx).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    info!("Run in new terminal: \ncargo run --bin floodsub --release");
    cli_loop(host_mpsc_tx).await.unwrap();

    Ok(())
}

async fn global_notification_receiver(mut global_event_rx: Receiver<Vec<u8>>) -> Result<()> {
    info!("Global notification receiver initiated");

    loop {
        let notification = global_event_rx.recv().await.unwrap();
        let msg = String::from_utf8_lossy(&notification).to_string();

        info!("{}", msg);
    }
}
