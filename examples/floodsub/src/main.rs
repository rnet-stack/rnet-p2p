mod cli;

use std::time::Duration;

use anyhow::Result;
use identity::{
    events::{FloodsubMsgType, GlobalEvent},
    multiaddr::Multiaddr,
};
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
        None,
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
        let decoded = bincode::deserialize::<GlobalEvent>(&notification).unwrap();
        match decoded {
            GlobalEvent::Floodsub(event) => {
                let original = event.clone();

                match event.msg_type {
                    FloodsubMsgType::Publish => {
                        let msg = String::from_utf8_lossy(&event.msg.unwrap()).to_string();
                        let topic = event.topic;
                        let source = event.source.unwrap();
                        info!("FloodsubEvent: {topic} - {source}: {msg}");
                    }
                    _ => info!("{:?}", original),
                }
            }
            GlobalEvent::Ping(event) => info!("{:?}", event),
        }
    }
}
