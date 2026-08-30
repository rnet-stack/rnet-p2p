mod cli;

use std::{
    env::args,
    io::{self, BufRead, Write},
    time::Duration,
};

use anyhow::Result;
use identity::{
    events::{FloodsubMsgType, GlobalEvent},
    multiaddr::Multiaddr,
};
use node::{inner::NodeInner, protocol::InnerProtocolOpt};
use tokio::sync::mpsc::Receiver;
use tracing::{info, warn};
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

    let args = args().skip(1).collect::<Vec<_>>();
    let flags = match args.iter().any(|a| a == "-i" || a == "--interactive") {
        true => prompt_flags(),
        false => parse_flags(args),
    };

    info!(
        "Config: rlnc: {} ping-check: {} udp: {}",
        flags.enable_rlnc, flags.ping_check, flags.enable_udp
    );

    let mut listen_addr = match flags.enable_udp {
        true => Multiaddr::new("ip4/127.0.0.1/udp/0").unwrap(),
        false => Multiaddr::new("ip4/127.0.0.1/tcp/0").unwrap(),
    };

    let (host_mpsc_tx, _global_rx) = NodeInner::new(
        &mut listen_addr,
        vec![
            InnerProtocolOpt::Floodsub,
            InnerProtocolOpt::Ping {
                enable_rlnc: flags.enable_rlnc,
            },
        ],
        None,
        flags.ping_check,
    )
    .await
    .unwrap();

    tokio::spawn(async move {
        global_notification_receiver(_global_rx).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    info!("Run in new terminal: \ncargo run --bin floodsub --release (add `-- -i` to pick flags interactively)");
    cli_loop(host_mpsc_tx).await.unwrap();

    Ok(())
}

#[derive(Debug, Default, Clone, Copy)]
struct Flags {
    enable_rlnc: bool,
    ping_check: bool,
    enable_udp: bool,
}

fn parse_flags(args: Vec<String>) -> Flags {
    let mut flags = Flags::default();

    for arg in args {
        match arg.as_str() {
            "--rlnc" => flags.enable_rlnc = true,
            "--udp" => flags.enable_udp = true,
            "--ping-check" => flags.ping_check = true,
            other => warn!("Unknown flag ignored: {other}"),
        }
    }

    // liveliness check only runs over udp transport
    if flags.ping_check && !flags.enable_udp {
        warn!("--ping-check requires --udp, ignoring");
        flags.ping_check = false;
    }

    flags
}

/// Interactive mode (`-- -i`): asks for each flag one by one, Enter means yes.
fn prompt_flags() -> Flags {
    let enable_rlnc = prompt_bool("Sharding RLNC", true);
    let enable_udp = prompt_bool("Transport UDP", true);
    // liveliness check only runs over udp transport
    let ping_check = enable_udp && prompt_bool("UDP liveliness check", true);

    Flags {
        enable_rlnc,
        ping_check,
        enable_udp,
    }
}

fn prompt_bool(name: &str, default: bool) -> bool {
    let hint = if default { "Y/n" } else { "y/N" };
    print!("{name} ({hint}): ");
    io::stdout().flush().ok();

    let mut line = String::new();
    io::stdin().lock().read_line(&mut line).ok();

    match line.trim().to_ascii_lowercase().as_str() {
        "" => default,
        "y" | "yes" => true,
        "n" | "no" => false,
        other => {
            warn!("Unrecognized input '{other}', using default");
            default
        }
    }
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
