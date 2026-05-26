use anyhow::Result;
use identity::multiaddr::Multiaddr;
use identity::traits::{
    core::INode,
    protocols::{INodeFloodsubAPI, INodePingAPI},
};
use node::node::Node;
use std::collections::HashMap;
use std::{io::Write, sync::Arc, time::Duration};
use tokio::io::{self, AsyncBufReadExt};

const CLI_DELAY: Duration = Duration::from_nanos(1000);
const FLOODSUB: &str = "rnet/floodsub/0.0.1";
const COMMANDS: &[&str] = &[
    "help                       => print all the commands",
    "local                      => get local peer-info",
    "connect <maddr>            => connect with a new peer",
    "ping <maddr> <count>       => exchange ping with a peer",
    "fsub <maddr>               => open a new floodsub stream with the peer",
    "join <topic>               => subscribe to a new-topic",
    "leave <topic>              => unsubscribe to a new-topic",
    "publish <topic> <msg>      => publish a msg to a topic",
    "topics                     => list the subscribed topics",
    "fpeers                     => list the connected Floodsub peers",
    "gpeers                     => list the connected peers",
    "mesh                       => map of topics -> peer",
];

fn print_commands() {
    for cmd in COMMANDS {
        println!("      {}", cmd);
    }
}

async fn handle_cmd(line: &str, host_tx: &Arc<Node>) -> Result<()> {
    let mut parts = line.split_whitespace();
    let cmd = parts.next().unwrap();

    match cmd {
        "help" => {
            print_commands();
        }

        "local" => {
            let peer_info = host_tx.get_local();
            println!("{}", peer_info.listen_addr);
        }

        "connect" => {
            let maddr = Multiaddr::new(parts.next().unwrap()).unwrap();
            host_tx.connect(&maddr).await?;
            tokio::time::sleep(Duration::from_millis(300)).await;
        }

        "ping" => {
            let maddr = parts.next().unwrap();
            let count: u32 = parts.next().unwrap_or("0").parse().unwrap_or(0);
            host_tx.ping(Some(count), maddr).await.unwrap();
        }

        "fsub" => {
            let maddr = parts.next().unwrap();
            host_tx
                .new_stream(maddr, vec![FLOODSUB.to_string()])
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(300)).await;
        }

        "join" => {
            let topic = parts.next().unwrap().to_string();
            host_tx.floodsub_subscribe(topic).await.unwrap();
        }

        "leave" => {
            let topic = parts.next().unwrap().to_string();
            host_tx.floodsub_unsubscribe(vec![topic]).await.unwrap();
        }

        "publish" => {
            let topic = parts.next().unwrap().to_string();
            let msg = parts.collect::<Vec<_>>().join(" ").as_bytes().to_vec();

            host_tx.floodsub_publish(topic, msg).await.unwrap();
        }

        "topics" => {
            let topics = host_tx.floodsub_topics().await.unwrap_or(vec![]);
            println!("{:?}", topics);
        }

        "fpeers" => {
            let fpeers = host_tx.floodsub_peers().await.unwrap_or(vec![]);
            fpeers.iter().for_each(|x| {
                println!("{}", x);
            });
        }

        "gpeers" => {
            let gpeers = host_tx.get_peers().await;
            gpeers.iter().for_each(|peer| println!("{}", peer));
        }

        "mesh" => {
            let mesh = host_tx.floodsub_mesh().await.unwrap_or(HashMap::new());
            mesh.iter().for_each(|(topic, peers)| {
                println!("- {}", topic);
                peers.iter().for_each(|peer| println!("  - {}", peer));
            });
        }

        _ => println!("Unknown command"),
    }
    Ok(())
}

pub async fn cli_loop(host_tx: Arc<Node>) -> Result<()> {
    let stdin = io::BufReader::new(io::stdin());
    let mut lines = stdin.lines();

    println!("\nFLOODSUB CLI ready. Commands:");
    print_commands();

    loop {
        print!("\nCommand => ");
        std::io::stdout().flush().unwrap();

        let line = match lines.next_line().await? {
            Some(line) => line,
            None => break,
        };

        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        handle_cmd(line, &host_tx).await?;
        tokio::time::sleep(CLI_DELAY).await;
    }

    Ok(())
}
