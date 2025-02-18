use replication_rally::{
    Address,
    Network,
    NodeIndex,
    NodeInterface,
    ferry,
    Tuning,
};

use std::collections::HashMap;
use std::io;
use std::process::{Child,Command,Stdio};
use std::time::{Duration, Instant};
use std::thread;
use tokio::sync::mpsc;

const REQUEST_NUM: u32 = 100000;

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut c = launch_ferry(0, &test_network(), "zxcd", false).unwrap();

    // thread::sleep(Duration::from_secs(1));

    // let (input_tx,input_rx) = tokio::sync::mpsc::channel(1024);
    // let (status_tx,mut status_rx) = tokio::sync::mpsc::channel(1024);
    // tokio::spawn(async move {
    //     ferry::run_transport(
    //         8,
    //         &Address { host: "127.0.0.1".to_string(), port: 7720 },
    //         input_rx,
    //         &status_tx,
    //     ).await;
    // });

    let client = ferry::Client::connect(
        8,
        &Address { host: "127.0.0.1".to_string(), port: 7720 },
    ).await;

    let (tx, mut rx) = mpsc::channel(REQUEST_NUM as usize);

    let start = Instant::now();

    for _ in 0 .. REQUEST_NUM {
        let mut client1 = client.clone();
        let tx1 = tx.clone();

        tokio::spawn(async move {
            client1.request("Hello World!".to_string()).await;
            tx1.send(()).await.unwrap();
        });
    }

    drop(tx);

    for _ in 0 .. REQUEST_NUM {
        rx.recv().await.unwrap()
    }

    let elapsed = start.elapsed();

    if rx.is_closed() {
        eprintln!("Finished in {}ms.", elapsed.as_millis());
    } else {
        eprintln!("rx did not close after all responses were received.");
    }

        // match input_tx.send(ferry::Input{
        //     client_request_id: i,
        //     content: "Hello, world!".to_string(),
        // }).await {
        //     Ok(()) => {},
        //     Err(e) => {
        //         eprintln!("Error sending request {}: {}", i, e);
        //     }
        // }

    // input_tx.send(ferry::Input{
    //     client_request_id: 98,
    //     content: "Hello, world!".to_string(),
    // }).await.unwrap();

    // input_tx.send(ferry::Input{
    //     client_request_id: 108,
    //     content: "Hello, moon!".to_string(),
    // }).await.unwrap();
    
    // let mut completed = 0;
    // while completed < REQUEST_NUM {
    //     completed = completed.max(status_rx.recv().await.expect("Completed channel closed."));
    //     // println!("Got status that {:?} is completed.", completed);
    // }

    // println!("Finished after {}ms", start.elapsed().as_millis());
    // // ferry::test().await?;

    drop(client);

    c.kill().unwrap();

    thread::sleep(Duration::from_secs(1));

    Ok(())
}

fn test_network() -> Network {
    HashMap::from([
        (0, NodeInterface {
            client_address: Address { host: "127.0.0.1".to_string(), port: 7720 },
            peer_address: Address { host: "127.0.0.1".to_string(), port: 7920 },
        }),
        (1, NodeInterface {
            client_address: Address { host: "127.0.0.1".to_string(), port: 7721 },
            peer_address: Address { host: "127.0.0.1".to_string(), port: 7921 },
        }),
        (2, NodeInterface {
            client_address: Address { host: "127.0.0.1".to_string(), port: 7722 },
            peer_address: Address { host: "127.0.0.1".to_string(), port: 7922 },
        }),
    ])
}

fn launch_ferry(
    node_index: NodeIndex,
    network: &Network,
    data_root: &str,
    verbose: bool,
) -> io::Result<Child> {
    let stderr =
        if verbose {
            Stdio::inherit()
        } else {
            Stdio::null()
        };
    Command::new("ferry")
        .args(ferry::node_args(node_index, data_root, network, verbose, false, Tuning::default()))
        .stderr(stderr)
        .spawn()
}
