use crate::{Address, Network, Tuning};

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::io;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::sleep;

pub async fn test() -> io::Result<()> {
    let mut stream = TcpStream::connect("127.0.0.1:7720").await?;
    // Declare client ID.
    stream.write_u16(8).await?;
    // Send a 1-byte message.
    stream.write_u16(1).await?;
    // Message consists of only a tag (1 = Status)
    stream.write_u8(1).await?;

    // Should receive a 5-byte message in response.
    assert_eq!(5, stream.read_u16().await?);
    // Read the message tag (0 = Completed).
    assert_eq!(0, stream.read_u8().await?);
    // Read the message content.
    println!("Status returned: {}", stream.read_u32().await?);

    Ok(())
}

pub struct Input {
    pub client_request_id: u32,
    pub content: String,
}

async fn connect_server(
    client_id: u16,
    server_addr: &Address,
) -> io::Result<TcpStream> {

    let mut stream = TcpStream::connect(server_addr.render()).await?;
    stream.set_nodelay(true)?;
    stream.write_u16(client_id).await?;
    Ok(stream)
}

async fn recv_server_msg(
    stream: &mut OwnedReadHalf,
) -> io::Result<u32> {
    // Network.Framed frame starts with payload-size, which is always
    // 5 bytes for this application.
    assert_eq!(5, stream.read_u16().await?);
    // 'Completed' message has tag #0.
    assert_eq!(0, stream.read_u8().await?);
    // Read the number of completed requests.
    stream.read_u32().await
}

async fn send_server_msg(
    stream: &mut OwnedWriteHalf,
    input: &Input,
) -> io::Result<()> {
    let content_bytes = input.content.as_bytes();
    let content_size = content_bytes.len();
    stream.write_u16(content_size as u16 + 5).await?;
    stream.write_u8(0).await?;
    stream.write_u32(input.client_request_id).await?;
    stream.write(content_bytes).await?;
    Ok(())
}

async fn recv_loop(mut read_half: OwnedReadHalf, tx: mpsc::Sender<u32>) {
    loop {
        match recv_server_msg(&mut read_half).await {
            Ok(completed) => match tx.send(completed).await {
                Err(e) => {
                    eprintln!("mpsc completed {} failed: {}", completed, e);
                },
                Ok(()) => {},
            },
            Err(e) => {
                // When the socket closes, we'll get UnexpectedEof.
                // This is expected.
                match e.kind() {
                    io::ErrorKind::UnexpectedEof => {},
                    _ => eprintln!("Receive failed: {}", e),
                }
                break;
            }
        }
    }
}

// Returns true if connection failed and should be retried, and false
// if the request channel closed (indicating client shutdown).
async fn send_loop(
    mut write_half: OwnedWriteHalf,
    rx: &mut mpsc::Receiver<Input>,
    deferred: &mut Option<Input>,
    mut recv_handle: tokio::task::JoinHandle<()>,
) -> bool {
    loop {
        if let Some(input) = deferred {
            match send_server_msg(&mut write_half, &input).await {
                Ok(()) => {
                    *deferred = None;
                },
                Err(e) => {
                    eprintln!("Send failed: {}", e);
                    recv_handle.abort();
                    let _ = recv_handle.await;
                    return true
                },
            }
        } else {
            tokio::select! {
                result = rx.recv() => match result {
                    Some(input) => {
                        match send_server_msg(&mut write_half, &input).await {
                            Ok(()) => {
                                // eprintln!("Success for {} at {}", &input.content, Utc::now().format("%H:%M:%S%.3f"));
                            },
                            Err(e) => {
                                eprintln!("Send failed: {}", e);
                                recv_handle.abort();
                                let _ = recv_handle.await;
                                *deferred = Some(input);
                                return true;
                            }
                        }
                    },
                    None => {
                        // eprintln!("Send loop saw closed input channel.");
                        recv_handle.abort();
                        let _ = recv_handle.await;
                        return false;
                    }
                },
                _ = &mut recv_handle => {
                    return true
                },
            }
        }
    }
}

const FAILURE_THRESHOLD: u32 = 20;

pub async fn run_transport(
    client_id: u16,
    a: &Address,
    mut rx: mpsc::Receiver<Input>,
    tx: &mpsc::Sender<u32>,
) {
    let mut deferred_request: Option<Input> = None;
    let mut failure_count: u32 = 0;
    loop {
        if rx.is_closed() {
            // eprintln!("Inputs closed, transport shut down.");
            break;
        }
        match connect_server(client_id, &a).await {
            Ok(stream) => {
                failure_count = 0;
                let (read_half, write_half) = stream.into_split();
        
                let recv_loop_tx = tx.clone();
                let recv_handle = tokio::spawn(async move {
                    recv_loop(read_half, recv_loop_tx).await;
                });
        
                send_loop(
                    write_half,
                    &mut rx,
                    &mut deferred_request,
                    recv_handle
                ).await;
            },
            Err(e) => {
                failure_count += 1;
                if failure_count >= FAILURE_THRESHOLD {
                    eprintln!("Connect failed: {}", e);
                }
                sleep(Duration::from_millis(100)).await;
            },
        };
    }
}

async fn run_sequencer(
    transport_tx: mpsc::Sender<Input>,
    mut transport_rx: mpsc::Receiver<u32>,
    mut request_rx: mpsc::Receiver<(String, oneshot::Sender<()>)>,
) {
    let mut callbacks = VecDeque::with_capacity(4096);
    let mut requested: u32 = 0;
    let mut completed: u32 = 0;
    let mut notified: u32 = 0;
    loop {
        tokio::select! {
            // _ = shutdown_rx => {
            //     transport_handle.abort();
            //     transport_handle.await;
            //     break;
            // }
            result = request_rx.recv() => match result {
                Some((content,tx)) => {
                    // Select unique client-local request ID.
                    let client_request_id = requested;
                    requested += 1;
                    // Forward request to the transport thread.
                    transport_tx.send(Input{
                        client_request_id,
                        content,
                    }).await.expect("Sequencer-to-transport send failed.");
                    // Enqueue callback for later completion.
                    callbacks.push_back(tx);
                },
                None => {
                    // Breaking this loop will drop the
                    // sequencer-to-transport tx, closing that channel
                    // and signaling the transport thread to shut down
                    // as well.
                    break;
                }
            },
            Some(n) = transport_rx.recv() => {
                // Update completed record (might be more than one
                // step at once).
                completed = completed.max(n);

                while notified < completed {
                    let next = callbacks.pop_front()
                        .expect("Sequencer ran out of client callbacks.");
                    next.send(()).unwrap();
                    notified += 1;
                }
            },
        }
    }
}

#[derive(Clone)]
pub struct Client {
    request_tx: mpsc::Sender<(String, oneshot::Sender<()>)>,
}

impl Client {
    pub async fn connect(client_id: u16, server_address: &Address) -> Client {
        let (request_tx, request_rx) = mpsc::channel(4096);
        let (input_tx, input_rx) = mpsc::channel(4096);
        let (status_tx, mut status_rx) = mpsc::channel(4096);

        let my_address = server_address.clone();

        tokio::spawn(async move {
            run_transport(
                client_id,
                &my_address,
                input_rx,
                &status_tx,
            ).await;
        });

        // Receive an initial status message to signal that server is
        // up and connected.
        let _ = status_rx.recv().await.unwrap();

        // Launch the sequencer thread that will receive all further
        // status messages.
        tokio::spawn(async move {
            run_sequencer(
                input_tx,
                status_rx,
                request_rx,
            ).await;
        });

        Client { request_tx }
    }

    pub async fn request(&mut self, content: String) {
        // let a = content.clone();
        let (tx,rx) = oneshot::channel();
        // let request_time = Utc::now();
        // eprintln!("Sending request: {} at {}", &a, request_time.format("%H:%M:%S%.3f"));
        self.request_tx.send((content, tx)).await.unwrap();
        rx.await.unwrap();
        // let response_time = Utc::now();
        // eprintln!("Got response for: {} at {}", &a, response_time.format("%H:%M:%S%.3f"));
    }
}

pub fn node_args(
    node_index: u16,
    data_root: &str,
    network: &Network,
    verbose: bool,
    persist: bool,
    tuning_parameters: Tuning,
) -> Vec<String> {
    let net = net2net(network);
    let ca = get_client_addr(node_index, network);
    let conf = FerryConfig {
        network: net,
        local_id: node_index,
        client_host: ca.host.clone(),
        client_port: format!("{}", ca.port),
        debug_selector: if verbose {
            vec![
                "error".to_string(),
            ]
        } else {
            Vec::new()
        },
        persist_file: if persist {
            format!("{}/{}.ferry", data_root, node_index)
        } else {
            "".to_string()
        },
        election_timeout_ms_low:
        tuning_parameters.election_timeout_ms_low,
        election_timeout_ms_high:
        tuning_parameters.election_timeout_ms_high,
        retransmission_timeout_ms:
        tuning_parameters.retransmission_timeout_ms,
        heartbeat_timeout_ms:
        tuning_parameters.heartbeat_timeout_ms,
    };
    vec![
        serde_json::to_string(&conf).expect("Failed to json-encode ferry config.")
    ]
}

fn get_client_addr(node_index: u16, net: &Network) -> Address {
    match net.get(&node_index) {
        Some(a) => a.client_address.clone(),
        None => panic!("No address for node index {}.", node_index),
    }
}

fn net2net(net: &Network) -> Vec<NetworkMember> {
    let mut v = Vec::new();
    for (i,a) in net.iter() {
        v.push(NetworkMember{
            member_index: *i,
            host: a.peer_address.host.clone(),
            port: format!("{}", a.peer_address.port),
        });
    }
    v
}

#[derive(Eq, PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct NetworkMember {
    member_index: u16,
    host: String,
    port: String,
}

#[derive(Eq, PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct FerryConfig {
    network: Vec<NetworkMember>,
    local_id: u16,
    client_host: String,
    client_port: String,
    debug_selector: Vec<String>,
    persist_file: String,
    election_timeout_ms_low: u32,
    election_timeout_ms_high: u32,
    retransmission_timeout_ms: u32,
    heartbeat_timeout_ms: u32,
}
