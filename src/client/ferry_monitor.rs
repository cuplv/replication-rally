use crate::{Address,Network};

use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

pub type Monitor = mpsc::Receiver<Event>;

pub enum Event {
    NoLeader,
    NewLeader(u32),
}

impl Event {
    pub async fn connect_followers(network: &Network, cluster_size: u16) -> io::Result<Vec<(u16, Monitor)>> {
        let mut ms = Vec::new();
        for i in 1 .. cluster_size {
            ms.push((i, Self::connect(&network[&i].client_address).await?));
        }
        Ok(ms)
    }
    pub async fn connect(server_address: &Address) -> io::Result<mpsc::Receiver<Self>> {
        match TcpStream::connect(server_address.render()).await {
            Ok(mut stream) => {
                stream.set_nodelay(true)?;
                // Only one client per node, so we just use 0.
                stream.write_u16(0).await?;
        
                let (event_tx,event_rx) = mpsc::channel(1024);
                tokio::spawn(async move {
                    recv_loop(stream, event_tx).await
                });
                Ok(event_rx)
            }
            Err(e) => {
                println!("Error connecting to {}", server_address.render());
                Err(e)
            }
        }
    }
}

async fn recv_loop(mut stream: TcpStream, event_tx: mpsc::Sender<Event>) {
    loop {
        match recv_server_msg(&mut stream).await {
            // If we get an event, send it on the channel
            Ok(ev) => match event_tx.send(ev).await {
                // If we fail to send it on the channel, it must be
                // closed, so we're done.
                Err(_) => break,
                _ => {},
            // If we fail to get an event, the connection must be
            // closed, so we're done.
            }
            Err(_) => break,
        }
    }
}

async fn recv_server_msg(
    stream: &mut TcpStream,
) -> io::Result<Event> {
    // Network.Framed frame starts with payload-size, which can be
    // either 1 or 5 bytes for this application.
    stream.read_u16().await?;

    match stream.read_u8().await? {
        1 => Ok(Event::NoLeader),
        // A new-leader announcement
        2 => {
            let n = stream.read_u32().await?;
            Ok(Event::NewLeader(n))
        }
        n => panic!("Monitor can't handle response type {}.", n)
    }
}
