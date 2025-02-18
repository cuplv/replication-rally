use crate::{Address, NodeIndex, Competitor, Network, Tuning};

use serde::{Deserialize, Serialize};

use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use std::io;

const BUF_SIZE: usize = 1024;

pub async fn read_bytes(
    socket: &mut TcpStream
) -> io::Result<Vec<u8>> {
    let mut buf = vec![0; 1024];
    let mut msg_length = socket.read_u16().await?;
    let mut v: Vec<u8> = Vec::with_capacity(msg_length as usize);
    socket.readable().await?;
    while msg_length > 0 {
        let read_max = BUF_SIZE.min(msg_length as usize);
        // eprintln!("Reading at most {} bytes...", read_max);
        let n = socket.read(&mut buf[0..read_max]).await?;
        // eprintln!("Read {} bytes.", n);
        v.extend(&buf[0..n]);
        msg_length -= n as u16;
    }
    Ok(v)
}

pub async fn write_bytes(
    socket: &mut TcpStream, bytes: &Vec<u8>
) -> io::Result<()> {
    socket.writable().await?;
    socket.write_u16(bytes.len() as u16).await?;
    socket.write_all(bytes).await?;
    Ok(())
}

#[derive(Debug, Deserialize, Serialize)]
pub enum NodeRequest {
    Start(NodeIndex, Competitor, Network, Tuning, u64),
    End,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum NodeResponse {
    Ok,
}

pub async fn rpc(
    address: &Address, request: &NodeRequest
) -> io::Result<NodeResponse>
{
    let request_bytes = serde_json::to_vec(request).unwrap();
    let mut stream = TcpStream::connect(
        &format!("{}:{}", address.host, address.port.to_string())
    ).await?;
    write_bytes(&mut stream, &request_bytes).await?;
    let resp_bytes = read_bytes(&mut stream).await?;
    Ok(serde_json::from_slice(&resp_bytes).unwrap())
}
