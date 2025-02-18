use crate::Network;

use std::path::PathBuf;

const LOCAL_HOST: &str = "127.0.0.1";

const HTTP_LOCAL: &str = "http://127.0.0.1";


pub fn cluster_addrs(c_size: u16) -> Vec<String> {
    let mut addrs = Vec::new();
    for j in 0..c_size {
        addrs.push(format!(
            "{}={}:{}",
            node_name(j),
            HTTP_LOCAL,
            peer_port(j),
        ))
    }
    addrs
}

fn cluster_addrs2(network: &Network) -> String {
    let mut addrs = Vec::new();
    for (i,v) in network {
        addrs.push(format!("{}={}", node_name(*i), v.peer_address.http()));
    }
    addrs.join(",")
}

pub fn client_addrs(c_size: u16) -> Vec<String> {
    let mut addrs = Vec::new();
    for j in 0..c_size {
        addrs.push(format!(
            "{}:{}",
            LOCAL_HOST,
            client_port(j),
        ))
    }
    addrs
}

pub fn node_args(cluster_size: u16, data_root: &str, i: u16) -> Vec<String> {
    let cluster_s = cluster_addrs(cluster_size).join(",");

    vec![
        "--name".to_string(),
        node_name(i),
        "--data-dir".to_string(),
        node_data_dir(data_root, i).into_os_string().into_string().unwrap(),
        "--listen-client-urls".to_string(),
        format!("{}:{}", HTTP_LOCAL, client_port(i)),
        "--advertise-client-urls".to_string(),
        format!("{}:{}", HTTP_LOCAL, client_port(i)),
        "--listen-peer-urls".to_string(),
        format!("{}:{}", HTTP_LOCAL, peer_port(i)),
        "--initial-advertise-peer-urls".to_string(),
        format!("{}:{}", HTTP_LOCAL, peer_port(i)),
        "--initial-cluster-token".to_string(),
        "etcd-cluster-1".to_string(),
        "--initial-cluster".to_string(),
        cluster_s,
        "--initial-cluster-state".to_string(),
        "new".to_string(),        
    ]
}

pub fn node_args2(
    index: u16,
    data_root: &str,
    network: &Network,
    tuning: Option<(u32,u32)>,
) -> Vec<String> {
    let peer_address =
        network.get(&index).unwrap().peer_address.http();
    let client_address =
        network.get(&index).unwrap().client_address.http();
    let cluster_s = cluster_addrs2(network);

    let mut args = vec![
        "--name".to_string(),
        node_name(index),
        "--data-dir".to_string(),
        node_data_dir(data_root, index).into_os_string().into_string().unwrap(),
        "--listen-client-urls".to_string(),
        client_address.clone(),
        "--advertise-client-urls".to_string(),
        client_address.clone(),
        "--listen-peer-urls".to_string(),
        peer_address.clone(),
        "--initial-advertise-peer-urls".to_string(),
        peer_address.clone(),
        "--initial-cluster-token".to_string(),
        "etcd-cluster-1".to_string(),
        "--initial-cluster".to_string(),
        cluster_s,
        "--initial-cluster-state".to_string(),
        "new".to_string(),
        "--log-level".to_string(),
        "warn".to_string(),
    ];
    match tuning {
        Some((heartbeat_ms,election_ms)) => {
            args.push(format!("--heartbeat-interval={}", heartbeat_ms));
            // etcd only allows election timeouts to be configured
            // within a certain range.
            args.push(format!(
                "--election-timeout={}",
                election_ms.min(45000).max(25),
            ));
        },
        None => {},
    }
    args
}

pub fn node_data_dir(data_root: &str, i: u16) -> PathBuf {
    [data_root, &format!("{}.etcd", node_name(i))].iter().collect()
}

fn node_name(node_index: u16) -> String {
    format!("rrx_node_{}", node_index)
}

fn client_port(node_index: u16) -> u16 {
    33060 + node_index
}

fn peer_port(node_index: u16) -> u16 {
    34060 + node_index
}
