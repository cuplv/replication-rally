use replication_rally::{
    Address,
    Competitor,
    Network,
    NodeIndex,
    NodeInterface,
    transport,
    Tuning,
};

use std::collections::HashMap;
use std::time::{Duration, Instant};

// The time (ms) between polling attempts when waiting for the new
// leader.
const LEADER_POLL_MS: u64 = 1;

#[tokio::main]
async fn main() {
    let network = test_network();

    instantiate_nodes(&network).await;
    println!("Nodes have been instantiated.");

    // This just connects a normal etcd_client::Client to all the
    // nodes.
    let mut monitor = connect_etcd_monitor(test_network()).await;
    println!("Monitor is connected.");

    // This gets a list of etcd instance members ...
    let m_list = monitor.member_list().await.unwrap();
    // ... and this maps each member's etcd ID to its
    // replication-rally ID.
    let mm = member_map(m_list.members());

    // Get initial leader from a status check.
    let status_0 = monitor.status().await.unwrap();
    let leader_0 = status_0.leader();
    println!(
        "Leader is {:?}, aka Node #{}.",
        leader_0,
        mm[&leader_0],
    );

    // Send kill command for the leader node, record the time.
    kill_node(&network, mm[&leader_0]).await;
    let kill_time = Instant::now();

    // Remove killed instance from client... I think this avoids some
    // failing attempts to connect to that instance when performing
    // the following status checks?
    monitor.remove_endpoint(
        format!("127.0.0.1:878{}", mm[&leader_0])
    ).await.unwrap();

    // Repeatedly check cluster status, waiting for new leader.  It
    // seems that the leader ID "0" is used while there is no leader.
    let mut new_leader: Option<u16> = None;
    while new_leader == None {
        match monitor.status().await {
            // We have a new leader when the leader value is not 0
            // (i.e. null) and also not equal to the old leader's ID,
            // since the old leader is dead and cannot win the new
            // election.
            Ok(s) if s.leader() != leader_0 && s.leader() != 0 => {
                new_leader = Some(
                    *mm.get(&s.leader()).expect(
                        // This is how I discovered that 0 is used for
                        // the null leader.
                        &format!(
                            "Key {} is unknown in mm {:?}.",
                            s.leader(),
                            mm
                        )
                    )
                );
            },
            Ok(_) => {
                tokio::time::sleep(
                    Duration::from_millis(LEADER_POLL_MS)
                ).await;
            }
            // These status checks frequently error out, so we just
            // ignore those errors.
            Err(_) => {
                tokio::time::sleep(
                    Duration::from_millis(LEADER_POLL_MS)
                ).await;
            }
        }
    }

    let time_to_put = kill_time.elapsed();
    println!(
        "After change, leader is node #{}.",
        new_leader.unwrap(),
    );
    println!(
        "The post-failure leader change took {:.6} seconds to complete",
        time_to_put.as_secs_f64(),
    );
}

fn member_map(members: &[etcd_client::Member]) -> HashMap<u64, u16> {
    let mut map = HashMap::new();
    for mem in members {
        let rr_id = &mem.name()[9..].parse::<u16>().unwrap();
        map.insert(mem.id(), *rr_id);
    }
    map
}

async fn kill_node(network: &Vec<NetworkMember>, node: u16) {
    let addr = &network[node as usize].admin_address;
    transport::rpc(
        addr,
        &transport::NodeRequest::End,
    ).await.unwrap();
}

async fn instantiate_nodes(network: &Vec<NetworkMember>) {
    let cluster_size = network.len();
    for addr in network {
        transport::rpc(
            &addr.admin_address,
            &transport::NodeRequest::End,
        ).await.unwrap();
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    for (node_id, addr) in network.iter().enumerate() {
        transport::rpc(
            &addr.admin_address,
            &transport::NodeRequest::Start(
                node_id as u16,
                Competitor::Etcd,
                mk_network(&network, cluster_size as u16),
                Tuning {
                    election_timeout_ms_low: 1000,
                    election_timeout_ms_high: 1000,
                    retransmission_timeout_ms: 200,
                    heartbeat_timeout_ms: 100,
                },
                0,
            ),
        ).await.unwrap();
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
}
        

async fn connect_etcd_monitor(
    network: Vec<NetworkMember>
) -> etcd_client::Client {
    let addrs: Vec<String> =
        network.iter().map(|m| m.client_address.render()).collect();
    let mut client = etcd_client::Client::connect(addrs, None).await
        .expect("Failed to connect etcd client.");
    let mut ready = false;
    while !ready {
        match client.status().await {
            Ok(_) => {
                ready = true;
            },
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
    client
}

#[derive(Clone)]
pub struct NetworkMember {
    pub admin_address: Address,
    pub client_address: Address,
    pub peer_address: Address,
}

pub fn mk_network(
    roster: &Vec<NetworkMember>,
    cluster_size: NodeIndex,
) -> Network {
    let mut network: Network =
        HashMap::with_capacity(cluster_size as usize);
    for index in 0..(cluster_size) {
        let member = roster[index as usize].clone();
        network.insert(
            index,
            NodeInterface{
                client_address: member.client_address,
                peer_address: member.peer_address,
            }
        );
    }
    network
}

fn test_network() -> Vec<NetworkMember> {
    vec![
        NetworkMember {
            admin_address: Address { host: "127.0.0.1".to_string(), port: 8780 },
            client_address: Address { host: "127.0.0.1".to_string(), port: 8880 },
            peer_address: Address { host: "127.0.0.1".to_string(), port: 8980 },
        },
        NetworkMember {
            admin_address: Address { host: "127.0.0.1".to_string(), port: 8781 },
            client_address: Address { host: "127.0.0.1".to_string(), port: 8881 },
            peer_address: Address { host: "127.0.0.1".to_string(), port: 8981 },
        },
        NetworkMember {
            admin_address: Address { host: "127.0.0.1".to_string(), port: 8782 },
            client_address: Address { host: "127.0.0.1".to_string(), port: 8882 },
            peer_address: Address { host: "127.0.0.1".to_string(), port: 8982 },
        },
        NetworkMember {
            admin_address: Address { host: "127.0.0.1".to_string(), port: 8783 },
            client_address: Address { host: "127.0.0.1".to_string(), port: 8883 },
            peer_address: Address { host: "127.0.0.1".to_string(), port: 8983 },
        },
        NetworkMember {
            admin_address: Address { host: "127.0.0.1".to_string(), port: 8784 },
            client_address: Address { host: "127.0.0.1".to_string(), port: 8884 },
            peer_address: Address { host: "127.0.0.1".to_string(), port: 8984 },
        },
    ]
}
