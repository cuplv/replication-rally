mod config;
use config::{
    Cli,
    Config,
    mk_network,
    NetworkMember,
};

use replication_rally::{
    Competitor,
    EOutcome,
    ERally,
    transport,
};

use replication_rally::client::ferry_monitor;

use clap::Parser;

use futures::stream::select_all;
use futures::StreamExt;

use rand::thread_rng;
use rand::seq::SliceRandom;

use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::process::exit;
// use std::thread;
use std::time::Duration;

use tokio::time::{Instant,sleep};
use tokio_stream::wrappers::ReceiverStream;

// The time (ms) between polling attempts when waiting for the new
// leader.
const LEADER_POLL_MS: u64 = 1;


#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    if cli.debug {
        console_subscriber::init();
    }

    let conf = if cli.json {
        Config::json_file(&cli.config)
    } else {
        Config::dhall(&cli.config)
    };

    let conf = match conf {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Config error: {}", e);
            exit(1)
        },
    };

    let conf = match conf {
        // Config::Local(_) => unimplemented!(),
        Config::Network(c) => c,
    };

    for r in conf.schedule.iter() {
        match r.competitor() {
            None => {
                eprintln!("Competitor \"{}\" is unknown.", r.competitor_id);
                exit(1);
            },
            _ => {},
        }
    }

    if cli.check {
        eprintln!("Config is OK.");
        exit(0);
    }

    let output_path = match cli.output.clone() {
        Some(p) => p,
        None => {
            eprintln!("An --output file path is required.");
            exit(1)
        },
    };

    // Multiply the scheduled rallies by the --reps factor, and then
    // shuffle the result.
    let mut rallies = Vec::new();
    for _ in 0 .. cli.reps {
        rallies.append(&mut conf.schedule.clone());
    }
    rallies.shuffle(&mut thread_rng());

    let total = rallies.len();
    let mut failures: u32 = 0;

    for (i, rally) in rallies.iter().enumerate() {
        eprintln!("[{}/{}] {:?}", i + 1, total, rally);

        let result = match rally.competitor() {
            Some(Competitor::Ferry) => {
                single_rally(
                    &rally,
                    &conf.roster,
                    cli.collect_timeout(),
                ).await
            },
            Some(Competitor::Etcd) => {
                single_etcd_rally(
                    &rally,
                    &conf.roster,
                    cli.collect_timeout(),
                    false,
                ).await
            },
            Some(Competitor::EtcdClientRemove) => {
                single_etcd_rally(
                    &rally,
                    &conf.roster,
                    cli.collect_timeout(),
                    true,
                ).await
            },
            _ => unimplemented!(),
        };

        match result {
            Some(outcome) => {
                // println!("{:?}", outcome);
                let mut s = serde_json::to_string(&outcome)
                    .expect("Failed to json-encode ferry config.");
                s.push_str("\n");
                let mut file = File::options()
                    .append(true)
                    .create(true)
                    .open(output_path.clone())
                    .expect(&format!(
                        "Failed to open/create output file {}",
                        output_path.display(),
                    ));
                file.write_all(s.as_bytes())
                    .expect(&format!(
                        "Failed to write to output file {}",
                        output_path.display(),
                    ));
            },
            None => {
                failures += 1;
            },
        }

        // This delay is intended to allow task cleanup to be
        // completed.  All request-tasks have been aborted by this
        // point, and ferry's transport tasks should be terminating as
        // they detect closed channels.
        sleep(Duration::from_secs(5)).await;
    }
    if failures > 0 {
        eprintln!("Some rallies failed: {}/{}", failures, total);
    }
}

async fn single_etcd_rally(
    rally: &ERally,
    roster: &Vec<NetworkMember>,
    _collect_timeout: Duration,
    remove_client: bool,
) -> Option<EOutcome> {
    // Launch nodes.
    let setup = tokio::time::timeout(
        Duration::from_secs(30),
        setup_etcd_network_rally(rally, roster),
    ).await;
    match setup {
        Ok(mut monitor) => {
            // Give the system a second to run
            sleep(Duration::from_millis(10)).await;

            // This gets a list of etcd instance members ...
            // let m_list = monitor.member_list().await.unwrap();
            let mut m_list_o = None;
            while m_list_o.is_none() {
                match monitor.member_list().await {
                    Ok(ms) => { m_list_o = Some(ms); },
                    Err(_) => { sleep(Duration::from_millis(10)).await; },
                }
            }
            let m_list = m_list_o.unwrap();

            // ... and this maps each member's etcd ID to its
            // replication-rally ID.
            let mm = member_map(m_list.members());
        
            // Get initial leader from a status check.
            let mut status_0_o = None;
            while status_0_o.is_none() {
                match monitor.status().await {
                    Ok(v) if v.leader() != 0 => { status_0_o = Some(v); },
                    // If status errors-out or the leader value is 0,
                    // try again.
                    _ => { sleep(Duration::from_millis(10)).await; },
                }
            }
            // status_0_o must be a Some value at this point.
            let status_0 = status_0_o.unwrap();
            // and the status value must have a non-zero leader.
            let leader_0 = status_0.leader();

            // Kill the leader node
            let _ = transport::rpc(
                &roster[mm[&leader_0] as usize].admin_address,
                &transport::NodeRequest::End,
            ).await.expect(&format!("Failed to shut down leader."));

            // Start the clock
            let kill_time = Instant::now();

            // Remove killed instance from client... I think this avoids some
            // failing attempts to connect to that instance when performing
            // the following status checks?
            if remove_client {
                let _ = monitor.remove_endpoint(
                    format!("127.0.0.1:878{}", mm[&leader_0])
                ).await;
            }
        
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
                    Ok(_s) => {
                        // println!("No new leader yet... (initial is {}, current is {})", leader_0, s.leader());
                        tokio::time::sleep(
                            Duration::from_millis(LEADER_POLL_MS)
                        ).await;
                    }
                    // These status checks frequently error out, so we just
                    // ignore those errors.
                    Err(_e) => {
                        // println!("Got an Error: {}", e);
                        tokio::time::sleep(
                            Duration::from_millis(LEADER_POLL_MS)
                        ).await;
                    }
                }
            }
            let time_to_elect = kill_time.elapsed();

            Some(EOutcome{
                rally: rally.clone(),
                duration_ms: time_to_elect.as_secs_f64() * 1000.0,
            })
        },
        Err(_) => {
            eprintln!("Network setup timed out.");
            None
        }
    }
}

fn member_map(members: &[etcd_client::Member]) -> HashMap<u64, u16> {
    let mut map = HashMap::new();
    for mem in members {
        let rr_id = &mem.name()[9..].parse::<u16>().unwrap();
        map.insert(mem.id(), *rr_id);
    }
    map
}

async fn single_rally(
    rally: &ERally,
    roster: &Vec<NetworkMember>,
    collect_timeout: Duration,
) -> Option<EOutcome> {
    // Launch nodes.  This waits until the nodes have returned a
    // "ready" status.
    let setup = tokio::time::timeout(
        Duration::from_secs(30),
        setup_network_rally(rally, roster),
    ).await;
    match setup {
        Ok(monitors) => {
            // Give the system a second to run
            sleep(Duration::from_secs(1)).await;

            // Kill the leader node
            let _ = transport::rpc(
                &roster[0].admin_address,
                &transport::NodeRequest::End,
            ).await.expect(&format!("Failed to shut down leader."));

            // Start the clock
            let start = Instant::now();

            // Start the timeout thread
            let mut timeout_handle = tokio::spawn(async move {
                sleep(collect_timeout).await;
            });

            // Make the stream
            let ss: Vec<_> = monitors.into_iter().map(|(i,m)| {
                ReceiverStream::new(m).map(move |e| (i,e))
            }).collect();
            let mut fused_streams = select_all(ss);

            // Loop on events from the stream, looking for a NewLeader
            // event.
            let mut result = None;
            loop {
                tokio::select! {
                    Some((_i,e)) = fused_streams.next() => {
                        match e {
                            ferry_monitor::Event::NoLeader => {
                                // println!("{} has lost its leader.", i);
                            }
                            ferry_monitor::Event::NewLeader(_l) => {
                                let duration = start.elapsed();
                                // println!("{} recognizes leader {}.", i, l);
                                result = Some(EOutcome{
                                    rally: rally.clone(),
                                    duration_ms: duration.as_secs_f64() * 1000.0,
                                });
                                break;
                            }
                        }
                    }
                    _ = &mut timeout_handle => {
                        eprintln!("Election timed out.");
                        break;
                    }
                }
            }

            shutdown_network_rally(rally.cluster_size, roster).await;

            result
        },
        Err(_) => {
            eprintln!("Network setup timed out.");
            None
        }
    }
}

async fn setup_etcd_network_rally(
    rally: &ERally,
    roster: &Vec<NetworkMember>,
) -> etcd_client::Client {
    if rally.cluster_size as usize > roster.len() {
        eprintln!(
            "Cluster size {} requested, roster only has {} members.",
            rally.cluster_size,
            roster.len(),
        );
        exit(1);
    }

    let network = mk_network(&roster, rally.cluster_size);

    // Command node managers to launch their etcd instances
    for i in 0..(rally.cluster_size) {
        let r = transport::rpc(
            &roster[i as usize].admin_address,
            &transport::NodeRequest::Start(i, rally.competitor().unwrap(), network.clone(), rally.tuning_parameters.clone(), rally.message_latency_ms as u64),
        ).await;
        match r {
            Ok(_) => {},
            Err(e) => {
                eprintln!(
                    "Error performing RPC on {:?}",
                    &roster[i as usize].admin_address,
                );
                eprintln!("{}", e);
                exit(1);
            }
        }
    }
    // Give the nodes time to initialize, else some of our monitor
    // connection attempts will fail.
    sleep(Duration::from_millis(100)).await;

    // This just connects a normal etcd_client::Client to all the
    // nodes.
    connect_etcd_monitor(roster.clone()).await
}

async fn setup_network_rally(
    rally: &ERally,
    roster: &Vec<NetworkMember>,
) -> Vec<(u16,ferry_monitor::Monitor)> {
    if rally.cluster_size as usize > roster.len() {
        eprintln!(
            "Cluster size {} requested, roster only has {} members.",
            rally.cluster_size,
            roster.len(),
        );
        exit(1);
    }

    let network = mk_network(&roster, rally.cluster_size);

    // Command node managers to launch their Ferry nodes
    for i in 0..(rally.cluster_size) {
        let r = transport::rpc(
            &roster[i as usize].admin_address,
            &transport::NodeRequest::Start(i, rally.competitor().unwrap(), network.clone(), rally.tuning_parameters.clone(), rally.message_latency_ms as u64),
        ).await;
        match r {
            Ok(_) => {},
            Err(e) => {
                eprintln!(
                    "Error performing RPC on {:?}",
                    &roster[i as usize].admin_address,
                );
                eprintln!("{}", e);
                exit(1);
            }
        }
    }

    // Give the nodes time to initialize, else some of our monitor
    // connection attempts will fail.
    sleep(Duration::from_millis(100)).await;

    // Connect monitors
    match ferry_monitor::Event::connect_followers(&network, rally.cluster_size).await {
        Ok(ms) => ms,
        Err(e) => {
            eprintln!("Error connecting monitors to followers.");
            eprintln!("{}", e);
            exit(1)
        }
    }
}

async fn shutdown_network_rally(cluster_size: u16, roster: &Vec<NetworkMember>) {
    for i in 0..(cluster_size) {
        let _ = transport::rpc(
            &roster[i as usize].admin_address,
            &transport::NodeRequest::End,
        ).await.expect(&format!("Failed to shut down node {}.", i));
    }
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
