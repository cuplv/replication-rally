mod config;
use config::{
    Cli,
    Config,
    mk_network,
    NetworkMember,
};

use replication_rally::{
    Competitor,
    DClient,
    Outcome,
    Rally,
    transport,
    Tuning,
};

use clap::Parser;

use rand::thread_rng;
use rand::seq::SliceRandom;

use std::process::exit;
use std::time::Duration;
use tokio::time::sleep;

const RETRY_MAX: u32 = 20;
const REQUEST_LOOP_CADENCE: Duration = Duration::from_micros(500);

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

        let result = single_rally(
            &rally,
            &conf.roster,
            cli.collect_timeout(),
            i,
        ).await;

        match result {
            Some(outcome) => {
                // Append the outcome to the output CSV file.
                outcome.csv_write_file(&output_path);
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

async fn single_rally(
    rally: &Rally,
    roster: &Vec<NetworkMember>,
    collect_timeout: Duration,
    rally_num: usize,
) -> Option<Outcome> {
    // Launch nodes.  This waits until the nodes have returned a
    // "ready" status.
    let setup = tokio::time::timeout(
        Duration::from_secs(30),
        setup_network_rally(rally, roster),
    ).await;
    match setup {
        Ok(mut client) => {
            sleep(Duration::from_millis(100)).await;
        
            let result = client.run_rally(
                rally.request_total,
                rally.request_ops_per_s,
                collect_timeout,
                rally_num,
            ).await;
            // Shut down the nodes.
            sleep(Duration::from_millis(100)).await;
            shutdown_network_rally(rally, roster).await;

            match result {
                Ok((records, rally_duration)) => {
                    Some(Outcome::new(rally, records, rally_duration))
                },
                Err(e) => {
                    eprintln!("Rally failed: {}", e);
                    None
                }
            }
        },
        Err(_) => {
            eprintln!("Network setup timed out.");
            None
        }
    }
}

async fn setup_network_rally(
    rally: &Rally,
    roster: &Vec<NetworkMember>,
) -> DClient {
    if rally.cluster_size as usize > roster.len() {
        eprintln!(
            "Cluster size {} requested, roster only has {} members.",
            rally.cluster_size,
            roster.len(),
        );
        exit(1);
    }

    let network = mk_network(&roster, rally.cluster_size);

    let mut ft = Tuning::default();
    // These large timeouts prevent elections from being triggered
    // during the normal-conditions throughput evaluation.
    ft.election_timeout_ms_low = 120000; // 2min
    ft.election_timeout_ms_high = 240000; // 4min

    for i in 0..(rally.cluster_size) {
        let r = transport::rpc(
            &roster[i as usize].admin_address,
            &transport::NodeRequest::Start(
                i,
                rally.competitor().unwrap(),
                network.clone(),
                ft.clone(),
                rally.message_latency_ms as u64,
            ),
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

    // We checked earlier that all competitors are well-defined, so we
    // unwrap here.
    match rally.competitor().expect(&format!("Unknown competitor: {}", rally.competitor_id)) {
        Competitor::Etcd => 
            DClient::connect_etcd(
                &network,
                rally.client_count,
                RETRY_MAX,
                REQUEST_LOOP_CADENCE,
            ).await,

        Competitor::EtcdClientRemove =>
            DClient::connect_etcd(
                &network,
                rally.client_count,
                RETRY_MAX,
                REQUEST_LOOP_CADENCE,
            ).await,

        Competitor::Ferry =>
            DClient::connect_ferry(
                &network,
                rally.client_count,
                RETRY_MAX,
                REQUEST_LOOP_CADENCE,
            ).await,

        Competitor::FerryNoDisk =>
            DClient::connect_ferry(
                &network,
                rally.client_count,
                RETRY_MAX,
                REQUEST_LOOP_CADENCE,
            ).await,
    }
}

async fn shutdown_network_rally(rally: &Rally, roster: &Vec<NetworkMember>) {
    for i in 0..(rally.cluster_size) {
        let _ = transport::rpc(
            &roster[i as usize].admin_address,
            &transport::NodeRequest::End,
        ).await.expect(&format!("Failed to shut down node {}.", i));
    }
}
