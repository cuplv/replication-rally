use replication_rally::{
    Address,
    Network,
    NodeIndex,
    NodeInterface,
    ERally,
};
use clap::Parser;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::process;
use std::time::Duration;

#[derive(Parser)]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[arg(short, long, value_name = "FILE", help = "file to append results")]
    pub output: Option<PathBuf>,

    #[arg(short, long, value_name = "FILE", help = "config file")]
    pub config: String,

    #[arg(long, help = "use a JSON config, rather than default Dhall")]
    pub json: bool,

    #[arg(short, long, value_name = "NUM", help = "times to repeat each rally", default_value_t = 3)]
    pub reps: u32,

    #[arg(long, help = "check the config file and exit")]
    pub check: bool,

    #[arg(long, help = "enable tokio console debugging")]
    pub debug: bool,

    #[arg(long, value_name = "SECS", help = "time to wait for lagging results", default_value_t = 60)]
    pub collect_seconds: u64,
}

impl Cli {
    pub fn collect_timeout(&self) -> Duration {
        Duration::from_secs(self.collect_seconds)
    }
}

#[derive(Clone,Deserialize)]
pub struct NetworkMember {
    pub admin_address: Address,
    pub client_address: Address,
    pub peer_address: Address,
}

pub fn mk_network(roster: &Vec<NetworkMember>, cluster_size: NodeIndex) -> Network {
    let mut network: Network = HashMap::with_capacity(cluster_size as usize);
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

#[derive(Clone,Deserialize)]
pub struct NetworkConfig {
    pub roster: Vec<NetworkMember>,
    pub schedule: Vec<ERally>,
}

// #[derive(Clone,Deserialize)]
// pub struct LocalConfig {
//     pub roster: Vec<NodeResource>,
//     pub schedule: Vec<Rally>,
// }

#[derive(Clone,Deserialize)]
pub enum Config {
    // Local(LocalConfig),
    Network(NetworkConfig),
}

impl Config {
    /// Read config from a Dhall string.  Use the 'local' boolean
    /// argument when it's a local config.
    pub fn dhall(config: &str) -> serde_json::Result<Config> {
        let mut c = process::Command::new("dhall-to-json")
            .stdin(process::Stdio::piped())
            .stdout(process::Stdio::piped())
            .spawn()
            .expect("Failed to spawn");
        let mut stdin = c.stdin.take()
            .expect("Failed to open dhall-to-json stdin");
        stdin.write_all(config.as_bytes())
            .expect("Failed to write to dhall-to-json stdin");
        drop(stdin);
        let output = c.wait_with_output()
            .expect("Failed to read dhall-to-json stdout");

        Ok(Config::Network(serde_json::from_slice(&output.stdout)?))
    }
    
    /// Read config from a JSON file.  Use the 'local' boolean
    /// argument when it's a local config.
    pub fn json_file(path: &str) -> serde_json::Result<Config> {
        let conf_file = match fs::File::open(path) {
            Ok(r) => r,
            Err(e) => {
                eprintln!(
                    "Could not open specified config file: {:?}",
                    path
                );
                eprintln!("{}", e);
                process::exit(1)
            },
        };

        Ok(Config::Network(serde_json::from_reader(conf_file)?))
    }
}
