use replication_rally::{
    Competitor,
    Network,
    NodeIndex,
    transport,
    transport::NodeRequest,
    etcd,
    ferry,
    Tuning,
};

use clap::Parser;
use tokio::net::{TcpListener, TcpStream};

use std::fs;
use std::io;
use std::process::{Child,Command,exit,Stdio};

#[derive(Parser)]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[arg(short, long, value_name = "IP:PORT", help = "address to listen for commands on")]
    pub address: Option<String>,
    #[arg(short, long, value_name = "PATH", help = "directory for node data")]
    pub data_root: Option<String>,
    #[arg(short, long)]
    pub verbose: bool,
}

impl Cli {
    fn process(&self) -> Opts {
        let address = match self.address {
            Some(ref a) => a.clone(),
            None => match std::env::var("RALLY_NODE_ADDRESS") {
                Ok(a) => a,
                Err(std::env::VarError::NotPresent) => {
                    eprintln!("Error: no address provided");
                    eprintln!("Use arguments \"--address IP:PORT\" or variable \"RALLY_NODE_ADDRESS=IP:PORT\"");
                    exit(1)
                },
                Err(e) => panic!("Error reading environment vars: {}", e),
            },
        };

        let data_root = match self.data_root {
            Some(ref a) => a.clone(),
            None => match std::env::var("RALLY_NODE_DATA_ROOT") {
                Ok(a) => a,
                Err(std::env::VarError::NotPresent) => "rally_node_data_root".to_string(),
                Err(e) => panic!("Error reading environment vars: {}", e),
            },
        };
        Opts { address, data_root, verbose: self.verbose }
    }
}

struct Opts {
    pub address: String,
    pub data_root: String,
    pub verbose: bool,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let cli: Opts = Cli::parse().process();

    let interface = std::env::var_os("RALLY_DELAY_INTERFACE");

    let mut proc: Option<Child> = None;
    let mut delay: Option<u64> = None;

    let listener = TcpListener::bind(&cli.address).await?;
    if cli.verbose {
        eprintln!("Waiting for commands on {} ...", &cli.address);
    }
    loop {
        let (mut socket, _) = listener.accept().await?;

        let bytes = match transport::read_bytes(&mut socket).await {
            Ok(bytes) => bytes,
            Err(e) => {
                eprintln!(
                    "Error receiving node command from director: {}",
                    e,
                );
                exit(1)
            }
        };
        let command: serde_json::Result<transport::NodeRequest> =
            serde_json::from_slice(&bytes);

        match command {
            Ok(NodeRequest::Start(node_index,competitor,network,ferry_params,delay_ms)) => {
                if kill_node_if_running(&mut proc) {
                    eprintln!("Shut down lingering node.");
                }

                if delay.is_some() {
                    uninstall_delay(&interface.clone().unwrap().to_str().unwrap());
                    delay = None;
                    eprintln!("Uninstalled lingering delay.");
                }

                prep_data_dir(node_index, &cli.data_root)?;
                if cli.verbose {
                    eprintln!("Prepped data root: {}", &cli.data_root);
                }

                if delay_ms > 0 {
                    install_delay(&interface.clone().expect("Requested delay with no interface configured.").to_str().unwrap(), delay_ms);
                    delay = Some(delay_ms);
                }

                let c = match competitor {
                    Competitor::Etcd => launch_etcd(node_index, &network, &cli.data_root, cli.verbose, ferry_params)?,
                    Competitor::EtcdClientRemove => launch_etcd(node_index, &network, &cli.data_root, cli.verbose, ferry_params)?,
                    Competitor::Ferry => launch_ferry(node_index, &network, &cli.data_root, cli.verbose, true, ferry_params)?,
                    Competitor::FerryNoDisk => launch_ferry(node_index, &network, &cli.data_root, cli.verbose, false, ferry_params)?,
                };
                proc = Some(c);
                if cli.verbose {
                    eprintln!(
                        "Launched node: index {}"
                            , node_index
                    );
                }
                say_ok(&mut socket).await?;
            },
            Ok(NodeRequest::End) => {
                if kill_node_if_running(&mut proc) {
                    if cli.verbose {
                        eprintln!("Shut down node.");
                    }
                } else {
                    eprintln!("Received shutdown request, but not running.");
                }
                if delay.is_some() {
                    uninstall_delay(&interface.clone().unwrap().to_str().unwrap());
                    delay = None;
                }
                say_ok(&mut socket).await?;
            },
            Err(e) => {
                eprintln!("Error decoding NodeRequest.");
                eprintln!("{}", e);
            },
        }

        match say_ok(&mut socket).await {
            Ok(()) => {},
            Err(e) => {
                eprintln!("Failed to say_ok: {}", e);
            }
        }
    }
}

fn install_delay(interface: &str, delay_ms: u64) {
    let s = Command::new("tc")
        .args(["qdisc", "add", "dev", interface, "root", "netem", "delay", &format!("{}ms", delay_ms)])
        .status();
    match s {
        Ok(es) if es.success() => {},
        Ok(es) => panic!(
            "Failed to install delay, got exit code: {:?}",
            es.code(),
        ),
        Err(e) => panic!("Failed to install delay, got error: {}", e),
    }
}

fn uninstall_delay(interface: &str) {
    let s = Command::new("tc")
        .args(["qdisc", "del", "dev", interface, "root"])
        .status();
    match s {
        Ok(es) if es.success() => {},
        Ok(es) => panic!(
            "Failed to uninstall delay, got exit code: {:?}",
            es.code(),
        ),
        Err(e) => panic!("Failed to uninstall delay, got error: {}", e),
    }
}

fn kill_node_if_running(child: &mut Option<Child>) -> bool {
    match child {
        Some(c) => {
            c.kill().unwrap();
            c.wait().unwrap();
            *child = None;
            true
        },
        None => false,
    }
}

async fn say_ok(socket: &mut TcpStream) -> io::Result<()> {
    let resp_bytes =
        serde_json::to_vec(&transport::NodeResponse::Ok).unwrap();
    transport::write_bytes(socket, &resp_bytes).await?;

    Ok(())
}

fn prep_data_dir(index: u16, data_root: &str) -> io::Result<()> {
    let data_dir = etcd::node_data_dir(data_root, index);
    match fs::remove_dir_all(data_dir) {
        Ok(_) => {},
        Err(e) => match e.kind() {
            io::ErrorKind::NotFound => {},
            _ => {
                eprintln!(
                    "Failed to clear existing data_root: {}",
                    data_root
                );
                eprintln!("{:?}", e);
                exit(1);
            }
        }
    }
    fs::create_dir_all(data_root)?;
    Ok(())
}

fn launch_etcd(
    node_index: NodeIndex,
    network: &Network,
    data_root: &str,
    verbose: bool,
    ferry_params: Tuning,
) -> io::Result<Child> {
    let stderr =
        if verbose {
            Stdio::inherit()
        } else {
            Stdio::null()
        };
    // Etcd takes a single timeout parameter, not a range.  This value
    // is used as the low end of the range, and it is doubled to
    // create the high end of the range.  So here, we ignore the
    // configured high end.
    let election_timeout_ms =
        ferry_params.election_timeout_ms_low;
    Command::new("etcd")
        .args(etcd::node_args2(
            node_index,
            data_root,
            &network,
            Some((ferry_params.heartbeat_timeout_ms, election_timeout_ms)),
        ))
        .stderr(stderr)
        .spawn()
}

fn launch_ferry(
    node_index: NodeIndex,
    network: &Network,
    data_root: &str,
    verbose: bool,
    persist: bool,
    ferry_params: Tuning,
) -> io::Result<Child> {
    Command::new("ferry")
        .args(ferry::node_args(node_index, data_root, network, verbose, persist, ferry_params))
        .stderr(Stdio::inherit())
        .spawn()
}
