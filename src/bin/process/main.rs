use replication_rally::{
    Outcome,
    EOutcome,
    Rally,
};

use clap::{Parser,Subcommand};

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(short, long, value_name = "PATH", help = "input file path")]
    input: PathBuf,
    #[arg(short, long, value_name = "PATH", help = "output file path")]
    output: PathBuf,
    #[command(subcommand)]
    command: Mode,
}

#[derive(Subcommand)]
enum Mode {
    /// Process an election experiment result file (.jsonlines)
    Election,
    /// Process a throughput experiment result file (.csv)
    Throughput,
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Mode::Election => process_election(cli.input, cli.output),
        Mode::Throughput => process_throughput(cli.input, cli.output),
    }
}

fn process_election(input: PathBuf, output: PathBuf) {
    let mut lines = Vec::new();

    for line in fs::read_to_string(&input).unwrap().lines() {
        lines.push(line.to_string());
    }

    let records: Vec<EOutcome> = lines.into_iter().map(|s| {
        serde_json::from_str::<EOutcome>(s.as_str()).unwrap()
    }).collect();

    let output = fs::File::create(&output).unwrap();
    serde_json::to_writer_pretty(output, &records).unwrap();
}

fn process_throughput(input: PathBuf, output: PathBuf) {
    let input = fs::File::open(&input).unwrap();
    let os = Outcome::csv_read(input).unwrap();

    let mut m: HashMap<Rally,Vec<Outcome>> = HashMap::new();
    for outcome in os {
        match m.get_mut(&outcome.rally) {
            Some(rs) => {
                rs.push(outcome);
            },
            None => {
                m.insert(outcome.rally.clone(), vec![outcome]);
            },
        }
    }

    let mut v: Vec<Outcome> = Vec::new();
    for (rally, reps) in m {
        v.push(Outcome::average(&rally, &reps));
    }
    v.sort_by(|a,b| {
        let competitor =
            a.rally.competitor_id.cmp(&b.rally.competitor_id);
        let cluster_size =
            a.rally.cluster_size.cmp(&b.rally.cluster_size);
        let drive_rate =
            a.rally.request_ops_per_s.cmp(&b.rally.request_ops_per_s);
        competitor.then(cluster_size.then(drive_rate))
    });

    Outcome::csv_write_file_all(&v, &output);
}
