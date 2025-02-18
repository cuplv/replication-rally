use crate::rally::{ERally, Rally};

use csv::Writer;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{Read,Write};
use std::path::PathBuf;
use std::error::Error;
use std::time::Duration;

fn duration_float_ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

#[derive(Deserialize, Serialize)]
pub struct RequestRecord {
    pub request_index: u32,
    pub request_time_offset: Duration,
    pub response_time_offset: Duration,
    pub retries: u32,
}

impl RequestRecord {
    pub fn request_latency(&self, rally: &Rally) -> Duration {
        self.request_time_offset -
            rally.logical_request_offset(self.request_index)
    }
    pub fn response_latency(&self, rally: &Rally) -> Duration {
        self.response_time_offset -
            rally.logical_request_offset(self.request_index)
    }
}

#[derive(PartialEq,Clone,Debug)]
pub struct Outcome {
    /// ms from logical request time to actual request time
    pub average_request_latency_ms: f64,
    /// ms from logical request time to response time
    pub average_response_latency_ms: f64,
    /// Round parameters
    pub rally: Rally,
    /// Throughput in ops per ms
    pub throughput_ops_per_ms: f64,
    /// Failed (timed-out) operations
    pub failures: u32,
    /// Number of averaged reps contributing to this outcome
    pub reps: u32,
    /// Single-retried operations
    pub single_retries: u32,
    /// Multi-retried operations
    pub multi_retries: u32,
}

impl Outcome {
    pub fn new(
        rally: &Rally,
        all_records: Vec<Option<RequestRecord>>,
        round_duration: Duration,
    ) -> Outcome {
        let attempts = all_records.len();
        let records: Vec<RequestRecord> =
            all_records.into_iter().filter_map(|a| a).collect();
        let failures = (attempts - records.len()) as u32;

        let average_request_latency = records.iter()
            .map(|r| r.request_latency(rally))
            .sum::<Duration>()
            / rally.request_total;
        let average_response_latency = records.iter()
            .map(|r| r.response_latency(rally))
            .sum::<Duration>()
            / rally.request_total;
        // let average_latency =
        //     latencies.iter().sum::<Duration>()
        //     / (latencies.len() as u32);
        let throughput_ops_per_ms =
            (rally.request_total as f64) /
            duration_float_ms(round_duration);
        let single_retries = records.iter()
            .filter(|r| r.retries == 1)
            .count() as u32;
        let multi_retries = records.iter()
            .filter(|r| r.retries > 1)
            .count() as u32;
        Outcome {
            rally: rally.clone(),
            average_request_latency_ms:
            duration_float_ms(average_request_latency),
            average_response_latency_ms:
            duration_float_ms(average_response_latency),
            throughput_ops_per_ms,
            failures,
            // An actual new Outcome records a single rep
            reps: 1,
            single_retries,
            multi_retries,
        }
    }

    pub fn average(rally: &Rally, outcomes: &Vec<Outcome>) -> Outcome {
        let mut req_ls = Vec::new();
        let mut resp_ls = Vec::new();
        let mut thrus = Vec::new();
        let mut ss = Vec::new();
        let mut repss = Vec::new();
        let mut s_r_s = Vec::new();
        let mut m_r_s = Vec::new();

        for o in outcomes {
            if o.rally != *rally {
                panic!("Outcome averaged against differing rally.");
            }
            req_ls.push(o.average_request_latency_ms);
            resp_ls.push(o.average_response_latency_ms);
            thrus.push(o.throughput_ops_per_ms);
            ss.push(o.failures);
            repss.push(o.reps);
            s_r_s.push(o.single_retries);
            m_r_s.push(o.multi_retries);
        }

        Outcome {
            rally: rally.clone(),
            average_request_latency_ms: avg_f64(&req_ls),
            average_response_latency_ms: avg_f64(&resp_ls),
            throughput_ops_per_ms: avg_f64(&thrus),
            failures: ss.iter().sum(),
            reps: repss.iter().sum(),
            single_retries: s_r_s.iter().sum(),
            multi_retries: m_r_s.iter().sum(),
        }
    }

    pub fn print(&self) {
        println!("Cluster size: {} nodes", self.rally.cluster_size);
        println!("Request rate: {} ops/s", self.rally.request_ops_per_s);
        println!("Request total: {} ops", self.rally.request_total);
        println!(
            "Throughput: {:.3} op/ms.",
            self.throughput_ops_per_ms,
        );
        println!(
            "Average request latency: {:.3} ms",
            self.average_request_latency_ms,
        );
        println!(
            "Average response latency: {:.3} ms",
            self.average_response_latency_ms,
        );
        println!(
            "Failed (timed-out) operations: {}",
            self.failures,
        );
        println!(
            "Contributing reps: {}",
            self.reps,
        );
        println!(
            "Single-retried operations: {}",
            self.single_retries,
        );
        println!(
            "Multi-retried operations: {}",
            self.multi_retries,
        );
        println!(
            "Client count: {}",
            self.multi_retries,
        );
        println!(
            "Imposed message latency: {} ms",
            self.multi_retries,
        );
    }

    pub fn csv_write<W: Write>(
        &self,
        wtr: W
    ) -> Result<(), Box<dyn Error>> {
        let mut csv_wtr = Writer::from_writer(wtr);
        csv_wtr.write_record(&[
            self.rally.network_id.clone(),
            self.rally.competitor_id.clone(),
            format!("{}", self.rally.cluster_size),
            format!("{}", self.rally.request_total),
            format!("{}", self.rally.request_ops_per_s),
            format!("{:.3}", self.average_request_latency_ms),
            format!("{:.3}", self.average_response_latency_ms),
            format!("{:.3}", self.throughput_ops_per_ms),
            format!("{}", self.failures),
            format!("{}", self.reps),
            format!("{}", self.single_retries),
            format!("{}", self.multi_retries),
            format!("{}", self.rally.client_count),
            format!("{}", self.rally.message_latency_ms),
        ])?;
        csv_wtr.flush()?;
        Ok(())
    }

    pub fn csv_read<R: Read>(rdr: R) -> Result<Vec<Outcome>, Box<dyn Error>> {
        let mut rdr = csv::Reader::from_reader(rdr);
        let mut v = Vec::new();
        for r in rdr.records() {
            let r = r?;
            let rally = Rally {
                network_id: r[0].to_string(),
                competitor_id: r[1].to_string(),
                cluster_size: r[2].parse().unwrap(),
                request_total: r[3].parse().unwrap(),
                request_ops_per_s: r[4].parse().unwrap(),
                client_count: r[12].parse().unwrap(),
                message_latency_ms: r[13].parse().unwrap(),
            };
            let outcome = Outcome {
                rally,
                average_request_latency_ms: r[5].parse().unwrap(),
                average_response_latency_ms: r[6].parse().unwrap(),
                throughput_ops_per_ms: r[7].parse().unwrap(),
                failures: r[8].parse().unwrap(),
                reps: r[9].parse().unwrap(),
                single_retries: r[10].parse().unwrap(),
                multi_retries: r[11].parse().unwrap(),
            };
            v.push(outcome);
        }
        Ok(v)
    }

    pub fn csv_write_file(&self, output_path: &PathBuf) {
        let results_file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&output_path)
            .unwrap();
        self.csv_write(&results_file).unwrap();
    }

    pub fn csv_write_file_all(outcomes: &Vec<Outcome>, output_path: &PathBuf) {
        let results_file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&output_path)
            .unwrap();
        for o in outcomes {
            o.csv_write(&results_file).unwrap();
        }
    }
}

fn avg_f64(vals: &Vec<f64>) -> f64 {
    vals.iter().sum::<f64>() / vals.len() as f64
}

#[derive(PartialEq,Clone,Debug,Deserialize,Serialize)]
pub struct EOutcome {
    pub rally: ERally,
    pub duration_ms: f64,
}
