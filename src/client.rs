pub mod etcd;
pub mod ferry;
pub mod ferry_monitor;

use crate::{Network, RequestRecord};

use std::thread;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

const REQUEST_LOOP_INTERVAL_MICROS: u64 = 500;

#[derive(Clone)]
pub enum CoreClient {
    Etcd(etcd_client::KvClient),
    Ferry(ferry::Client),
}

impl CoreClient {
    pub async fn request(&mut self, request_index: u32) -> bool {
        let value = format!("R{}", request_index);
        match self {
            Self::Etcd(client) => {
                let key = "mainkey";
                match client.put(key, value, None).await {
                    Ok(_) => true,
                    Err(_) => false,
                }
            },
            Self::Ferry(client) => {
                client.request(value).await;
                true
            },
        }
    }

    pub async fn connect_etcd(net: &Network) -> CoreClient {
        let addrs: Vec<String> =
            net.values().map(|n| n.client_address.render()).collect();
        let mut client = etcd_client::Client::connect(addrs, None).await
            .expect("Failed to connect etcd client.");
        let mut ready = false;
        while !ready {
            match client.status().await {
                Ok(_) => {
                    ready = true;
                },
                Err(_) => {
                    thread::sleep(Duration::from_millis(50));
                }
            }
        }
        Self::Etcd(client.kv_client())
    }

    // The Ferry client connects to node 0, which is the
    // designated initial leader in the Ferry cluster.
    pub async fn connect_ferry(client_id: u16, net: &Network) -> CoreClient {
        Self::Ferry(
            ferry::Client::connect(client_id, &net[&0].client_address).await
        )
    }
}

#[derive(Clone)]
pub struct DClient {
    pub clients: Vec<CoreClient>,
    next_put_index: usize,
    pub verbose: bool,
    pub retry_max: u32,
    pub request_loop_cadence: Duration,
}

impl DClient {
    fn spawn_request(
        &mut self,
        rally_start: &Instant,
        index: u32,
        loop_time_offset: &Duration,
    ) -> JoinHandle<Option<RequestRecord>> {
        let mut my_client = self.next_kv_client();
        self.inc_next();

        let my_retry_max = self.retry_max.clone();
        let my_rally_start = rally_start.clone();
        let my_loop_time_offset = loop_time_offset.clone();
        tokio::spawn(async move {
            let result = Self::request_with_retry(
                &mut my_client,
                index,
                my_retry_max,
            ).await;
            let response_time_offset = my_rally_start.elapsed();
            match result {
                Ok(retries) => {
                    Some(RequestRecord {
                        request_index: index,
                        request_time_offset: my_loop_time_offset,
                        response_time_offset,
                        retries,
                    })
                },
                Err(()) => {
                    eprintln!("Failed to put request #{}.", index);
                    None
                },
            }
        })
    }

    fn next_kv_client(&self) -> CoreClient {
        let i = self.next_put_index;
        self.clients[i].clone()
    }

    fn inc_next(&mut self) {
        if self.next_put_index < (self.clients.len() - 1) {
            self.next_put_index += 1;
        } else {
            self.next_put_index = 0;
        }
    }

    pub async fn run_rally(
        &mut self,
        request_count: u32,
        request_ops_per_s: u32,
        collect_timeout: Duration,
        rally_num: usize,
    ) -> Result<(Vec<Option<RequestRecord>>, Duration), String> {
        let request_ops_per_s = request_ops_per_s as f64;

        let mut requested = 0;
        let mut requests: Vec<JoinHandle<Option<RequestRecord>>> =
            Vec::with_capacity(request_count as usize);
        let rally_start = Instant::now();

        while requested < request_count {
            let loop_time_offset = rally_start.elapsed();
            let to_request =
                ((loop_time_offset.as_secs_f64() * request_ops_per_s) as u32 + 1)
                .min(request_count);
            for i in requested .. to_request {
                requests.push(self.spawn_request(
                    &rally_start,
                    i,
                    &loop_time_offset,
                ));
            }
            // requested will not go down even if somehow time goes
            // backwards.
            if to_request < requested {
                panic!("Time went backwards.");
            }
            requested = to_request;
            thread::sleep(Duration::from_micros(REQUEST_LOOP_INTERVAL_MICROS));
        }

        let mut aborts = Vec::with_capacity(request_count as usize);
        for r in requests.iter() {
            aborts.push(r.abort_handle());
        }

        let collect_task = tokio::spawn(async move {
            let mut records = Vec::with_capacity(request_count as usize);
            for (i,r) in requests.into_iter().enumerate() {
                match r.await {
                    Ok(a) => { records.push(a); },
                    Err(e) => {
                        panic!(
                            "Request {} of rally {} panicked: {}",
                            i, rally_num, e
                        );
                    },
                }
            }
            records
        });

        // collect_task returns an inner Err if it panics, and timeout
        // returns an outer Err if it times out.
        match tokio::time::timeout(collect_timeout, collect_task).await {
            Ok(Ok(records)) => {
                let rally_duration = rally_start.elapsed();
                if records.len() != request_count as usize {
                    panic!(
                        "{} requests, but {} records.",
                        request_count,
                        records.len(),
                    );
                }
                Ok((records, rally_duration))
            },
            Ok(Err(e)) => Err(format!("Record collection panic: {}", e)),
            Err(e) => {
                for a in aborts {
                    a.abort();
                }
                Err(format!("Record collection timeout: {}", e))
            }
        }
    }

    /// Returns a client connected to every node in the network.  This
    /// blocks until the client status() call returns Ok.
    pub async fn connect_etcd(
        network: &Network,
        client_count: u16,
        retry_max: u32,
        request_loop_cadence: Duration,
    ) -> DClient {
        let mut clients = Vec::new();
        for _ in 0 .. client_count {
            let c = CoreClient::connect_etcd(network).await;
            clients.push(c);
        }

        DClient{
            clients,
            next_put_index: 0,
            verbose: false,
            retry_max,
            request_loop_cadence,
        }
    }

    pub async fn connect_ferry(
        network: &Network,
        client_count: u16,
        retry_max: u32,
        request_loop_cadence: Duration,
    ) -> DClient {
        let mut clients = Vec::new();
        for i in 0 .. client_count {
            let c = CoreClient::connect_ferry(i, network).await;
            clients.push(c);
        }

        DClient{
            clients,
            next_put_index: 0,
            verbose: false,
            retry_max,
            request_loop_cadence,
        }
    }

    async fn request_with_retry(
        client: &mut CoreClient,
        index: u32,
        retry_max: u32
    ) -> Result<u32,()> {
        let mut done = false;
        let mut retries: u32 = 0;

        while !done && retries <= retry_max {
            match client.request(index).await {
                true => {
                    done = true;
                },
                false => {
                    retries += 1;
                    thread::sleep(Duration::from_millis(5));
                },
            }
        }
        if done {
            Ok(retries)
        } else {
            Err(())
        }
    }
}
