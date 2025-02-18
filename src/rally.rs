use crate::Tuning;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Clone,Deserialize)]
pub struct NodeResource {
    pub data_root: String,
    pub client_address: Address,
    pub peer_address: Address
}

#[derive(Clone,Debug,Deserialize,Serialize)]
pub struct NodeInterface {
    pub client_address: Address,
    pub peer_address: Address
}

#[derive(Clone,Debug,Deserialize,Serialize)]
pub struct Address {
    pub host: String,
    pub port: u16,
}

impl Address {
    pub fn render(&self) -> String {
        format!("{}:{}", self.host, self.port.to_string())
    }
    pub fn http(&self) -> String {
        format!("http://{}", self.render())
    }
}

pub type NodeIndex = u16;

pub type Network = HashMap<NodeIndex,NodeInterface>;

#[derive(PartialEq,Eq,Clone,Deserialize,Debug,Hash,Serialize)]
pub struct Rally {
    pub network_id: String,
    pub competitor_id: String,
    pub cluster_size: NodeIndex,
    pub request_ops_per_s: u32,
    pub request_total: u32,
    pub client_count: u16,
    pub message_latency_ms: u32,
}

impl Rally {
    pub fn logical_request_offset(&self, index: u32) -> Duration {
        let cadence = 1.0 / (self.request_ops_per_s as f64);
        Duration::from_secs_f64(cadence * (index as f64))
    }

    pub fn competitor(&self) -> Option<Competitor> {
        Competitor::from_string(&self.competitor_id)
    }
}

#[derive(PartialEq,Eq,Clone,Deserialize,Debug,Hash,Serialize)]
pub enum Competitor {
    Etcd,
    EtcdClientRemove,
    Ferry,
    FerryNoDisk,
}

impl Competitor {
    pub fn from_string(s: &str) -> Option<Self> {
        match s {
            s if s == "etcd".to_string() => Some(Self::Etcd),
            s if s == "etcd-r".to_string() => Some(Self::EtcdClientRemove),
            s if s == "ferry".to_string() => Some(Self::Ferry),
            s if s == "ferry-nodisk".to_string() => Some(Self::FerryNoDisk),
            _ => None
        }
    }
}

#[derive(PartialEq,Eq,Clone,Deserialize,Debug,Hash,Serialize)]
pub struct ERally {
    pub network_id: String,
    pub competitor_id: String,
    pub cluster_size: NodeIndex,
    pub message_latency_ms: u32,
    pub tuning_parameters: Tuning,
}

impl ERally {
    pub fn competitor(&self) -> Option<Competitor> {
        Competitor::from_string(&self.competitor_id)
    }
}
