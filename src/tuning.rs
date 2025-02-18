use serde::{Deserialize,Serialize};

#[derive(Eq, PartialEq, Clone, Debug, Deserialize, Hash, Serialize)]
pub struct Tuning {
    pub election_timeout_ms_low: u32,
    pub election_timeout_ms_high: u32,
    pub retransmission_timeout_ms: u32,
    pub heartbeat_timeout_ms: u32,
}

impl Tuning {
    pub fn default() -> Tuning {
        Tuning {
            election_timeout_ms_low: 150,
            election_timeout_ms_high: 300,
            retransmission_timeout_ms: 200,
            heartbeat_timeout_ms: 10,
        }
    }
}
