// crates\bm-schema\src\ranking.rs
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankingRecord {
    pub workload_family: String,
    pub engine_name: String,
    pub normalized_score: f64,
    pub rank: u32,
}