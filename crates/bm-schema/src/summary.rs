// crates\bm-schema\src\summary.rs
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuerySummary {
    pub engine_name: String,
    pub engine_version: String,
    pub workload_name: String,
    pub workload_family: String,
    pub dataset_name: String,
    pub dataset_family: String,
    pub dataset_format: String,
    pub query_id: String,
    pub query_name: String,
    pub query_category: String,
    pub attempts: u32,
    pub successes: u32,
    pub mean_all_ms: f64,
    pub min_all_ms: u64,
    pub max_all_ms: u64,
    pub mean_cold_ms: f64,
    pub mean_hot_ms: f64,
}