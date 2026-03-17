// crates\bm-schema\src\raw_result.rs
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawObservation {
    pub run_id: String,
    pub run_name: String,
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
    pub repetition: u32,
    pub warm_or_cold: String,
    pub started_at: DateTime<Utc>,
    pub elapsed_ms: u64,
    pub success: bool,
    pub error_message: Option<String>,
}