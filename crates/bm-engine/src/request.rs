// crates\bm-engine\src\request.rs
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapRequest {
    pub run_id: String,
    pub capture_version: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareDatasetRequest {
    pub dataset_manifest_path: String,
    pub dataset_name: String,
    pub dataset_family: String,
    pub dataset_format: String,
    pub files: Vec<String>,
    pub mode: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunQueryRequest {
    pub query_id: String,
    pub query_name: String,
    pub query_category: String,
    pub sql: String,
    pub repetition: u32,
    pub warm_or_cold: String,
    pub capture_plan: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CleanupRequest {
    pub run_id: String,
}