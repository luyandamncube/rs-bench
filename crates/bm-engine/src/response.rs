// crates\bm-engine\src\response.rs
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapResponse {
    pub engine_name: String,
    pub engine_version: String,
    pub adapter_version: String,
    pub started_service: bool,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareDatasetResponse {
    pub setup_started_at: DateTime<Utc>,
    pub setup_elapsed_ms: u64,
    pub registered_objects: Vec<String>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryExecutionResult {
    pub started_at: DateTime<Utc>,
    pub elapsed_ms: u64,
    pub success: bool,
    pub row_count: Option<u64>,
    pub error_message: Option<String>,
    pub plan_text: Option<String>,
    pub diagnostics_json: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CleanupResponse {
    pub success: bool,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineMetadata {
    pub engine_name: String,
    pub engine_version: String,
    pub adapter_version: String,
    pub execution_mode: String,
    pub file_format: Option<String>,
    pub table_mode: Option<String>,
    pub notes: Vec<String>,
}