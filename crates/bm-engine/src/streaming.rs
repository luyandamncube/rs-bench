use crate::error::EngineError;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingBootstrapRequest {
    pub run_id: String,
    pub capture_version: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingBootstrapResponse {
    pub engine_name: String,
    pub engine_version: String,
    pub adapter_version: String,
    pub started_service: bool,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingPrepareScenarioRequest {
    pub workload_name: String,
    pub workload_family: String,
    pub workload_path: String,
    pub duration_secs: u64,
    pub warmup_secs: u64,
    pub event_rate_per_sec: u64,
    pub seed: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingPrepareScenarioResponse {
    pub setup_started_at: DateTime<Utc>,
    pub setup_elapsed_ms: u64,
    pub registered_objects: Vec<String>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingRunRequest {
    pub repetition: u32,
    pub duration_secs: u64,
    pub warmup_secs: u64,
    pub event_rate_per_sec: u64,
    pub seed: u64,
    pub correctness_mode: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingRunResult {
    pub started_at: DateTime<Utc>,
    pub startup_time_ms: u64,
    pub throughput_events_per_sec: f64,
    pub latency_p50_ms: f64,
    pub latency_p95_ms: f64,
    pub latency_p99_ms: f64,
    pub processed_events: u64,
    pub dropped_events: u64,
    pub failed_events: u64,
    pub records_emitted: u64,
    pub emitted_windows: u64,
    pub sink_output_path: Option<String>,
    pub correctness_passed: bool,
    pub correctness_message: Option<String>,
    pub success: bool,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingCleanupRequest {
    pub run_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingCleanupResponse {
    pub success: bool,
    pub notes: Vec<String>,
}

pub trait StreamingEngineAdapter: Send {
    fn name(&self) -> &'static str;

    fn bootstrap_streaming(
        &mut self,
        req: StreamingBootstrapRequest,
    ) -> Result<StreamingBootstrapResponse, EngineError>;

    fn prepare_streaming_scenario(
        &mut self,
        req: StreamingPrepareScenarioRequest,
    ) -> Result<StreamingPrepareScenarioResponse, EngineError>;

    fn run_streaming(
        &mut self,
        req: StreamingRunRequest,
    ) -> Result<StreamingRunResult, EngineError>;

    fn cleanup_streaming(
        &mut self,
        req: StreamingCleanupRequest,
    ) -> Result<StreamingCleanupResponse, EngineError>;
}
