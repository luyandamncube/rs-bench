use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingRunConfig {
    pub run_name: String,
    pub engine: String,
    pub workload_path: String,
    pub duration_secs: u64,
    pub warmup_secs: u64,
    pub repetitions: u32,
    pub event_rate_per_sec: u64,
    pub seed: u64,
    pub correctness_mode: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingEventSchemaField {
    pub name: String,
    pub data_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingSourceShape {
    pub family: String,
    pub event_time_field: String,
    pub key_field: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingParseSpec {
    pub mode: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingFilterSpec {
    pub field: String,
    pub op: Option<String>,
    pub equals: Option<String>,
    pub values: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingWindowSpec {
    pub window_type: String,
    pub size_secs: u64,
    pub slide_secs: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingExpectedOutput {
    pub aggregate_by: String,
    pub value_field: String,
    pub expected_group_totals: Vec<StreamingExpectedGroupTotal>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingExpectedGroupTotal {
    pub key: String,
    pub event_count: u64,
    pub value_sum: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingScenario {
    pub name: String,
    pub operation: String,
    pub group_by: Vec<String>,
    pub aggregate_count_as: Option<String>,
    pub aggregate_sum_field: Option<String>,
    pub aggregate_sum_as: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingPipelineOperator {
    pub kind: String,
    pub family: Option<String>,
    pub mode: Option<String>,
    pub field: Option<String>,
    pub op: Option<String>,
    pub equals: Option<String>,
    pub values: Option<Vec<String>>,
    pub size_secs: Option<u64>,
    pub count_as: Option<String>,
    pub sum_field: Option<String>,
    pub sum_as: Option<String>,
    pub aggregate_functions: Option<Vec<String>>,
    pub sink_mode: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingWorkloadDefinition {
    pub name: String,
    pub family: String,
    pub description: String,
    pub source: StreamingSourceShape,
    pub parse: Option<StreamingParseSpec>,
    pub filter: Option<StreamingFilterSpec>,
    pub schema: Vec<StreamingEventSchemaField>,
    pub scenario: StreamingScenario,
    pub window: StreamingWindowSpec,
    pub pipeline: Option<Vec<StreamingPipelineOperator>>,
    pub expected_output: StreamingExpectedOutput,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingRawObservation {
    pub run_id: String,
    pub run_name: String,
    pub engine_name: String,
    pub engine_version: String,
    pub workload_name: String,
    pub workload_family: String,
    pub repetition: u32,
    pub started_at: DateTime<Utc>,
    pub duration_secs: u64,
    pub warmup_secs: u64,
    pub event_rate_per_sec: u64,
    pub seed: u64,
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
pub struct StreamingSummary {
    pub engine_name: String,
    pub engine_version: String,
    pub workload_name: String,
    pub workload_family: String,
    pub attempts: u32,
    pub successes: u32,
    pub correctness_passes: u32,
    pub mean_startup_time_ms: f64,
    pub mean_throughput_events_per_sec: f64,
    pub mean_latency_p50_ms: f64,
    pub mean_latency_p95_ms: f64,
    pub mean_latency_p99_ms: f64,
    pub total_processed_events: u64,
    pub total_dropped_events: u64,
    pub total_failed_events: u64,
    pub total_records_emitted: u64,
    pub total_emitted_windows: u64,
}
