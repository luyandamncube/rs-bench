use bm_engine::error::EngineError;
use bm_engine::streaming::{
    StreamingBootstrapRequest, StreamingBootstrapResponse, StreamingCleanupRequest,
    StreamingCleanupResponse, StreamingEngineAdapter, StreamingPrepareScenarioRequest,
    StreamingPrepareScenarioResponse, StreamingRunRequest, StreamingRunResult,
};
use bm_runner::run_streaming_benchmark;
use chrono::Utc;
use std::fs;

struct TestStreamingAdapter;

impl StreamingEngineAdapter for TestStreamingAdapter {
    fn name(&self) -> &'static str {
        "test_stream"
    }

    fn bootstrap_streaming(
        &mut self,
        _req: StreamingBootstrapRequest,
    ) -> Result<StreamingBootstrapResponse, EngineError> {
        Ok(StreamingBootstrapResponse {
            engine_name: "test_stream".into(),
            engine_version: "0.1.0".into(),
            adapter_version: "0.1.0".into(),
            started_service: false,
            notes: vec![],
        })
    }

    fn prepare_streaming_scenario(
        &mut self,
        _req: StreamingPrepareScenarioRequest,
    ) -> Result<StreamingPrepareScenarioResponse, EngineError> {
        Ok(StreamingPrepareScenarioResponse {
            setup_started_at: Utc::now(),
            setup_elapsed_ms: 1,
            registered_objects: vec!["test".into()],
            notes: vec![],
        })
    }

    fn run_streaming(
        &mut self,
        req: StreamingRunRequest,
    ) -> Result<StreamingRunResult, EngineError> {
        Ok(StreamingRunResult {
            started_at: Utc::now(),
            startup_time_ms: 2,
            throughput_events_per_sec: req.event_rate_per_sec as f64,
            latency_p50_ms: 0.5,
            latency_p95_ms: 0.8,
            latency_p99_ms: 1.1,
            processed_events: req.event_rate_per_sec * req.duration_secs,
            dropped_events: 0,
            failed_events: 0,
            records_emitted: req.event_rate_per_sec * req.duration_secs,
            emitted_windows: 1,
            sink_output_path: Some("results/test_streaming_sink.jsonl".into()),
            correctness_passed: true,
            correctness_message: None,
            success: true,
            error_message: None,
        })
    }

    fn cleanup_streaming(
        &mut self,
        _req: StreamingCleanupRequest,
    ) -> Result<StreamingCleanupResponse, EngineError> {
        Ok(StreamingCleanupResponse {
            success: true,
            notes: vec![],
        })
    }
}

#[test]
fn streaming_runner_writes_raw_observations() {
    let base = std::env::temp_dir().join("bm_runner_streaming_test");
    let _ = fs::remove_dir_all(&base);
    fs::create_dir_all(base.join("workloads")).unwrap();

    let workload_path = base.join("workloads/windowed.yaml");
    fs::write(
        &workload_path,
        r#"name: test_stream
family: streaming
description: test
source:
  family: clickstream
  event_time_field: event_time
  key_field: device_type
schema:
  - name: event_time
    data_type: timestamp_ms
scenario:
  name: agg
  operation: tumbling_window_grouped_sum
  group_by: [device_type]
  aggregate_count_as: event_count
  aggregate_sum_field: value
  aggregate_sum_as: value_sum
window:
  window_type: tumbling
  size_secs: 2
  slide_secs: null
expected_output:
  aggregate_by: device_type
  value_field: value
  expected_group_totals:
    - key: mobile
      event_count: 20
      value_sum: 40
"#,
    )
    .unwrap();

    let config_path = base.join("streaming.toml");
    fs::write(
        &config_path,
        format!(
            r#"run_name = "streaming_test"
engine = "test_stream"
workload_path = "{}"
duration_secs = 2
warmup_secs = 1
repetitions = 2
event_rate_per_sec = 10
seed = 9
correctness_mode = "baseline"
"#,
            workload_path.display()
        ),
    )
    .unwrap();

    let original_dir = std::env::current_dir().unwrap();
    std::env::set_current_dir(&base).unwrap();

    let mut adapter = TestStreamingAdapter;
    let results_dir = run_streaming_benchmark(config_path.to_str().unwrap(), &mut adapter).unwrap();
    let raw = fs::read_to_string(results_dir.join("streaming_raw_observations.jsonl")).unwrap();

    std::env::set_current_dir(original_dir).unwrap();

    assert_eq!(raw.lines().count(), 2);
    assert!(raw.contains("\"throughput_events_per_sec\":10.0"));
    assert!(raw.contains("\"correctness_passed\":true"));
    assert!(raw.contains("\"emitted_windows\":1"));
}
