use bm_engine_mini_flink::MiniFlinkAdapter;
use bm_runner::run_streaming_benchmark;
use std::fs;

#[test]
fn mini_flink_streaming_run_writes_sink_output() {
    let base = std::env::temp_dir().join("bm_runner_mini_flink_test");
    let _ = fs::remove_dir_all(&base);
    fs::create_dir_all(base.join("configs/engines")).unwrap();
    fs::create_dir_all(base.join("configs/streaming")).unwrap();
    fs::create_dir_all(base.join("workloads/streaming/compute")).unwrap();

    fs::write(
        base.join("configs/engines/mini_flink.toml"),
        r#"keyed_parallelism = 2
channel_capacity = 64
sink_mode = "file"
sink_subdir = "results/streaming_sinks"
"#,
    )
    .unwrap();

    fs::write(
        base.join("workloads/streaming/compute/mini.yaml"),
        r#"name: mini_test
family: clickstream_streaming
description: mini
source:
  family: clickstream
  event_time_field: event_time
  key_field: device_type
parse:
  mode: synthetic_clickstream_v1
filter:
  field: event_type
  equals: page_view
schema:
  - name: event_time
    data_type: timestamp_ms
  - name: device_type
    data_type: string
  - name: event_type
    data_type: string
  - name: value
    data_type: u64
scenario:
  name: fixed
  operation: tumbling_window_grouped_sum
  group_by: [device_type]
  aggregate_count_as: event_count
  aggregate_sum_field: value
  aggregate_sum_as: value_sum
window:
  window_type: tumbling
  size_secs: 1
  slide_secs: null
pipeline:
  - kind: source
    family: clickstream
  - kind: map
    mode: synthetic_clickstream_v1
  - kind: filter
    field: event_type
    equals: page_view
  - kind: key_by
    field: device_type
  - kind: window
    size_secs: 1
  - kind: aggregate
    count_as: event_count
    sum_field: value
    sum_as: value_sum
  - kind: sink
    sink_mode: file
expected_output:
  aggregate_by: device_type
  value_field: value
  expected_group_totals:
    - key: mobile
      event_count: 5
      value_sum: 10
    - key: desktop
      event_count: 5
      value_sum: 15
"#,
    )
    .unwrap();

    fs::write(
        base.join("configs/streaming/mini.toml"),
        r#"run_name = "mini"
engine = "mini_flink"
workload_path = "workloads/streaming/compute/mini.yaml"
duration_secs = 1
warmup_secs = 0
repetitions = 1
event_rate_per_sec = 10
seed = 1
correctness_mode = "baseline"
"#,
    )
    .unwrap();

    let original_dir = std::env::current_dir().unwrap();
    std::env::set_current_dir(&base).unwrap();

    let mut adapter = MiniFlinkAdapter::new();
    let results_dir = run_streaming_benchmark("configs/streaming/mini.toml", &mut adapter).unwrap();
    let raw = fs::read_to_string(results_dir.join("streaming_raw_observations.jsonl")).unwrap();
    let pipeline_text = fs::read_to_string(results_dir.join("pipeline_graph.txt")).unwrap();
    let pipeline_json = fs::read_to_string(results_dir.join("pipeline_graph.json")).unwrap();
    let validation_json = fs::read_to_string(results_dir.join("pipeline_validation.json")).unwrap();

    std::env::set_current_dir(original_dir).unwrap();

    assert!(raw.contains("\"engine_name\":\"mini_flink\""));
    assert!(raw.contains("\"emitted_windows\":"));
    assert!(raw.contains("\"sink_output_path\":\"results/streaming_sinks/"));
    assert!(pipeline_text.contains("source(clickstream)"));
    assert!(pipeline_text.contains(".key_by(device_type)"));
    assert!(pipeline_json.contains("\"type\": \"WindowTumbling\""));
    assert!(validation_json.contains("\"operator_sequence\""));
    assert!(validation_json.contains("\"requires_keyed_state\": true"));
}
