// crates\bm-report\tests\summarize.rs
use bm_report::{summarize_run, summarize_streaming_run};
use std::fs;

#[test]
fn summarize_run_writes_query_summary() {
    let base = std::env::temp_dir().join("bm_report_test");
    let _ = fs::remove_dir_all(&base);
    fs::create_dir_all(&base).unwrap();

    let input = base.join("raw_observations.jsonl");

    let sample = r#"{"run_id":"r1","run_name":"smoke","engine_name":"mock","engine_version":"0.0.1","workload_name":"w1","workload_family":"clickstream","dataset_name":"d1","dataset_family":"clickstream","dataset_format":"csv","query_id":"q01","query_name":"session_filter","query_category":"scan_filter","repetition":1,"warm_or_cold":"cold","started_at":"2026-03-17T08:24:56.282157162Z","elapsed_ms":51,"success":true,"error_message":null}
{"run_id":"r1","run_name":"smoke","engine_name":"mock","engine_version":"0.0.1","workload_name":"w1","workload_family":"clickstream","dataset_name":"d1","dataset_family":"clickstream","dataset_format":"csv","query_id":"q01","query_name":"session_filter","query_category":"scan_filter","repetition":2,"warm_or_cold":"hot","started_at":"2026-03-17T08:24:56.282160666Z","elapsed_ms":52,"success":true,"error_message":null}
{"run_id":"r1","run_name":"smoke","engine_name":"mock","engine_version":"0.0.1","workload_name":"w1","workload_family":"clickstream","dataset_name":"d1","dataset_family":"clickstream","dataset_format":"csv","query_id":"q01","query_name":"session_filter","query_category":"scan_filter","repetition":3,"warm_or_cold":"hot","started_at":"2026-03-17T08:24:56.282161756Z","elapsed_ms":53,"success":true,"error_message":null}
"#;

    fs::write(&input, sample).unwrap();

    let (rows, _output_path) = summarize_run(input.to_str().unwrap()).unwrap();

    assert_eq!(rows[0].attempts, 3);
    assert_eq!(rows[0].successes, 3);
    assert_eq!(rows[0].min_all_ms, 51);
    assert_eq!(rows[0].max_all_ms, 53);
    assert!((rows[0].mean_all_ms - 52.0).abs() < 0.001);
    assert!((rows[0].mean_cold_ms - 51.0).abs() < 0.001);
    assert!((rows[0].mean_hot_ms - 52.5).abs() < 0.001);
}

#[test]
fn summarize_streaming_run_writes_streaming_summary() {
    let base = std::env::temp_dir().join("bm_report_streaming_test");
    let _ = fs::remove_dir_all(&base);
    fs::create_dir_all(&base).unwrap();

    let input = base.join("streaming_raw_observations.jsonl");

    let sample = r#"{"run_id":"s1","run_name":"stream","engine_name":"stream_local","engine_version":"0.1.0","workload_name":"windowed_clickstream_core","workload_family":"clickstream_streaming","repetition":1,"started_at":"2026-03-17T08:24:56.282157162Z","duration_secs":3,"warmup_secs":1,"event_rate_per_sec":2400,"seed":7,"startup_time_ms":2,"throughput_events_per_sec":1200.0,"latency_p50_ms":0.5,"latency_p95_ms":0.8,"latency_p99_ms":1.0,"processed_events":7200,"dropped_events":0,"failed_events":0,"records_emitted":7200,"emitted_windows":1,"sink_output_path":"results/test1.jsonl","correctness_passed":true,"correctness_message":null,"success":true,"error_message":null}
{"run_id":"s1","run_name":"stream","engine_name":"stream_local","engine_version":"0.1.0","workload_name":"windowed_clickstream_core","workload_family":"clickstream_streaming","repetition":2,"started_at":"2026-03-17T08:24:57.282157162Z","duration_secs":3,"warmup_secs":1,"event_rate_per_sec":2400,"seed":7,"startup_time_ms":4,"throughput_events_per_sec":1800.0,"latency_p50_ms":0.6,"latency_p95_ms":0.9,"latency_p99_ms":1.1,"processed_events":7200,"dropped_events":0,"failed_events":0,"records_emitted":7200,"emitted_windows":1,"sink_output_path":"results/test2.jsonl","correctness_passed":true,"correctness_message":null,"success":true,"error_message":null}
"#;

    fs::write(&input, sample).unwrap();

    let (rows, _output_path) = summarize_streaming_run(input.to_str().unwrap()).unwrap();

    assert_eq!(rows[0].attempts, 2);
    assert_eq!(rows[0].successes, 2);
    assert_eq!(rows[0].correctness_passes, 2);
    assert_eq!(rows[0].total_processed_events, 14400);
    assert_eq!(rows[0].total_records_emitted, 14400);
    assert_eq!(rows[0].total_emitted_windows, 2);
    assert!((rows[0].mean_throughput_events_per_sec - 1500.0).abs() < 0.001);
    assert!((rows[0].mean_latency_p95_ms - 0.85).abs() < 0.001);
}
