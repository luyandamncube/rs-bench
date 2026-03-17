// crates\bm-report\tests\summarize.rs
use bm_report::summarize_run;
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