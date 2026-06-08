// crates\bm-report\src\lib.rs
use anyhow::{Context, Result};
use bm_schema::raw_result::RawObservation;
use bm_schema::streaming::{StreamingRawObservation, StreamingSummary};
use bm_schema::summary::QuerySummary;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

pub fn summarize_run(input_path: &str) -> Result<(Vec<QuerySummary>, PathBuf)> {
    let observations = load_jsonl(input_path)?;
    let summaries = build_query_summaries(&observations);

    let output_path = output_summary_path(input_path, "query_summary.json")?;
    fs::write(&output_path, serde_json::to_vec_pretty(&summaries)?)?;

    Ok((summaries, output_path))
}

pub fn compare_runs(input_paths: &[String]) -> Result<(Vec<QuerySummary>, PathBuf)> {
    if input_paths.is_empty() {
        anyhow::bail!("compare requires at least one input path");
    }

    let mut all_observations = Vec::new();

    for input_path in input_paths {
        all_observations.extend(load_jsonl(input_path)?);
    }

    let summaries = build_query_summaries(&all_observations);

    let output_path = comparison_output_path("comparison_summary.json");
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }

    fs::write(&output_path, serde_json::to_vec_pretty(&summaries)?)?;

    Ok((summaries, output_path))
}

pub fn summarize_streaming_run(input_path: &str) -> Result<(Vec<StreamingSummary>, PathBuf)> {
    let observations = load_streaming_jsonl(input_path)?;
    let summaries = build_streaming_summaries(&observations);

    let output_path = output_summary_path(input_path, "streaming_summary.json")?;
    fs::write(&output_path, serde_json::to_vec_pretty(&summaries)?)?;

    Ok((summaries, output_path))
}

pub fn compare_streaming_runs(input_paths: &[String]) -> Result<(Vec<StreamingSummary>, PathBuf)> {
    if input_paths.is_empty() {
        anyhow::bail!("streaming compare requires at least one input path");
    }

    let mut all_observations = Vec::new();

    for input_path in input_paths {
        all_observations.extend(load_streaming_jsonl(input_path)?);
    }

    let summaries = build_streaming_summaries(&all_observations);

    let output_path = comparison_output_path("streaming_comparison_summary.json");
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }

    fs::write(&output_path, serde_json::to_vec_pretty(&summaries)?)?;

    Ok((summaries, output_path))
}

fn load_jsonl(path: &str) -> Result<Vec<RawObservation>> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read observations file: {path}"))?;

    let mut rows = Vec::new();

    for (idx, line) in raw.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }

        let row: RawObservation = serde_json::from_str(line)
            .with_context(|| format!("failed to parse JSONL line {} in {}", idx + 1, path))?;
        rows.push(row);
    }

    Ok(rows)
}

fn load_streaming_jsonl(path: &str) -> Result<Vec<StreamingRawObservation>> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read streaming observations file: {path}"))?;

    let mut rows = Vec::new();

    for (idx, line) in raw.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }

        let row: StreamingRawObservation = serde_json::from_str(line).with_context(|| {
            format!(
                "failed to parse streaming JSONL line {} in {}",
                idx + 1,
                path
            )
        })?;
        rows.push(row);
    }

    Ok(rows)
}

fn build_query_summaries(rows: &[RawObservation]) -> Vec<QuerySummary> {
    let mut groups: BTreeMap<
        (
            String,
            String,
            String,
            String,
            String,
            String,
            String,
            String,
            String,
            String,
        ),
        Vec<&RawObservation>,
    > = BTreeMap::new();

    for row in rows {
        let key = (
            row.engine_name.clone(),
            row.engine_version.clone(),
            row.workload_name.clone(),
            row.workload_family.clone(),
            row.dataset_name.clone(),
            row.dataset_family.clone(),
            row.dataset_format.clone(),
            row.query_id.clone(),
            row.query_name.clone(),
            row.query_category.clone(),
        );
        groups.entry(key).or_default().push(row);
    }

    let mut summaries = Vec::new();

    for (
        (
            engine_name,
            engine_version,
            workload_name,
            workload_family,
            dataset_name,
            dataset_family,
            dataset_format,
            query_id,
            query_name,
            query_category,
        ),
        entries,
    ) in groups
    {
        let attempts = entries.len() as u32;
        let successes = entries.iter().filter(|r| r.success).count() as u32;

        let all_elapsed: Vec<u64> = entries
            .iter()
            .filter(|r| r.success)
            .map(|r| r.elapsed_ms)
            .collect();

        let cold_elapsed: Vec<u64> = entries
            .iter()
            .filter(|r| r.success && r.warm_or_cold == "cold")
            .map(|r| r.elapsed_ms)
            .collect();

        let hot_elapsed: Vec<u64> = entries
            .iter()
            .filter(|r| r.success && r.warm_or_cold == "hot")
            .map(|r| r.elapsed_ms)
            .collect();

        let (mean_all_ms, min_all_ms, max_all_ms) = summarize_elapsed(&all_elapsed);
        let (mean_cold_ms, _, _) = summarize_elapsed(&cold_elapsed);
        let (mean_hot_ms, _, _) = summarize_elapsed(&hot_elapsed);

        summaries.push(QuerySummary {
            engine_name,
            engine_version,
            workload_name,
            workload_family,
            dataset_name,
            dataset_family,
            dataset_format,
            query_id,
            query_name,
            query_category,
            attempts,
            successes,
            mean_all_ms,
            min_all_ms,
            max_all_ms,
            mean_cold_ms,
            mean_hot_ms,
        });
    }

    summaries
}

fn build_streaming_summaries(rows: &[StreamingRawObservation]) -> Vec<StreamingSummary> {
    let mut groups: BTreeMap<(String, String, String, String), Vec<&StreamingRawObservation>> =
        BTreeMap::new();

    for row in rows {
        let key = (
            row.engine_name.clone(),
            row.engine_version.clone(),
            row.workload_name.clone(),
            row.workload_family.clone(),
        );
        groups.entry(key).or_default().push(row);
    }

    let mut summaries = Vec::new();

    for ((engine_name, engine_version, workload_name, workload_family), entries) in groups {
        let attempts = entries.len() as u32;
        let successes = entries.iter().filter(|row| row.success).count() as u32;
        let correctness_passes = entries
            .iter()
            .filter(|row| row.success && row.correctness_passed)
            .count() as u32;

        summaries.push(StreamingSummary {
            engine_name,
            engine_version,
            workload_name,
            workload_family,
            attempts,
            successes,
            correctness_passes,
            mean_startup_time_ms: mean_f64(&entries, |row| row.startup_time_ms as f64),
            mean_throughput_events_per_sec: mean_f64(&entries, |row| row.throughput_events_per_sec),
            mean_latency_p50_ms: mean_f64(&entries, |row| row.latency_p50_ms),
            mean_latency_p95_ms: mean_f64(&entries, |row| row.latency_p95_ms),
            mean_latency_p99_ms: mean_f64(&entries, |row| row.latency_p99_ms),
            total_processed_events: entries.iter().map(|row| row.processed_events).sum(),
            total_dropped_events: entries.iter().map(|row| row.dropped_events).sum(),
            total_failed_events: entries.iter().map(|row| row.failed_events).sum(),
            total_records_emitted: entries.iter().map(|row| row.records_emitted).sum(),
            total_emitted_windows: entries.iter().map(|row| row.emitted_windows).sum(),
        });
    }

    summaries
}

fn summarize_elapsed(values: &[u64]) -> (f64, u64, u64) {
    if values.is_empty() {
        return (0.0, 0, 0);
    }

    let sum: u64 = values.iter().sum();
    let mean = sum as f64 / values.len() as f64;
    let min = *values.iter().min().unwrap();
    let max = *values.iter().max().unwrap();

    (mean, min, max)
}

fn mean_f64<T>(values: &[T], accessor: impl Fn(&T) -> f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }

    values.iter().map(accessor).sum::<f64>() / values.len() as f64
}

fn output_summary_path(input_path: &str, filename: &str) -> Result<PathBuf> {
    let input = Path::new(input_path);
    let parent = input
        .parent()
        .context("input observations path has no parent directory")?;
    Ok(parent.join(filename))
}

fn comparison_output_path(filename: &str) -> PathBuf {
    PathBuf::from("results").join("comparisons").join(filename)
}

pub fn print_terminal_summary(rows: &[QuerySummary]) {
    println!(
        "{:<12} {:<8} {:<8} {:<10} {:<10} {:<12} {:<12} {:<12} {:<8} {:<8}",
        "Engine",
        "Format",
        "Query",
        "Attempts",
        "Successes",
        "MeanAll(ms)",
        "MeanCold(ms)",
        "MeanHot(ms)",
        "Min",
        "Max"
    );

    for row in rows {
        println!(
            "{:<12} {:<8} {:<8} {:<10} {:<10} {:<12.1} {:<12.1} {:<12.1} {:<8} {:<8}",
            row.engine_name,
            row.dataset_format,
            row.query_id,
            row.attempts,
            row.successes,
            row.mean_all_ms,
            row.mean_cold_ms,
            row.mean_hot_ms,
            row.min_all_ms,
            row.max_all_ms
        );
    }
}

pub fn print_terminal_streaming_summary(rows: &[StreamingSummary]) {
    println!(
        "{:<14} {:<22} {:<8} {:<8} {:<10} {:<14} {:<12} {:<12} {:<12}",
        "Engine",
        "Workload",
        "Runs",
        "OK",
        "Correct",
        "Throughput",
        "P50(ms)",
        "P95(ms)",
        "P99(ms)"
    );

    for row in rows {
        println!(
            "{:<14} {:<22} {:<8} {:<8} {:<10} {:<14.1} {:<12.3} {:<12.3} {:<12.3}",
            row.engine_name,
            row.workload_name,
            row.attempts,
            row.successes,
            row.correctness_passes,
            row.mean_throughput_events_per_sec,
            row.mean_latency_p50_ms,
            row.mean_latency_p95_ms,
            row.mean_latency_p99_ms
        );
    }
}
