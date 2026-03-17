// crates\bm-report\src\lib.rs
use anyhow::{Context, Result};
use bm_schema::raw_result::RawObservation;
use bm_schema::summary::QuerySummary;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

pub fn summarize_run(input_path: &str) -> Result<(Vec<QuerySummary>, PathBuf)> {
    let observations = load_jsonl(input_path)?;
    let summaries = build_query_summaries(&observations);

    let output_path = output_summary_path(input_path)?;
    fs::write(&output_path, serde_json::to_vec_pretty(&summaries)?)?;

    Ok((summaries, output_path))
}

pub fn compare_runs(input_paths: &[String]) -> Result<(Vec<QuerySummary>, PathBuf)> {
    if input_paths.is_empty() {
        anyhow::bail!("compare requires at least one input path");
    }

    let mut all_observations = Vec::new();

    for input_path in input_paths {
        let observations = load_jsonl(input_path)?;
        all_observations.extend(observations);
    }

    let summaries = build_query_summaries(&all_observations);

    let output_path = comparison_output_path()?;
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

fn output_summary_path(input_path: &str) -> Result<PathBuf> {
    let input = Path::new(input_path);
    let parent = input
        .parent()
        .context("input observations path has no parent directory")?;
    Ok(parent.join("query_summary.json"))
}

fn comparison_output_path() -> Result<PathBuf> {
    Ok(PathBuf::from("results")
        .join("comparisons")
        .join("comparison_summary.json"))
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