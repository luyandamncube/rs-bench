// crates\bm-runner\src\lib.rs
use anyhow::{Context, Result};
use bm_engine::adapter::EngineAdapter;
use bm_engine::request::{BootstrapRequest, CleanupRequest, PrepareDatasetRequest, RunQueryRequest};
use bm_schema::dataset::DatasetManifest;
use bm_schema::raw_result::RawObservation;
use bm_schema::run::RunConfig;
use bm_schema::workload::WorkloadDefinition;
use chrono::Utc;
use csv::Writer;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use uuid::Uuid;

pub fn run_benchmark<A: EngineAdapter>(config_path: &str, adapter: &mut A) -> Result<PathBuf> {
    let run_config = load_run_config(config_path)?;
    let dataset_manifest = load_dataset_manifest(&run_config.dataset_manifest_path)?;
    let workload = load_workload(&run_config.workload_path)?;

    let run_id = Uuid::new_v4().to_string();
    let results_dir = PathBuf::from("results").join("runs").join(&run_id);
    fs::create_dir_all(&results_dir)?;

    let workload_dir = Path::new(&run_config.workload_path)
        .parent()
        .context("workload path has no parent directory")?;

    let bootstrap = adapter.bootstrap(BootstrapRequest {
        run_id: run_id.clone(),
        capture_version: true,
    })?;

    let dataset_base_dir = Path::new(&run_config.dataset_manifest_path)
        .parent()
        .context("dataset manifest path has no parent directory")?;

    let files: Vec<String> = dataset_manifest
        .files
        .iter()
        .map(|f| dataset_base_dir.join(f).to_string_lossy().to_string())
        .collect();

    let _prepare = adapter.prepare_dataset(PrepareDatasetRequest {
        dataset_manifest_path: run_config.dataset_manifest_path.clone(),
        dataset_name: dataset_manifest.dataset_name.clone(),
        dataset_family: dataset_manifest.dataset_family.clone(),
        dataset_format: dataset_manifest.format.clone(),
        files,
        mode: "file-backed".to_string(),
    })?;

    let mut observations = Vec::new();

    for query in &workload.queries {
        let sql_path = workload_dir.join(&query.file);
        let sql = fs::read_to_string(&sql_path)
            .with_context(|| format!("failed to read SQL file: {}", sql_path.display()))?;

        for repetition in 1..=run_config.repetitions {
            let warm_or_cold = if repetition == 1 { "cold" } else { "hot" }.to_string();

            let result = adapter.run_query(RunQueryRequest {
                query_id: query.id.clone(),
                query_name: query.name.clone(),
                query_category: query.category.clone(),
                sql: sql.clone(),
                repetition,
                warm_or_cold: warm_or_cold.clone(),
                capture_plan: run_config.capture_plans,
            })?;

            observations.push(RawObservation {
                run_id: run_id.clone(),
                run_name: run_config.run_name.clone(),
                engine_name: bootstrap.engine_name.clone(),
                engine_version: bootstrap.engine_version.clone(),
                workload_name: workload.name.clone(),
                workload_family: workload.family.clone(),
                dataset_name: dataset_manifest.dataset_name.clone(),
                dataset_family: dataset_manifest.dataset_family.clone(),
                dataset_format: dataset_manifest.format.clone(),
                query_id: query.id.clone(),
                query_name: query.name.clone(),
                query_category: query.category.clone(),
                repetition,
                warm_or_cold,
                started_at: result.started_at,
                elapsed_ms: result.elapsed_ms,
                success: result.success,
                error_message: result.error_message.clone(),
            });
        }
    }

    adapter.cleanup(CleanupRequest {
        run_id: run_id.clone(),
    })?;

    write_run_manifest(&results_dir, &run_id, &run_config, &dataset_manifest, &workload)?;
    write_jsonl(&results_dir.join("raw_observations.jsonl"), &observations)?;
    write_csv(&results_dir.join("raw_observations.csv"), &observations)?;

    Ok(results_dir)
}

fn load_run_config(path: &str) -> Result<RunConfig> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read run config: {path}"))?;
    let config = toml::from_str(&raw).context("failed to parse run config TOML")?;
    Ok(config)
}

fn load_dataset_manifest(path: &str) -> Result<DatasetManifest> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read dataset manifest: {path}"))?;
    let manifest = serde_json::from_str(&raw).context("failed to parse dataset manifest JSON")?;
    Ok(manifest)
}

fn load_workload(path: &str) -> Result<WorkloadDefinition> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read workload file: {path}"))?;
    let workload = serde_yaml::from_str(&raw).context("failed to parse workload YAML")?;
    Ok(workload)
}

fn write_run_manifest(
    results_dir: &Path,
    run_id: &str,
    run_config: &RunConfig,
    dataset_manifest: &DatasetManifest,
    workload: &WorkloadDefinition,
) -> Result<()> {
    let manifest = serde_json::json!({
        "run_id": run_id,
        "run_name": run_config.run_name,
        "engine": run_config.engine,
        "dataset_name": dataset_manifest.dataset_name,
        "dataset_family": dataset_manifest.dataset_family,
        "workload_name": workload.name,
        "workload_family": workload.family,
        "repetitions": run_config.repetitions,
        "capture_plans": run_config.capture_plans,
        "generated_at": Utc::now(),
    });

    fs::write(
        results_dir.join("run_manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;

    Ok(())
}

fn write_jsonl(path: &Path, rows: &[RawObservation]) -> Result<()> {
    let mut file = File::create(path)?;
    for row in rows {
        let line = serde_json::to_string(row)?;
        writeln!(file, "{line}")?;
    }
    Ok(())
}

fn write_csv(path: &Path, rows: &[RawObservation]) -> Result<()> {
    let file = File::create(path)?;
    let mut writer = Writer::from_writer(file);
    for row in rows {
        writer.serialize(row)?;
    }
    writer.flush()?;
    Ok(())
}