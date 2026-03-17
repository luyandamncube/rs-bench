// apps\bmrun\src\main.rs
use anyhow::{Context, Result};
use bm_engine::adapter::EngineAdapter;
use bm_engine::error::EngineError;
use bm_engine::request::*;
use bm_engine::response::*;
use bm_engine_datafusion::DataFusionAdapter;
use bm_engine_duckdb::DuckDbAdapter;
use bm_engine_clickhouse::ClickHouseAdapter;
use bm_runner::run_benchmark;
use chrono::Utc;
use clap::{Parser, Subcommand};
use std::fs;

#[derive(Parser)]
#[command(name = "bmrun")]
#[command(about = "Benchmark runner")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Run {
        #[arg(long)]
        config: String,
    },
}

struct MockAdapter;

impl EngineAdapter for MockAdapter {
    fn name(&self) -> &'static str {
        "mock"
    }

    fn bootstrap(&mut self, req: BootstrapRequest) -> Result<BootstrapResponse, EngineError> {
        Ok(BootstrapResponse {
            engine_name: "mock".into(),
            engine_version: "0.0.1".into(),
            adapter_version: "0.0.1".into(),
            started_service: false,
            notes: vec![format!("run_id={}", req.run_id)],
        })
    }

    fn prepare_dataset(
        &mut self,
        _req: PrepareDatasetRequest,
    ) -> Result<PrepareDatasetResponse, EngineError> {
        Ok(PrepareDatasetResponse {
            setup_started_at: Utc::now(),
            setup_elapsed_ms: 10,
            registered_objects: vec!["mock_table".into()],
            notes: vec![],
        })
    }

    fn run_query(&mut self, req: RunQueryRequest) -> Result<QueryExecutionResult, EngineError> {
        Ok(QueryExecutionResult {
            started_at: Utc::now(),
            elapsed_ms: 50 + req.repetition as u64,
            success: true,
            row_count: Some(1),
            error_message: None,
            plan_text: None,
            diagnostics_json: None,
        })
    }

    fn cleanup(&mut self, _req: CleanupRequest) -> Result<CleanupResponse, EngineError> {
        Ok(CleanupResponse {
            success: true,
            notes: vec![],
        })
    }

    fn collect_metadata(&self) -> EngineMetadata {
        EngineMetadata {
            engine_name: "mock".into(),
            engine_version: "0.0.1".into(),
            adapter_version: "0.0.1".into(),
            execution_mode: "mock".into(),
            file_format: None,
            table_mode: None,
            notes: vec![],
        }
    }
}

fn read_engine_name(config_path: &str) -> Result<String> {
    let raw = fs::read_to_string(config_path)
        .with_context(|| format!("failed to read run config: {config_path}"))?;
    let value: toml::Value = toml::from_str(&raw).context("failed to parse run config TOML")?;
    let engine = value
        .get("engine")
        .and_then(|v| v.as_str())
        .context("run config missing string field: engine")?;
    Ok(engine.to_string())
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Run { config } => {
            let engine = read_engine_name(&config)?;

            let results_dir = match engine.as_str() {
                "mock" => {
                    let mut adapter = MockAdapter;
                    run_benchmark(&config, &mut adapter)?
                }
                "datafusion" => {
                    let mut adapter = DataFusionAdapter::new();
                    run_benchmark(&config, &mut adapter)?
                }
                "duckdb" => {
                    let mut adapter = DuckDbAdapter::new();
                    run_benchmark(&config, &mut adapter)?
                }
                "clickhouse" => {
                    let mut adapter = ClickHouseAdapter::new();
                    run_benchmark(&config, &mut adapter)?
                }
                other => anyhow::bail!("unsupported engine: {other}"),
            };

            println!("Benchmark run complete");
            println!("Results: {}", results_dir.display());
        }
    }

    Ok(())
}