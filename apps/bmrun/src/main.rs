// apps\bmrun\src\main.rs
mod streaming_graphs;

use anyhow::{Context, Result};
use bm_engine::adapter::EngineAdapter;
use bm_engine::error::EngineError;
use bm_engine::request::*;
use bm_engine::response::*;
use bm_engine_clickhouse::ClickHouseAdapter;
use bm_engine_datafusion::DataFusionAdapter;
use bm_engine_duckdb::DuckDbAdapter;
use bm_engine_mini_flink::{
    compile_pipeline, execute_live_runtime, LiveSourceMode, MiniFlinkAdapter, MiniFlinkGraph,
};
use bm_engine_polars::PolarsAdapter;
use bm_engine_spark::SparkAdapter;
use bm_engine_stream_local::StreamLocalAdapter;
use bm_runner::{run_benchmark, run_streaming_benchmark};
use chrono::Utc;
use clap::{Parser, Subcommand};
use std::fs;
use std::path::PathBuf;
use tokio::runtime::Runtime;

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
    RunStreaming {
        #[arg(long)]
        config: String,
    },
    ScaffoldStreamingPipeline {
        #[arg(long)]
        output: String,
        #[arg(long)]
        name: String,
        #[arg(long, default_value = "clickstream")]
        source_family: String,
        #[arg(long, default_value = "device_type")]
        key_field: String,
        #[arg(long, default_value = "synthetic_clickstream_v1")]
        map_mode: String,
        #[arg(long)]
        filter_field: Option<String>,
        #[arg(long, default_value = "eq")]
        filter_op: String,
        #[arg(long)]
        filter_equals: Option<String>,
        #[arg(long, value_delimiter = ',')]
        filter_values: Vec<String>,
        #[arg(long, default_value_t = 3)]
        window_secs: u64,
        #[arg(long, value_delimiter = ',', default_value = "count,sum")]
        aggregate_functions: Vec<String>,
        #[arg(long, default_value = "event_count")]
        count_as: String,
        #[arg(long, default_value = "value")]
        sum_field: String,
        #[arg(long, default_value = "value_sum")]
        sum_as: String,
    },
    ScaffoldStreamingExample {
        #[arg(long)]
        example: String,
        #[arg(long)]
        output: String,
    },
    ListStreamingGraphs,
    RenderStreamingGraph {
        graph: String,
    },
    NewStreamingGraph {
        name: String,
        #[arg(long)]
        description: Option<String>,
    },
    RemoveStreamingGraph {
        graph: String,
    },
    ScaffoldStreamingGraph {
        graph: String,
        #[arg(long)]
        output: Option<String>,
    },
    RunStreamingGraph {
        graph: String,
        #[arg(long)]
        config: Option<String>,
        #[arg(long)]
        output: Option<String>,
    },
    RunLiveStreamingGraph {
        graph: String,
        #[arg(long, conflicts_with = "connect")]
        listen: Option<String>,
        #[arg(long, conflicts_with = "listen")]
        connect: Option<String>,
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

fn run_streaming_from_config(config: &str) -> Result<PathBuf> {
    let engine = read_engine_name(config)?;

    match engine.as_str() {
        "stream_local" => {
            let mut adapter = StreamLocalAdapter::new();
            run_streaming_benchmark(config, &mut adapter)
        }
        "mini_flink" => {
            let mut adapter = MiniFlinkAdapter::new();
            run_streaming_benchmark(config, &mut adapter)
        }
        other => anyhow::bail!("unsupported streaming engine: {other}"),
    }
}

fn default_graph_output(graph: &str) -> Result<String> {
    Ok(streaming_graphs::resolve_graph_spec(graph)?
        .default_output
        .to_string())
}

fn default_graph_config(graph: &str) -> Result<String> {
    Ok(streaming_graphs::resolve_graph_spec(graph)?
        .default_config
        .to_string())
}

fn write_temp_graph_config(config: &str, workload_output: &str) -> Result<PathBuf> {
    let raw = fs::read_to_string(config)
        .with_context(|| format!("failed to read streaming run config: {config}"))?;
    let mut value: toml::Value =
        toml::from_str(&raw).context("failed to parse streaming run config TOML")?;
    value["workload_path"] = toml::Value::String(workload_output.to_string());

    let temp_path = std::env::temp_dir().join(format!(
        "bmrun_streaming_graph_{}.toml",
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    fs::write(
        &temp_path,
        toml::to_string_pretty(&value).context("failed to serialize temp graph run config")?,
    )
    .with_context(|| {
        format!(
            "failed to write temp graph run config: {}",
            temp_path.display()
        )
    })?;

    Ok(temp_path)
}

fn run_live_streaming_graph(
    graph: &str,
    listen: Option<&str>,
    connect: Option<&str>,
) -> Result<()> {
    let spec = streaming_graphs::resolve_graph_spec(graph)?;
    let workload = spec.build_graph().build_workload();
    let pipeline = compile_pipeline(&workload, "stdout")
        .map_err(|error| anyhow::anyhow!("failed to compile live graph: {error}"))?;
    let engine_config = MiniFlinkAdapter::load_engine_config()
        .map_err(|error| anyhow::anyhow!("failed to load mini_flink engine config: {error}"))?;
    let source_mode = match (listen, connect) {
        (Some(listen), None) => LiveSourceMode::Listen(listen.to_string()),
        (None, Some(connect)) => LiveSourceMode::Connect(connect.to_string()),
        (None, None) => LiveSourceMode::Listen("127.0.0.1:7001".to_string()),
        (Some(_), Some(_)) => anyhow::bail!("use either --listen or --connect, not both"),
    };

    println!("Starting live streaming graph");
    println!("Graph: {}", spec.name);
    match &source_mode {
        LiveSourceMode::Listen(listen_addr) => println!("Listen: {}", listen_addr),
        LiveSourceMode::Connect(connect_addr) => println!("Connect: {}", connect_addr),
    }
    println!("Press Ctrl-C to stop the processor.");

    let runtime = Runtime::new().context("failed to create tokio runtime for live graph")?;
    let summary = runtime
        .block_on(execute_live_runtime(engine_config, pipeline, source_mode))
        .map_err(|error| anyhow::anyhow!("live streaming graph failed: {error}"))?;

    println!("Live streaming graph stopped");
    println!("Processed events: {}", summary.processed_events);
    println!("Dropped events: {}", summary.dropped_events);
    println!("Records emitted: {}", summary.records_emitted);
    println!("Emitted windows: {}", summary.emitted_windows);

    Ok(())
}

fn sanitize_graph_module_name(name: &str) -> Result<String> {
    let sanitized = name.trim().to_lowercase().replace('-', "_");
    if sanitized.is_empty() {
        anyhow::bail!("graph name cannot be empty");
    }
    if !sanitized
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
    {
        anyhow::bail!(
            "graph name must contain only lowercase letters, digits, underscores, or hyphens"
        );
    }
    if sanitized.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        anyhow::bail!("graph name cannot start with a digit");
    }
    Ok(sanitized)
}

fn create_streaming_graph_scaffold(name: &str, description: Option<&str>) -> Result<()> {
    let module_name = sanitize_graph_module_name(name)?;
    let spec_name = module_name.clone();
    let workload_name = format!("mini_flink_{module_name}");
    let description = description
        .map(ToString::to_string)
        .unwrap_or_else(|| format!("Starter mini_flink graph for {module_name}."));
    let default_output = format!("workloads/streaming/jobs/{module_name}.yaml");
    let default_config = format!("configs/streaming/mini_flink_{module_name}.toml");

    let graph_dir = PathBuf::from("apps/bmrun/src/streaming_graphs");
    let graph_file = graph_dir.join(format!("{module_name}.rs"));
    if graph_file.exists() {
        anyhow::bail!("graph module already exists: {}", graph_file.display());
    }

    let workload_path = PathBuf::from(&default_output);
    let config_path = PathBuf::from(&default_config);
    if workload_path.exists() {
        anyhow::bail!(
            "default workload already exists: {}",
            workload_path.display()
        );
    }
    if config_path.exists() {
        anyhow::bail!("default config already exists: {}", config_path.display());
    }

    let module_source = format!(
        r#"use super::StreamingGraphSpec;
use bm_engine_mini_flink::MiniFlinkGraph;

pub const SPEC: StreamingGraphSpec = StreamingGraphSpec {{
    name: "{spec_name}",
    description: "{description}",
    default_output: "{default_output}",
    default_config: "{default_config}",
    build,
}};

pub fn build() -> MiniFlinkGraph {{
    MiniFlinkGraph::source("clickstream")
        .named("{workload_name}")
        .family("clickstream_streaming_demo")
        .description("{description}")
        .map("synthetic_clickstream_v1")
        .filter_eq("event_type", "page_view")
        .key_by("device_type")
        .window_tumbling_secs(1)
        .aggregate_count_sum("event_count", "value", "value_sum")
        .sink_stdout()
        .expected_group_totals_from_tuples(vec![
            ("mobile", 2, 6),
            ("desktop", 2, 8),
            ("tablet", 2, 4),
            ("tv", 2, 10),
        ])
}}

#[cfg(test)]
mod tests {{
    use super::SPEC;

    #[test]
    fn {module_name}_spec_smoke_test() {{
        super::super::assert_graph_spec_smoke(&SPEC);
    }}
}}
"#
    );

    let workload_source = format!(
        r#"name: {workload_name}
family: clickstream_streaming_demo
description: {description}
source:
  family: clickstream
  event_time_field: event_time
  key_field: device_type
parse:
  mode: synthetic_clickstream_v1
filter:
  field: event_type
  op: eq
  equals: page_view
schema:
  - name: event_time
    data_type: timestamp_ms
  - name: user_id
    data_type: u64
  - name: session_id
    data_type: u64
  - name: device_type
    data_type: string
  - name: event_type
    data_type: string
  - name: value
    data_type: u64
scenario:
  name: {workload_name}_pipeline
  operation: tumbling_window_grouped_sum
  group_by:
    - device_type
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
    op: eq
    equals: page_view
  - kind: key_by
    field: device_type
  - kind: window
    size_secs: 1
  - kind: aggregate
    count_as: event_count
    sum_field: value
    sum_as: value_sum
    aggregate_functions:
      - count
      - sum
  - kind: sink
    sink_mode: stdout
expected_output:
  aggregate_by: device_type
  value_field: value
  expected_group_totals:
    - key: mobile
      event_count: 2
      value_sum: 6
    - key: desktop
      event_count: 2
      value_sum: 8
    - key: tablet
      event_count: 2
      value_sum: 4
    - key: tv
      event_count: 2
      value_sum: 10
"#
    );

    let config_source = format!(
        r#"run_name = "{workload_name}"
engine = "mini_flink"
workload_path = "{default_output}"
duration_secs = 1
warmup_secs = 0
repetitions = 1
event_rate_per_sec = 8
seed = 1
correctness_mode = "baseline"
"#
    );

    fs::create_dir_all(&graph_dir)?;
    if let Some(parent) = workload_path.parent() {
        fs::create_dir_all(parent)?;
    }
    if let Some(parent) = config_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&graph_file, module_source)?;
    fs::write(&workload_path, workload_source)?;
    fs::write(&config_path, config_source)?;

    let mod_rs_path = graph_dir.join("mod.rs");
    let mod_rs = fs::read_to_string(&mod_rs_path)
        .with_context(|| format!("failed to read {}", mod_rs_path.display()))?;
    let mod_decl = format!("mod {module_name};");
    if mod_rs.contains(&mod_decl) {
        anyhow::bail!("registry already contains module {}", module_name);
    }
    let updated_mod_rs = mod_rs
        .replacen(
            "mod terminal_demo;",
            &format!("{mod_decl}\nmod terminal_demo;"),
            1,
        )
        .replacen(
            "const GRAPH_SPECS: &[StreamingGraphSpec] = &[",
            &format!("const GRAPH_SPECS: &[StreamingGraphSpec] = &[{module_name}::SPEC, "),
            1,
        );
    fs::write(&mod_rs_path, updated_mod_rs)?;

    println!("Created streaming graph scaffold");
    println!("Graph module: {}", graph_file.display());
    println!("Default workload: {}", workload_path.display());
    println!("Default config: {}", config_path.display());
    println!("Next step: rebuild bmrun, then run `cargo run -p bmrun -- render-streaming-graph --graph {module_name}`");

    Ok(())
}

fn remove_streaming_graph_scaffold(graph: &str) -> Result<()> {
    let spec = *streaming_graphs::resolve_graph_spec(graph)?;
    let module_name = sanitize_graph_module_name(graph)?;
    let graph_dir = PathBuf::from("apps/bmrun/src/streaming_graphs");
    let graph_file = graph_dir.join(format!("{module_name}.rs"));
    let workload_path = PathBuf::from(spec.default_output);
    let config_path = PathBuf::from(spec.default_config);
    let mod_rs_path = graph_dir.join("mod.rs");

    let mod_rs = fs::read_to_string(&mod_rs_path)
        .with_context(|| format!("failed to read {}", mod_rs_path.display()))?;
    let mod_decl = format!("mod {module_name};");
    if !mod_rs.contains(&mod_decl) {
        anyhow::bail!(
            "graph registry is missing module declaration for {}",
            module_name
        );
    }

    let spec_ref = format!("{module_name}::SPEC");
    let rewritten_lines = mod_rs
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            if trimmed == mod_decl {
                return None;
            }

            if trimmed.starts_with("const GRAPH_SPECS:") {
                let mut rewritten = line.replace(&format!("[{spec_ref}, "), "[");
                rewritten = rewritten.replace(&format!(", {spec_ref}]"), "]");
                rewritten = rewritten.replace(&format!("[{spec_ref}]"), "[]");
                rewritten = rewritten.replace(&format!("{spec_ref}, "), "");
                rewritten = rewritten.replace(&format!(", {spec_ref}"), "");
                rewritten = rewritten.replace(&spec_ref, "");
                rewritten = rewritten.replace("[,", "[");
                rewritten = rewritten.replace(", ]", " ]");
                return Some(rewritten);
            }

            if trimmed == format!("{spec_ref},") || trimmed == spec_ref {
                return None;
            }

            Some(line.to_string())
        })
        .collect::<Vec<_>>();
    let mut updated_mod_rs = rewritten_lines.join("\n");
    if !updated_mod_rs.ends_with('\n') {
        updated_mod_rs.push('\n');
    }

    fs::write(&mod_rs_path, updated_mod_rs)
        .with_context(|| format!("failed to write {}", mod_rs_path.display()))?;

    for path in [&graph_file, &workload_path, &config_path] {
        if path.exists() {
            fs::remove_file(path)
                .with_context(|| format!("failed to remove {}", path.display()))?;
        }
    }

    println!("Removed streaming graph");
    println!("Graph module: {}", graph_file.display());
    println!("Default workload: {}", workload_path.display());
    println!("Default config: {}", config_path.display());

    Ok(())
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
                "polars" => {
                    let mut adapter = PolarsAdapter::new();
                    run_benchmark(&config, &mut adapter)?
                }
                "spark" => {
                    let mut adapter = SparkAdapter::new();
                    run_benchmark(&config, &mut adapter)?
                }
                other => anyhow::bail!("unsupported engine: {other}"),
            };

            println!("Benchmark run complete");
            println!("Results: {}", results_dir.display());
        }
        Commands::RunStreaming { config } => {
            let results_dir = run_streaming_from_config(&config)?;

            println!("Streaming benchmark run complete");
            println!("Results: {}", results_dir.display());
        }
        Commands::ScaffoldStreamingPipeline {
            output,
            name,
            source_family,
            key_field,
            map_mode,
            filter_field,
            filter_op,
            filter_equals,
            filter_values,
            window_secs,
            aggregate_functions,
            count_as,
            sum_field,
            sum_as,
        } => {
            let mut builder = MiniFlinkGraph::source(source_family)
                .named(name.clone())
                .description(format!("Generated streaming workload for {}", name))
                .map(map_mode)
                .key_by(key_field)
                .window_tumbling_secs(window_secs)
                .aggregate(
                    aggregate_functions,
                    Some(count_as),
                    Some(sum_field),
                    Some(sum_as),
                )
                .sink_file();

            match (filter_field, filter_equals, filter_values.is_empty()) {
                (Some(field), Some(equals), true) => {
                    builder = match filter_op.as_str() {
                        "eq" => builder.filter_eq(field, equals),
                        "ne" => builder.filter_ne(field, equals),
                        "gt" => builder.filter_gt(field, equals),
                        "gte" => builder.filter_gte(field, equals),
                        "lt" => builder.filter_lt(field, equals),
                        "lte" => builder.filter_lte(field, equals),
                        other => anyhow::bail!("unsupported scalar filter op for scaffold: {other}"),
                    };
                }
                (Some(field), None, false) if filter_op == "in" => {
                    builder = builder.filter_in(field, filter_values);
                }
                (None, None, true) => {}
                _ => anyhow::bail!("filter config must be either --filter-field + --filter-equals, or --filter-field + --filter-values with --filter-op in"),
            }

            builder
                .write_yaml(&output)
                .map_err(|e| anyhow::anyhow!("failed to write workload yaml: {e}"))?;

            println!("Streaming workload scaffold written");
            println!("Output: {}", output);
        }
        Commands::ScaffoldStreamingExample { example, output } => {
            streaming_graphs::write_graph(&example, &output)?;
            println!("Streaming example scaffold written");
            println!("Example: {}", example);
            println!("Output: {}", output);
        }
        Commands::ListStreamingGraphs => {
            println!("Available streaming graphs:");
            for graph in streaming_graphs::all_graphs() {
                println!("- {}", graph.name);
                println!("  description: {}", graph.description);
                println!("  default workload: {}", graph.default_output);
                println!("  default config: {}", graph.default_config);
            }
        }
        Commands::RenderStreamingGraph { graph } => {
            let rendered = streaming_graphs::render_graph(&graph)?;
            println!("{rendered}");
        }
        Commands::NewStreamingGraph { name, description } => {
            create_streaming_graph_scaffold(&name, description.as_deref())?;
        }
        Commands::RemoveStreamingGraph { graph } => {
            remove_streaming_graph_scaffold(&graph)?;
        }
        Commands::ScaffoldStreamingGraph { graph, output } => {
            let output = match output {
                Some(output) => output,
                None => default_graph_output(&graph)?,
            };
            streaming_graphs::write_graph(&graph, &output)?;
            println!("Streaming graph scaffold written");
            println!("Graph: {}", graph);
            println!("Output: {}", output);
        }
        Commands::RunStreamingGraph {
            graph,
            config,
            output,
        } => {
            let output = match output {
                Some(output) => output,
                None => default_graph_output(&graph)?,
            };
            let config = match config {
                Some(config) => config,
                None => default_graph_config(&graph)?,
            };
            streaming_graphs::write_graph(&graph, &output)?;
            let temp_config = write_temp_graph_config(&config, &output)?;
            let results_dir = run_streaming_from_config(
                temp_config.to_str().context("invalid temp config path")?,
            )?;
            let _ = fs::remove_file(&temp_config);

            println!("Streaming graph run complete");
            println!("Graph: {}", graph);
            println!("Config: {}", config);
            println!("Workload: {}", output);
            println!("Results: {}", results_dir.display());
        }
        Commands::RunLiveStreamingGraph {
            graph,
            listen,
            connect,
        } => {
            run_live_streaming_graph(&graph, listen.as_deref(), connect.as_deref())?;
        }
    }

    Ok(())
}
