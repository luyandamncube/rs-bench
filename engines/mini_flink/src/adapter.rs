use crate::graph::{compile_pipeline, render_pipeline, CompiledPipeline};
use crate::runtime::{execute_runtime, MiniFlinkEngineConfig};
use bm_engine::error::EngineError;
use bm_engine::streaming::{
    StreamingBootstrapRequest, StreamingBootstrapResponse, StreamingCleanupRequest,
    StreamingCleanupResponse, StreamingEngineAdapter, StreamingPrepareScenarioRequest,
    StreamingPrepareScenarioResponse, StreamingRunRequest, StreamingRunResult,
};
use bm_schema::streaming::StreamingWorkloadDefinition;
use chrono::Utc;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;
use tokio::runtime::Runtime;

pub struct MiniFlinkAdapter {
    run_id: Option<String>,
    config: Option<MiniFlinkEngineConfig>,
    workload: Option<StreamingWorkloadDefinition>,
    pipeline: Option<CompiledPipeline>,
    sink_output_dir: Option<PathBuf>,
}

impl MiniFlinkAdapter {
    pub fn new() -> Self {
        Self {
            run_id: None,
            config: None,
            workload: None,
            pipeline: None,
            sink_output_dir: None,
        }
    }

    fn runtime() -> Result<Runtime, EngineError> {
        Runtime::new()
            .map_err(|e| EngineError::Other(format!("failed to create tokio runtime: {e}")))
    }

    pub fn load_engine_config() -> Result<MiniFlinkEngineConfig, EngineError> {
        let raw = fs::read_to_string("configs/engines/mini_flink.toml").map_err(|e| {
            EngineError::Bootstrap(format!("failed to read mini_flink config: {e}"))
        })?;
        toml::from_str(&raw)
            .map_err(|e| EngineError::Bootstrap(format!("failed to parse mini_flink config: {e}")))
    }

    fn workload(&self) -> Result<StreamingWorkloadDefinition, EngineError> {
        self.workload
            .clone()
            .ok_or_else(|| EngineError::Prepare("mini_flink workload not prepared".into()))
    }

    fn config(&self) -> Result<MiniFlinkEngineConfig, EngineError> {
        self.config
            .clone()
            .ok_or_else(|| EngineError::Prepare("mini_flink config not loaded".into()))
    }
}

impl Default for MiniFlinkAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamingEngineAdapter for MiniFlinkAdapter {
    fn name(&self) -> &'static str {
        "mini_flink"
    }

    fn bootstrap_streaming(
        &mut self,
        req: StreamingBootstrapRequest,
    ) -> Result<StreamingBootstrapResponse, EngineError> {
        let config = Self::load_engine_config()?;
        self.run_id = Some(req.run_id.clone());
        self.config = Some(config.clone());

        Ok(StreamingBootstrapResponse {
            engine_name: self.name().into(),
            engine_version: env!("CARGO_PKG_VERSION").into(),
            adapter_version: env!("CARGO_PKG_VERSION").into(),
            started_service: false,
            notes: vec![
                format!("run_id={}", req.run_id),
                format!("keyed_parallelism={}", config.keyed_parallelism),
                format!("channel_capacity={}", config.channel_capacity),
                format!("sink_mode={}", config.sink_mode),
            ],
        })
    }

    fn prepare_streaming_scenario(
        &mut self,
        req: StreamingPrepareScenarioRequest,
    ) -> Result<StreamingPrepareScenarioResponse, EngineError> {
        let started = Utc::now();
        let t0 = Instant::now();
        let raw = fs::read_to_string(&req.workload_path)
            .map_err(|e| EngineError::Prepare(format!("failed to read workload: {e}")))?;
        let workload: StreamingWorkloadDefinition = serde_yaml::from_str(&raw)
            .map_err(|e| EngineError::Prepare(format!("failed to parse workload: {e}")))?;

        let run_id = self
            .run_id
            .clone()
            .ok_or_else(|| EngineError::Prepare("mini_flink run_id missing".into()))?;
        let config = self.config()?;
        let sink_output_dir = PathBuf::from(&config.sink_subdir).join(&run_id);
        let pipeline = compile_pipeline(&workload, &config.sink_mode)
            .map_err(|e| EngineError::Prepare(format!("failed to compile pipeline: {e}")))?;

        fs::create_dir_all(&sink_output_dir)
            .map_err(|e| EngineError::Prepare(format!("failed to create sink dir: {e}")))?;

        let run_results_dir = PathBuf::from("results")
            .join("streaming_runs")
            .join(&run_id);
        fs::create_dir_all(&run_results_dir)
            .map_err(|e| EngineError::Prepare(format!("failed to create results dir: {e}")))?;
        fs::write(
            run_results_dir.join("pipeline_graph.json"),
            serde_json::to_vec_pretty(&pipeline.pipeline).map_err(|e| {
                EngineError::Prepare(format!("failed to serialize pipeline graph: {e}"))
            })?,
        )
        .map_err(|e| EngineError::Prepare(format!("failed to write pipeline graph json: {e}")))?;
        fs::write(
            run_results_dir.join("pipeline_validation.json"),
            serde_json::to_vec_pretty(&pipeline.validation_report).map_err(|e| {
                EngineError::Prepare(format!("failed to serialize pipeline validation: {e}"))
            })?,
        )
        .map_err(|e| {
            EngineError::Prepare(format!("failed to write pipeline validation json: {e}"))
        })?;
        fs::write(
            run_results_dir.join("pipeline_graph.txt"),
            render_pipeline(&pipeline.pipeline),
        )
        .map_err(|e| EngineError::Prepare(format!("failed to write pipeline graph text: {e}")))?;

        self.workload = Some(workload);
        self.pipeline = Some(pipeline.clone());
        self.sink_output_dir = Some(sink_output_dir.clone());

        Ok(StreamingPrepareScenarioResponse {
            setup_started_at: started,
            setup_elapsed_ms: t0.elapsed().as_millis() as u64,
            registered_objects: vec![
                "source".into(),
                "router".into(),
                "keyed_window_workers".into(),
                format!("{}_sink", pipeline.sink_mode),
            ],
            notes: vec![
                format!("workload_name={}", req.workload_name),
                format!("duration_secs={}", req.duration_secs),
                format!("warmup_secs={}", req.warmup_secs),
                format!("event_rate_per_sec={}", req.event_rate_per_sec),
                format!("sink_output_dir={}", sink_output_dir.display()),
                format!(
                    "operator_sequence={}",
                    pipeline.validation_report.operator_sequence.join(" -> ")
                ),
                format!(
                    "pipeline={}",
                    render_pipeline(&pipeline.pipeline).replace('\n', " -> ")
                ),
            ],
        })
    }

    fn run_streaming(
        &mut self,
        req: StreamingRunRequest,
    ) -> Result<StreamingRunResult, EngineError> {
        let started = Utc::now();
        let startup_t0 = Instant::now();
        let workload = self.workload()?;
        let pipeline = self
            .pipeline
            .clone()
            .ok_or_else(|| EngineError::Prepare("mini_flink pipeline missing".into()))?;
        let config = self.config()?;
        let sink_output_dir = self
            .sink_output_dir
            .clone()
            .ok_or_else(|| EngineError::Prepare("mini_flink sink output dir missing".into()))?;
        let sink_output_path = if pipeline.sink_mode == "stdout" {
            PathBuf::from("stdout")
        } else {
            sink_output_dir.join(format!("window_outputs_rep{}.jsonl", req.repetition))
        };

        let startup_time_ms = startup_t0.elapsed().as_millis() as u64;
        let runtime = Self::runtime()?;
        let outcome = runtime.block_on(execute_runtime(
            config,
            workload,
            pipeline,
            &req,
            sink_output_path,
        ))?;

        Ok(StreamingRunResult {
            started_at: started,
            startup_time_ms,
            throughput_events_per_sec: outcome.throughput_events_per_sec,
            latency_p50_ms: outcome.latency_p50_ms,
            latency_p95_ms: outcome.latency_p95_ms,
            latency_p99_ms: outcome.latency_p99_ms,
            processed_events: outcome.processed_events,
            dropped_events: outcome.dropped_events,
            failed_events: outcome.failed_events,
            records_emitted: outcome.records_emitted,
            emitted_windows: outcome.emitted_windows,
            sink_output_path: Some(outcome.sink_output_path),
            correctness_passed: outcome.correctness_passed,
            correctness_message: outcome.correctness_message,
            success: true,
            error_message: None,
        })
    }

    fn cleanup_streaming(
        &mut self,
        _req: StreamingCleanupRequest,
    ) -> Result<StreamingCleanupResponse, EngineError> {
        self.workload = None;
        self.pipeline = None;
        self.sink_output_dir = None;

        Ok(StreamingCleanupResponse {
            success: true,
            notes: vec![],
        })
    }
}
