use crate::graph::{build_runtime_graph, CompiledPipeline};
use crate::router::run_router;
use crate::sink::{
    run_file_sink, run_live_stdout_sink, run_stdout_sink, validate_sink_output, validate_sink_rows,
};
use crate::source::{run_live_tcp_client_source, run_live_tcp_source, run_source};
use crate::worker::run_worker;
use bm_engine::error::EngineError;
use bm_engine::streaming::StreamingRunRequest;
use bm_schema::streaming::StreamingWorkloadDefinition;
use serde::Deserialize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[derive(Debug, Clone, Deserialize)]
pub struct MiniFlinkEngineConfig {
    pub keyed_parallelism: usize,
    pub channel_capacity: usize,
    pub sink_mode: String,
    pub sink_subdir: String,
}

pub struct RuntimeMetrics {
    processed_events: AtomicU64,
    dropped_events: AtomicU64,
    records_emitted: AtomicU64,
    emitted_windows: AtomicU64,
    latencies_micros: Mutex<Vec<u64>>,
}

impl RuntimeMetrics {
    pub fn new() -> Self {
        Self {
            processed_events: AtomicU64::new(0),
            dropped_events: AtomicU64::new(0),
            records_emitted: AtomicU64::new(0),
            emitted_windows: AtomicU64::new(0),
            latencies_micros: Mutex::new(Vec::new()),
        }
    }

    pub fn increment_processed(&self) {
        self.processed_events.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_dropped(&self) {
        self.dropped_events.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_records_emitted(&self) {
        self.records_emitted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_emitted_windows(&self) {
        self.emitted_windows.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_latency(&self, value_micros: u64) {
        let mut guard = self.latencies_micros.lock().unwrap();
        guard.push(value_micros);
    }

    pub fn processed_events(&self) -> u64 {
        self.processed_events.load(Ordering::Relaxed)
    }

    pub fn dropped_events(&self) -> u64 {
        self.dropped_events.load(Ordering::Relaxed)
    }

    pub fn records_emitted(&self) -> u64 {
        self.records_emitted.load(Ordering::Relaxed)
    }

    pub fn emitted_windows(&self) -> u64 {
        self.emitted_windows.load(Ordering::Relaxed)
    }

    pub fn latency_percentiles_ms(&self) -> (f64, f64, f64) {
        let mut values = self.latencies_micros.lock().unwrap().clone();
        if values.is_empty() {
            return (0.0, 0.0, 0.0);
        }

        values.sort_unstable();
        (
            percentile_ms(&values, 0.50),
            percentile_ms(&values, 0.95),
            percentile_ms(&values, 0.99),
        )
    }
}

pub struct RuntimeOutcome {
    pub throughput_events_per_sec: f64,
    pub latency_p50_ms: f64,
    pub latency_p95_ms: f64,
    pub latency_p99_ms: f64,
    pub processed_events: u64,
    pub dropped_events: u64,
    pub failed_events: u64,
    pub records_emitted: u64,
    pub emitted_windows: u64,
    pub sink_output_path: String,
    pub correctness_passed: bool,
    pub correctness_message: Option<String>,
}

pub struct LiveRuntimeSummary {
    pub processed_events: u64,
    pub dropped_events: u64,
    pub records_emitted: u64,
    pub emitted_windows: u64,
}

#[derive(Debug, Clone)]
pub enum LiveSourceMode {
    Listen(String),
    Connect(String),
}

pub async fn execute_runtime(
    engine_config: MiniFlinkEngineConfig,
    workload: StreamingWorkloadDefinition,
    pipeline: CompiledPipeline,
    req: &StreamingRunRequest,
    sink_output_path: PathBuf,
) -> Result<RuntimeOutcome, EngineError> {
    if pipeline.sink_mode != "file" && pipeline.sink_mode != "stdout" {
        return Err(EngineError::Prepare(format!(
            "mini_flink only supports sink_mode=file|stdout, got {}",
            pipeline.sink_mode
        )));
    }
    if engine_config.sink_mode != "file" && engine_config.sink_mode != "stdout" {
        return Err(EngineError::Prepare(format!(
            "mini_flink engine config only supports sink_mode=file|stdout, got {}",
            engine_config.sink_mode
        )));
    }
    if let Some(map_mode) = &pipeline.map_mode {
        if map_mode != "synthetic_clickstream_v1" {
            return Err(EngineError::Prepare(format!(
                "mini_flink only supports map mode synthetic_clickstream_v1, got {}",
                map_mode
            )));
        }
    }

    let expected_total_events: u64 = workload
        .expected_output
        .expected_group_totals
        .iter()
        .map(|group| group.event_count)
        .sum();
    let configured_total_events = req.event_rate_per_sec.saturating_mul(req.duration_secs);
    if expected_total_events != configured_total_events {
        return Err(EngineError::Query(format!(
            "workload expects {} passing events but config requests {}",
            expected_total_events, configured_total_events
        )));
    }

    let metrics = Arc::new(RuntimeMetrics::new());
    let graph = build_runtime_graph(
        engine_config.keyed_parallelism.max(1),
        engine_config.channel_capacity.max(1),
    );
    let mut handles = Vec::new();
    let runtime_started_at = Instant::now();

    let crate::graph::RuntimeGraph {
        source_tx,
        source_rx,
        worker_txs,
        worker_rxs,
        sink_tx,
        sink_rx,
    } = graph;

    let sink_mode = pipeline.sink_mode.clone();
    let sink_path_clone = sink_output_path.clone();
    let sink_handle = tokio::spawn(async move {
        match sink_mode.as_str() {
            "file" => run_file_sink(sink_rx, sink_path_clone).await,
            "stdout" => run_stdout_sink(sink_rx).await,
            other => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("unsupported sink mode {}", other),
            )),
        }
    });

    for (partition, rx) in worker_rxs.into_iter().enumerate() {
        let sink_tx = sink_tx.clone();
        let metrics = Arc::clone(&metrics);
        let window_size_ms = pipeline.window_size_ms;
        let aggregate_functions = pipeline.aggregate_functions.clone();
        let runtime_started_at = runtime_started_at;
        handles.push(tokio::spawn(async move {
            run_worker(
                partition,
                rx,
                sink_tx,
                window_size_ms,
                aggregate_functions,
                runtime_started_at,
                metrics,
            )
            .await;
        }));
    }
    drop(sink_tx);

    let router_metrics = Arc::clone(&metrics);
    let filter = pipeline
        .filter
        .as_ref()
        .map(|filter| bm_schema::streaming::StreamingFilterSpec {
            field: filter.field.clone(),
            op: Some(filter.op.clone()),
            equals: filter.value.clone(),
            values: if filter.values.is_empty() {
                None
            } else {
                Some(filter.values.clone())
            },
        });
    let router_handle = tokio::spawn(async move {
        run_router(source_rx, worker_txs, filter, router_metrics).await;
    });
    handles.push(router_handle);

    let expected = workload.expected_output.expected_group_totals.clone();
    let source_event_type = pipeline
        .filter
        .as_ref()
        .filter(|spec| spec.field == "event_type" && spec.op == "eq")
        .and_then(|spec| spec.value.clone());
    let event_rate_per_sec = req.event_rate_per_sec;
    let duration_secs = req.duration_secs;
    let start = Instant::now();
    let source_handle = tokio::spawn(async move {
        run_source(
            source_tx,
            &expected,
            event_rate_per_sec,
            duration_secs,
            source_event_type,
        )
        .await;
    });
    handles.push(source_handle);

    for handle in handles {
        let _ = handle.await;
    }

    let sink_rows = sink_handle
        .await
        .map_err(|e| EngineError::Query(format!("sink task failed: {e}")))?
        .map_err(|e| EngineError::Query(format!("sink execution failed: {e}")))?;

    let run_elapsed = start.elapsed();
    let correctness = if pipeline.sink_mode == "file" {
        validate_sink_output(
            &sink_output_path,
            &workload.expected_output.expected_group_totals,
        )
    } else {
        validate_sink_rows(&sink_rows, &workload.expected_output.expected_group_totals)
    };
    let (latency_p50_ms, latency_p95_ms, latency_p99_ms) = metrics.latency_percentiles_ms();

    Ok(RuntimeOutcome {
        throughput_events_per_sec: if run_elapsed.as_secs_f64() > 0.0 {
            metrics.processed_events() as f64 / run_elapsed.as_secs_f64()
        } else {
            metrics.processed_events() as f64
        },
        latency_p50_ms,
        latency_p95_ms,
        latency_p99_ms,
        processed_events: metrics.processed_events(),
        dropped_events: metrics.dropped_events(),
        failed_events: 0,
        records_emitted: metrics.records_emitted(),
        emitted_windows: metrics.emitted_windows(),
        sink_output_path: if pipeline.sink_mode == "stdout" {
            "stdout".to_string()
        } else {
            sink_output_path.to_string_lossy().to_string()
        },
        correctness_passed: correctness.is_ok(),
        correctness_message: correctness.err(),
    })
}

fn percentile_ms(sorted_values_micros: &[u64], percentile: f64) -> f64 {
    let idx = ((sorted_values_micros.len() - 1) as f64 * percentile).round() as usize;
    sorted_values_micros[idx] as f64 / 1000.0
}

pub async fn execute_live_runtime(
    engine_config: MiniFlinkEngineConfig,
    pipeline: CompiledPipeline,
    source_mode: LiveSourceMode,
) -> Result<LiveRuntimeSummary, EngineError> {
    if pipeline.sink_mode != "stdout" {
        return Err(EngineError::Prepare(format!(
            "live mini_flink runtime currently requires sink_mode=stdout, got {}",
            pipeline.sink_mode
        )));
    }
    if let Some(map_mode) = &pipeline.map_mode {
        if map_mode != "synthetic_clickstream_v1" {
            return Err(EngineError::Prepare(format!(
                "mini_flink live runtime only supports map mode synthetic_clickstream_v1, got {}",
                map_mode
            )));
        }
    }

    let metrics = Arc::new(RuntimeMetrics::new());
    let graph = build_runtime_graph(
        engine_config.keyed_parallelism.max(1),
        engine_config.channel_capacity.max(1),
    );
    let runtime_started_at = Instant::now();

    let crate::graph::RuntimeGraph {
        source_tx,
        source_rx,
        worker_txs,
        worker_rxs,
        sink_tx,
        sink_rx,
    } = graph;

    let sink_handle = tokio::spawn(async move { run_live_stdout_sink(sink_rx).await });

    let mut handles = Vec::new();
    for (partition, rx) in worker_rxs.into_iter().enumerate() {
        let sink_tx = sink_tx.clone();
        let metrics = Arc::clone(&metrics);
        let window_size_ms = pipeline.window_size_ms;
        let aggregate_functions = pipeline.aggregate_functions.clone();
        handles.push(tokio::spawn(async move {
            run_worker(
                partition,
                rx,
                sink_tx,
                window_size_ms,
                aggregate_functions,
                runtime_started_at,
                metrics,
            )
            .await;
        }));
    }
    drop(sink_tx);

    let router_metrics = Arc::clone(&metrics);
    let filter = pipeline
        .filter
        .as_ref()
        .map(|filter| bm_schema::streaming::StreamingFilterSpec {
            field: filter.field.clone(),
            op: Some(filter.op.clone()),
            equals: filter.value.clone(),
            values: if filter.values.is_empty() {
                None
            } else {
                Some(filter.values.clone())
            },
        });
    let router_handle = tokio::spawn(async move {
        run_router(source_rx, worker_txs, filter, router_metrics).await;
    });

    let key_field = pipeline.validation_report.key_field.clone();
    let mut source_handle = tokio::spawn(async move {
        match source_mode {
            LiveSourceMode::Listen(listen_addr) => {
                run_live_tcp_source(&listen_addr, &key_field, source_tx).await
            }
            LiveSourceMode::Connect(connect_addr) => {
                run_live_tcp_client_source(&connect_addr, &key_field, source_tx).await
            }
        }
    });

    tokio::select! {
        result = &mut source_handle => {
            result
                .map_err(|error| EngineError::Query(format!("live source task failed: {error}")))?
                .map_err(|error| EngineError::Query(format!("live source failed: {error}")))?;
        }
        _ = tokio::signal::ctrl_c() => {
            println!("mini_flink live runtime received Ctrl-C, shutting down");
            source_handle.abort();
            let _ = source_handle.await;
        }
    }

    let _ = router_handle.await;
    for handle in handles {
        let _ = handle.await;
    }
    let _ = sink_handle
        .await
        .map_err(|error| EngineError::Query(format!("live sink task failed: {error}")))?
        .map_err(|error| EngineError::Query(format!("live sink execution failed: {error}")))?;

    Ok(LiveRuntimeSummary {
        processed_events: metrics.processed_events(),
        dropped_events: metrics.dropped_events(),
        records_emitted: metrics.records_emitted(),
        emitted_windows: metrics.emitted_windows(),
    })
}
