// engines\datafusion\src\lib.rs
use bm_engine::adapter::EngineAdapter;
use bm_engine::error::EngineError;
use bm_engine::request::{BootstrapRequest, CleanupRequest, PrepareDatasetRequest, RunQueryRequest};
use bm_engine::response::{
    BootstrapResponse, CleanupResponse, EngineMetadata, PrepareDatasetResponse, QueryExecutionResult,
};
use chrono::Utc;
use datafusion::prelude::{CsvReadOptions, ParquetReadOptions, SessionContext};
use std::time::Instant;
use tokio::runtime::Runtime;

pub struct DataFusionAdapter {
    ctx: Option<SessionContext>,
    table_name: String,
}

impl DataFusionAdapter {
    pub fn new() -> Self {
        Self {
            ctx: None,
            table_name: "benchmark_table".to_string(),
        }
    }

    fn runtime() -> Result<Runtime, EngineError> {
        Runtime::new().map_err(|e| EngineError::Other(format!("failed to create tokio runtime: {e}")))
    }
}

impl Default for DataFusionAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl EngineAdapter for DataFusionAdapter {
    fn name(&self) -> &'static str {
        "datafusion"
    }

    fn bootstrap(&mut self, _req: BootstrapRequest) -> Result<BootstrapResponse, EngineError> {
        self.ctx = Some(SessionContext::new());

        Ok(BootstrapResponse {
            engine_name: "datafusion".into(),
            engine_version: env!("CARGO_PKG_VERSION").into(),
            adapter_version: env!("CARGO_PKG_VERSION").into(),
            started_service: false,
            notes: vec!["in-process session".into()],
        })
    }

    fn prepare_dataset(
        &mut self,
        req: PrepareDatasetRequest,
    ) -> Result<PrepareDatasetResponse, EngineError> {
        let started = Utc::now();
        let t0 = Instant::now();

        let format = req.dataset_format.to_lowercase();

        let first_file = req
            .files
            .first()
            .ok_or_else(|| EngineError::Prepare("no dataset files supplied".into()))?
            .clone();

        let ctx = self
            .ctx
            .as_ref()
            .ok_or_else(|| EngineError::Prepare("session not bootstrapped".into()))?;

        let rt = Self::runtime()?;
        match format.as_str() {
            "csv" => {
                rt.block_on(async {
                    ctx.register_csv(&self.table_name, &first_file, CsvReadOptions::new())
                        .await
                        .map_err(|e| EngineError::Prepare(format!("register_csv failed: {e}")))
                })?;
            }
            "parquet" => {
                rt.block_on(async {
                    ctx.register_parquet(
                        &self.table_name,
                        &first_file,
                        ParquetReadOptions::default(),
                    )
                    .await
                    .map_err(|e| EngineError::Prepare(format!("register_parquet failed: {e}")))
                })?;
            }
            other => {
                return Err(EngineError::Prepare(format!(
                    "datafusion adapter currently supports only csv and parquet, got {}",
                    other
                )));
            }
        }

        Ok(PrepareDatasetResponse {
            setup_started_at: started,
            setup_elapsed_ms: t0.elapsed().as_millis() as u64,
            registered_objects: vec![self.table_name.clone()],
            notes: vec![format!("registered {} file: {}", format, first_file)],
        })
    }    

    fn run_query(&mut self, req: RunQueryRequest) -> Result<QueryExecutionResult, EngineError> {
        let started = Utc::now();
        let t0 = Instant::now();

        let ctx = self
            .ctx
            .as_ref()
            .ok_or_else(|| EngineError::Query("session not bootstrapped".into()))?;

        let rt = Self::runtime()?;
        let query = req.sql.replace("${table_name}", &self.table_name);

        let exec = rt.block_on(async {
            let df = ctx
                .sql(&query)
                .await
                .map_err(|e| EngineError::Query(format!("sql planning failed: {e}")))?;

            let batches = df
                .collect()
                .await
                .map_err(|e| EngineError::Query(format!("collect failed: {e}")))?;

            Ok::<usize, EngineError>(batches.len())
        });

        match exec {
            Ok(batch_count) => Ok(QueryExecutionResult {
                started_at: started,
                elapsed_ms: t0.elapsed().as_millis() as u64,
                success: true,
                row_count: Some(batch_count as u64),
                error_message: None,
                plan_text: None,
                diagnostics_json: Some(format!(r#"{{"batches": {batch_count}}}"#)),
            }),
            Err(err) => Ok(QueryExecutionResult {
                started_at: started,
                elapsed_ms: t0.elapsed().as_millis() as u64,
                success: false,
                row_count: None,
                error_message: Some(err.to_string()),
                plan_text: None,
                diagnostics_json: None,
            }),
        }
    }

    fn cleanup(&mut self, _req: CleanupRequest) -> Result<CleanupResponse, EngineError> {
        self.ctx = None;

        Ok(CleanupResponse {
            success: true,
            notes: vec!["session released".into()],
        })
    }

    fn collect_metadata(&self) -> EngineMetadata {
        EngineMetadata {
            engine_name: "datafusion".into(),
            engine_version: env!("CARGO_PKG_VERSION").into(),
            adapter_version: env!("CARGO_PKG_VERSION").into(),
            execution_mode: "in-process".into(),
            file_format: Some("csv".into()),
            table_mode: Some("registered_csv".into()),
            notes: vec![],
        }
    }
}