// engines\polars\src\lib.rs
use bm_engine::adapter::EngineAdapter;
use bm_engine::error::EngineError;
use bm_engine::request::{BootstrapRequest, CleanupRequest, PrepareDatasetRequest, RunQueryRequest};
use bm_engine::response::{
    BootstrapResponse, CleanupResponse, EngineMetadata, PrepareDatasetResponse, QueryExecutionResult,
};
use chrono::Utc;
use polars::lazy::dsl::{col, len};
use polars::prelude::*;
use polars::prelude::PlPath;
use std::time::Instant;

pub struct PolarsAdapter {
    dataset_path: Option<String>,
    dataset_format: Option<String>,
}

impl PolarsAdapter {
    pub fn new() -> Self {
        Self {
            dataset_path: None,
            dataset_format: None,
        }
    }

    fn lazy_frame(&self) -> Result<LazyFrame, EngineError> {
        let path = self
            .dataset_path
            .as_ref()
            .ok_or_else(|| EngineError::Query("dataset not prepared".into()))?;

        let format = self
            .dataset_format
            .as_ref()
            .ok_or_else(|| EngineError::Query("dataset format not prepared".into()))?;

        let pl_path = PlPath::new(path.as_str());

        match format.as_str() {
            "parquet" => LazyFrame::scan_parquet(pl_path.clone(), ScanArgsParquet::default())
                .map_err(|e| EngineError::Query(format!("polars scan_parquet failed: {e}"))),

            "csv" => LazyCsvReader::new(pl_path)
                .with_has_header(true)
                .finish()
                .map_err(|e| EngineError::Query(format!("polars scan_csv failed: {e}"))),

            other => Err(EngineError::Query(format!(
                "polars adapter currently supports only csv and parquet, got {}",
                other
            ))),
        }
    }
}

impl Default for PolarsAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl EngineAdapter for PolarsAdapter {
    fn name(&self) -> &'static str {
        "polars"
    }

    fn bootstrap(&mut self, _req: BootstrapRequest) -> Result<BootstrapResponse, EngineError> {
        Ok(BootstrapResponse {
            engine_name: "polars".into(),
            engine_version: env!("CARGO_PKG_VERSION").into(),
            adapter_version: env!("CARGO_PKG_VERSION").into(),
            started_service: false,
            notes: vec!["in-process lazy engine".into()],
        })
    }

    fn prepare_dataset(
        &mut self,
        req: PrepareDatasetRequest,
    ) -> Result<PrepareDatasetResponse, EngineError> {
        let started = Utc::now();
        let t0 = Instant::now();

        let first_file = req
            .files
            .first()
            .ok_or_else(|| EngineError::Prepare("no dataset files supplied".into()))?
            .clone();

        match req.dataset_format.to_lowercase().as_str() {
            "parquet" | "csv" => {
                self.dataset_path = Some(first_file.clone());
                self.dataset_format = Some(req.dataset_format.to_lowercase());
            }
            other => {
                return Err(EngineError::Prepare(format!(
                    "polars adapter currently supports only csv and parquet, got {}",
                    other
                )));
            }
        }

        Ok(PrepareDatasetResponse {
            setup_started_at: started,
            setup_elapsed_ms: t0.elapsed().as_millis() as u64,
            registered_objects: vec!["polars_lazy_source".into()],
            notes: vec![format!("prepared {} file: {}", req.dataset_format, first_file)],
        })
    }

    fn run_query(&mut self, req: RunQueryRequest) -> Result<QueryExecutionResult, EngineError> {
        let started = Utc::now();
        let t0 = Instant::now();

        let lf = self.lazy_frame()?;

        let outcome: Result<DataFrame, EngineError> = match req.query_id.as_str() {
            "q01" => lf
                .filter(col("country_code").eq(lit("US")))
                .select([len().alias("row_count")])
                .collect()
                .map_err(|e| EngineError::Query(format!("polars q01 failed: {e}"))),

            "q02" => lf
                .group_by([col("device_type")])
                .agg([len().alias("event_count")])
                .sort(
                    ["event_count"],
                    SortMultipleOptions::default().with_order_descending(true),
                )
                .collect()
                .map_err(|e| EngineError::Query(format!("polars q02 failed: {e}"))),

            "q03" => lf
                .group_by([col("event_type")])
                .agg([col("revenue").sum().alias("total_revenue")])
                .sort(
                    ["total_revenue"],
                    SortMultipleOptions::default().with_order_descending(true),
                )
                .collect()
                .map_err(|e| EngineError::Query(format!("polars q03 failed: {e}"))),

            other => Err(EngineError::Query(format!(
                "polars adapter does not implement query id {} yet",
                other
            ))),
        };

        match outcome {
            Ok(df) => Ok(QueryExecutionResult {
                started_at: started,
                elapsed_ms: t0.elapsed().as_millis() as u64,
                success: true,
                row_count: Some(df.height() as u64),
                error_message: None,
                plan_text: None,
                diagnostics_json: Some(format!(r#"{{"rows_returned": {}}}"#, df.height())),
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
        self.dataset_path = None;
        self.dataset_format = None;

        Ok(CleanupResponse {
            success: true,
            notes: vec![],
        })
    }

    fn collect_metadata(&self) -> EngineMetadata {
        EngineMetadata {
            engine_name: "polars".into(),
            engine_version: env!("CARGO_PKG_VERSION").into(),
            adapter_version: env!("CARGO_PKG_VERSION").into(),
            execution_mode: "in-process".into(),
            file_format: self.dataset_format.clone(),
            table_mode: Some("lazy_scan".into()),
            notes: vec![],
        }
    }
}