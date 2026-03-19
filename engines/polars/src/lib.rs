// engines\polars\src\lib.rs
use bm_engine::adapter::EngineAdapter;
use bm_engine::error::EngineError;
use bm_engine::request::{BootstrapRequest, CleanupRequest, PrepareDatasetRequest, RunQueryRequest};
use bm_engine::response::{
    BootstrapResponse, CleanupResponse, EngineMetadata, PrepareDatasetResponse, QueryExecutionResult,
};
use chrono::Utc;
use polars::lazy::dsl::{col, len, when};
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

            "q04" => lf
                .group_by([col("device_type")])
                .agg([
                    col("session_id").n_unique().alias("session_count"),
                    when(col("event_type").eq(lit("page_view")))
                        .then(lit(1i64))
                        .otherwise(lit(0i64))
                        .sum()
                        .alias("page_view_count"),
                    when(col("event_type").eq(lit("add_to_cart")))
                        .then(lit(1i64))
                        .otherwise(lit(0i64))
                        .sum()
                        .alias("add_to_cart_count"),
                    when(col("event_type").eq(lit("purchase")))
                        .then(lit(1i64))
                        .otherwise(lit(0i64))
                        .sum()
                        .alias("purchase_count"),
                    when(col("event_type").eq(lit("purchase")))
                        .then(col("revenue"))
                        .otherwise(lit(0.0f64))
                        .sum()
                        .alias("purchase_revenue"),
                ])
                .sort(
                    ["purchase_revenue", "purchase_count", "device_type"],
                    SortMultipleOptions::default()
                        .with_order_descending_multi([true, true, false]),
                )
                .collect()
                .map_err(|e| EngineError::Query(format!("polars q04 failed: {e}"))),

            "q05" => lf
                .group_by([col("country_code"), col("device_type")])
                .agg([
                    len().alias("event_count"),
                    col("session_id").n_unique().alias("session_count"),
                    col("latency_ms").mean().alias("avg_latency_ms"),
                    col("revenue").sum().alias("total_revenue"),
                ])
                .filter(col("event_count").gt_eq(lit(20u32)))
                .sort(
                    ["total_revenue", "session_count", "avg_latency_ms"],
                    SortMultipleOptions::default()
                        .with_order_descending_multi([true, true, false]),
                )
                .limit(10)
                .collect()
                .map_err(|e| EngineError::Query(format!("polars q05 failed: {e}"))),

            "q06" => lf
                .group_by([col("country_code"), col("referrer_domain")])
                .agg([
                    len().alias("event_count"),
                    col("revenue").sum().alias("total_revenue"),
                ])
                .collect()
                .and_then(|df| {
                    let mut sorted = df.sort(
                        ["country_code", "total_revenue", "referrer_domain"],
                        SortMultipleOptions::default()
                            .with_order_descending_multi([false, true, false]),
                    )?;

                    let countries = sorted
                        .column("country_code")?
                        .str()?;

                    let mut ranks = Vec::with_capacity(sorted.height());
                    let mut previous_country: Option<&str> = None;
                    let mut rank = 0u32;

                    for country in countries {
                        let country = country.ok_or_else(|| {
                            PolarsError::ComputeError("country_code cannot be null".into())
                        })?;

                        if previous_country != Some(country) {
                            previous_country = Some(country);
                            rank = 1;
                        } else {
                            rank += 1;
                        }

                        ranks.push(rank);
                    }

                    sorted.with_column(Series::new("revenue_rank".into(), ranks))?;

                    sorted
                        .lazy()
                        .filter(col("revenue_rank").lt_eq(lit(3u32)))
                        .sort(
                            [
                                "country_code",
                                "revenue_rank",
                                "total_revenue",
                                "referrer_domain",
                            ],
                            SortMultipleOptions::default()
                                .with_order_descending_multi([false, false, true, false]),
                        )
                        .collect()
                })
                .map_err(|e| EngineError::Query(format!("polars q06 failed: {e}"))),

            "q07" => lf
                .group_by([col("session_id"), col("country_code"), col("device_type")])
                .agg([
                    when(col("event_type").eq(lit("page_view")))
                        .then(lit(1i64))
                        .otherwise(lit(0i64))
                        .max()
                        .alias("saw_page_view"),
                    when(col("event_type").eq(lit("add_to_cart")))
                        .then(lit(1i64))
                        .otherwise(lit(0i64))
                        .max()
                        .alias("saw_add_to_cart"),
                    when(col("event_type").eq(lit("purchase")))
                        .then(lit(1i64))
                        .otherwise(lit(0i64))
                        .max()
                        .alias("saw_purchase"),
                    when(col("event_type").eq(lit("purchase")))
                        .then(col("revenue"))
                        .otherwise(lit(0.0f64))
                        .sum()
                        .alias("session_revenue"),
                ])
                .group_by([col("country_code"), col("device_type")])
                .agg([
                    len().alias("session_group_count"),
                    col("saw_page_view").sum().alias("sessions_with_page_view"),
                    col("saw_add_to_cart")
                        .sum()
                        .alias("sessions_with_add_to_cart"),
                    col("saw_purchase").sum().alias("sessions_with_purchase"),
                    col("session_revenue").sum().alias("purchase_revenue"),
                ])
                .sort(
                    [
                        "purchase_revenue",
                        "sessions_with_purchase",
                        "country_code",
                        "device_type",
                    ],
                    SortMultipleOptions::default()
                        .with_order_descending_multi([true, true, false, false]),
                )
                .collect()
                .map_err(|e| EngineError::Query(format!("polars q07 failed: {e}"))),

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
