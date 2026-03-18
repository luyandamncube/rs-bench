// engines/spark/src/lib.rs
use bm_engine::adapter::EngineAdapter;
use bm_engine::error::EngineError;
use bm_engine::request::{BootstrapRequest, CleanupRequest, PrepareDatasetRequest, RunQueryRequest};
use bm_engine::response::{
    BootstrapResponse, CleanupResponse, EngineMetadata, PrepareDatasetResponse, QueryExecutionResult,
};
use chrono::Utc;
use std::fs;
use std::time::Instant;

pub struct SparkAdapter {
    master_url: Option<String>,
    dataset_path: Option<String>,
    dataset_format: Option<String>,
}

impl SparkAdapter {
    pub fn new() -> Self {
        Self {
            master_url: None,
            dataset_path: None,
            dataset_format: None,
        }
    }

    fn load_master_url(&mut self) -> Result<(), EngineError> {
        let env_url = std::env::var("SPARK_MASTER_URL").ok();

        if let Some(url) = env_url {
            self.master_url = Some(url);
            return Ok(());
        }

        let path = "configs/engines/spark.toml";
        let raw = fs::read_to_string(path)
            .map_err(|e| EngineError::Bootstrap(format!("failed to read spark config: {e}")))?;
        let value: toml::Value = toml::from_str(&raw)
            .map_err(|e| EngineError::Bootstrap(format!("failed to parse spark config: {e}")))?;

        let master_url = value
            .get("master_url")
            .and_then(|v| v.as_str())
            .unwrap_or("spark://spark-master:7077")
            .to_string();

        self.master_url = Some(master_url);
        Ok(())
    }
}

impl Default for SparkAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl EngineAdapter for SparkAdapter {
    fn name(&self) -> &'static str {
        "spark"
    }

    fn bootstrap(&mut self, _req: BootstrapRequest) -> Result<BootstrapResponse, EngineError> {
        self.load_master_url()?;

        Ok(BootstrapResponse {
            engine_name: "spark".into(),
            engine_version: "unknown".into(),
            adapter_version: env!("CARGO_PKG_VERSION").into(),
            started_service: false,
            notes: vec![
                format!(
                    "master_url={}",
                    self.master_url.clone().unwrap_or_else(|| "unknown".into())
                ),
                "spark standalone submitter scaffold".into(),
            ],
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
                    "spark adapter currently supports only csv and parquet, got {}",
                    other
                )));
            }
        }

        Ok(PrepareDatasetResponse {
            setup_started_at: started,
            setup_elapsed_ms: t0.elapsed().as_millis() as u64,
            registered_objects: vec!["spark_input".into()],
            notes: vec![format!("prepared {} file: {}", req.dataset_format, first_file)],
        })
    }

    fn run_query(&mut self, req: RunQueryRequest) -> Result<QueryExecutionResult, EngineError> {
        let started = Utc::now();
        let t0 = Instant::now();

        let _master_url = self
            .master_url
            .as_ref()
            .ok_or_else(|| EngineError::Query("spark master not configured".into()))?;

        let _dataset_path = self
            .dataset_path
            .as_ref()
            .ok_or_else(|| EngineError::Query("dataset not prepared".into()))?;

        let _dataset_format = self
            .dataset_format
            .as_ref()
            .ok_or_else(|| EngineError::Query("dataset format not prepared".into()))?;

        let _query_id = req.query_id;

        Ok(QueryExecutionResult {
            started_at: started,
            elapsed_ms: t0.elapsed().as_millis() as u64,
            success: false,
            row_count: None,
            error_message: Some("spark query execution not implemented yet".into()),
            plan_text: None,
            diagnostics_json: None,
        })
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
            engine_name: "spark".into(),
            engine_version: "unknown".into(),
            adapter_version: env!("CARGO_PKG_VERSION").into(),
            execution_mode: "standalone-submit".into(),
            file_format: self.dataset_format.clone(),
            table_mode: Some("spark_dataframe".into()),
            notes: vec![format!(
                "master_url={}",
                self.master_url.clone().unwrap_or_else(|| "unknown".into())
            )],
        }
    }
}