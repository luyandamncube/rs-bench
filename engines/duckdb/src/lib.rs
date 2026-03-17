// engines\duckdb\src\lib.rs
use bm_engine::adapter::EngineAdapter;
use bm_engine::error::EngineError;
use bm_engine::request::{BootstrapRequest, CleanupRequest, PrepareDatasetRequest, RunQueryRequest};
use bm_engine::response::{
    BootstrapResponse, CleanupResponse, EngineMetadata, PrepareDatasetResponse, QueryExecutionResult,
};
use chrono::Utc;
use duckdb::Connection;
use std::time::Instant;

pub struct DuckDbAdapter {
    conn: Option<Connection>,
    table_name: String,
    engine_version: String,
}

impl DuckDbAdapter {
    pub fn new() -> Self {
        Self {
            conn: None,
            table_name: "benchmark_table".to_string(),
            engine_version: "unknown".to_string(),
        }
    }
}

impl Default for DuckDbAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl EngineAdapter for DuckDbAdapter {
    fn name(&self) -> &'static str {
        "duckdb"
    }

    fn bootstrap(&mut self, _req: BootstrapRequest) -> Result<BootstrapResponse, EngineError> {
        let conn = Connection::open_in_memory()
            .map_err(|e| EngineError::Bootstrap(format!("failed to open in-memory duckdb: {e}")))?;

        let version = conn
            .version()
            .map_err(|e| EngineError::Bootstrap(format!("failed to get duckdb version: {e}")))?;

        self.engine_version = version.clone();
        self.conn = Some(conn);

        Ok(BootstrapResponse {
            engine_name: "duckdb".into(),
            engine_version: version,
            adapter_version: env!("CARGO_PKG_VERSION").into(),
            started_service: false,
            notes: vec!["in-process in-memory database".into()],
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

        let conn = self
            .conn
            .as_ref()
            .ok_or_else(|| EngineError::Prepare("connection not bootstrapped".into()))?;

        let escaped_path = first_file.replace('\'', "''");

        let sql = match format.as_str() {
            "csv" => format!(
                "CREATE OR REPLACE VIEW {} AS SELECT * FROM read_csv_auto('{}', HEADER=TRUE);",
                self.table_name, escaped_path
            ),
            "parquet" => format!(
                "CREATE OR REPLACE VIEW {} AS SELECT * FROM read_parquet('{}');",
                self.table_name, escaped_path
            ),
            other => {
                return Err(EngineError::Prepare(format!(
                    "duckdb adapter currently supports only csv and parquet, got {}",
                    other
                )));
            }
        };

        conn.execute_batch(&sql)
            .map_err(|e| EngineError::Prepare(format!("failed to register {} view: {e}", format)))?;

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

        let conn = self
            .conn
            .as_ref()
            .ok_or_else(|| EngineError::Query("connection not bootstrapped".into()))?;

        let query = req.sql.replace("${table_name}", &self.table_name);

        let outcome = (|| -> Result<u64, EngineError> {
            let mut stmt = conn
                .prepare(&query)
                .map_err(|e| EngineError::Query(format!("prepare failed: {e}")))?;

            let mut rows = stmt
                .query([])
                .map_err(|e| EngineError::Query(format!("query failed: {e}")))?;

            let mut row_count = 0_u64;
            while let Some(_row) = rows
                .next()
                .map_err(|e| EngineError::Query(format!("row fetch failed: {e}")))? {
                row_count += 1;
            }

            Ok(row_count)
        })();

        match outcome {
            Ok(row_count) => Ok(QueryExecutionResult {
                started_at: started,
                elapsed_ms: t0.elapsed().as_millis() as u64,
                success: true,
                row_count: Some(row_count),
                error_message: None,
                plan_text: None,
                diagnostics_json: Some(format!(r#"{{"rows_returned": {row_count}}}"#)),
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
        self.conn = None;

        Ok(CleanupResponse {
            success: true,
            notes: vec!["connection released".into()],
        })
    }

    fn collect_metadata(&self) -> EngineMetadata {
        EngineMetadata {
            engine_name: "duckdb".into(),
            engine_version: self.engine_version.clone(),
            adapter_version: env!("CARGO_PKG_VERSION").into(),
            execution_mode: "in-process".into(),
            file_format: Some("csv".into()),
            table_mode: Some("read_csv_auto view".into()),
            notes: vec![],
        }
    }
}