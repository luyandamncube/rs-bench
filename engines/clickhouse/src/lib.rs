// engines\clickhouse\src\lib.rs
use bm_engine::adapter::EngineAdapter;
use bm_engine::error::EngineError;
use bm_engine::request::{BootstrapRequest, CleanupRequest, PrepareDatasetRequest, RunQueryRequest};
use bm_engine::response::{
    BootstrapResponse, CleanupResponse, EngineMetadata, PrepareDatasetResponse, QueryExecutionResult,
};
use chrono::Utc;
use clickhouse::{Client, Row};
use csv::Reader;
use serde::{Deserialize, Serialize};
use std::fs;
use std::fs::File;
use std::time::Instant;
use tokio::runtime::Runtime;

use arrow_array::{
    Float64Array, RecordBatch, StringArray, UInt32Array, UInt64Array,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;


#[derive(Clone)]
pub struct ClickHouseAdapter {
    client: Option<Client>,
    database: String,
    table_name: String,
    engine_version: String,
}

#[derive(Debug, Row, Serialize, Deserialize)]
struct ClickstreamInsertRow {
    event_time: String,
    user_id: u64,
    session_id: u64,
    page_id: u32,
    device_type: String,
    country_code: String,
    referrer_domain: String,
    event_type: String,
    revenue: f64,
    latency_ms: u32,
}

fn read_clickstream_rows_from_csv(path: &str) -> Result<Vec<ClickstreamInsertRow>, EngineError> {
    let mut rdr = Reader::from_path(path)
        .map_err(|e| EngineError::Prepare(format!("failed to open csv: {e}")))?;

    let mut rows = Vec::new();
    for record in rdr.deserialize::<ClickstreamInsertRow>() {
        let row = record
            .map_err(|e| EngineError::Prepare(format!("csv deserialize failed: {e}")))?;
        rows.push(row);
    }

    Ok(rows)
}

fn read_clickstream_rows_from_parquet(path: &str) -> Result<Vec<ClickstreamInsertRow>, EngineError> {
    let file = File::open(path)
        .map_err(|e| EngineError::Prepare(format!("failed to open parquet: {e}")))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| EngineError::Prepare(format!("failed to build parquet reader: {e}")))?;

    let mut reader = builder
        .build()
        .map_err(|e| EngineError::Prepare(format!("failed to create parquet batch reader: {e}")))?;

    let mut out = Vec::new();

    while let Some(batch) = reader.next() {
        let batch = batch
            .map_err(|e| EngineError::Prepare(format!("failed reading parquet batch: {e}")))?;
        out.extend(rows_from_batch(&batch)?);
    }

    Ok(out)
}

fn rows_from_batch(batch: &RecordBatch) -> Result<Vec<ClickstreamInsertRow>, EngineError> {
    let event_time = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| EngineError::Prepare("event_time column type mismatch".into()))?;
    let user_id = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| EngineError::Prepare("user_id column type mismatch".into()))?;
    let session_id = batch
        .column(2)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| EngineError::Prepare("session_id column type mismatch".into()))?;
    let page_id = batch
        .column(3)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| EngineError::Prepare("page_id column type mismatch".into()))?;
    let device_type = batch
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| EngineError::Prepare("device_type column type mismatch".into()))?;
    let country_code = batch
        .column(5)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| EngineError::Prepare("country_code column type mismatch".into()))?;
    let referrer_domain = batch
        .column(6)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| EngineError::Prepare("referrer_domain column type mismatch".into()))?;
    let event_type = batch
        .column(7)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| EngineError::Prepare("event_type column type mismatch".into()))?;
    let revenue = batch
        .column(8)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| EngineError::Prepare("revenue column type mismatch".into()))?;
    let latency_ms = batch
        .column(9)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| EngineError::Prepare("latency_ms column type mismatch".into()))?;

    let mut rows = Vec::with_capacity(batch.num_rows());

    for i in 0..batch.num_rows() {
        rows.push(ClickstreamInsertRow {
            event_time: event_time.value(i).to_string(),
            user_id: user_id.value(i),
            session_id: session_id.value(i),
            page_id: page_id.value(i),
            device_type: device_type.value(i).to_string(),
            country_code: country_code.value(i).to_string(),
            referrer_domain: referrer_domain.value(i).to_string(),
            event_type: event_type.value(i).to_string(),
            revenue: revenue.value(i),
            latency_ms: latency_ms.value(i),
        });
    }

    Ok(rows)
}

impl ClickHouseAdapter {
    pub fn new() -> Self {
        Self {
            client: None,
            database: "benchmark".to_string(),
            table_name: "benchmark_table".to_string(),
            engine_version: "unknown".to_string(),
        }
    }

    fn runtime() -> Result<Runtime, EngineError> {
        Runtime::new().map_err(|e| EngineError::Other(format!("failed to create tokio runtime: {e}")))
    }

    fn load_config(&mut self) -> Result<(String, String, String), EngineError> {
        let path = "configs/engines/clickhouse.toml";
        let raw = fs::read_to_string(path)
            .map_err(|e| EngineError::Bootstrap(format!("failed to read clickhouse config: {e}")))?;
        let value: toml::Value = toml::from_str(&raw)
            .map_err(|e| EngineError::Bootstrap(format!("failed to parse clickhouse config: {e}")))?;

        self.database = std::env::var("CLICKHOUSE_DATABASE")
            .ok()
            .or_else(|| value.get("database").and_then(|v| v.as_str()).map(str::to_string))
            .unwrap_or_else(|| "benchmark".to_string());

        self.table_name = std::env::var("CLICKHOUSE_TABLE_NAME")
            .ok()
            .or_else(|| value.get("table_name").and_then(|v| v.as_str()).map(str::to_string))
            .unwrap_or_else(|| "benchmark_table".to_string());

        let url = std::env::var("CLICKHOUSE_URL")
            .ok()
            .or_else(|| value.get("url").and_then(|v| v.as_str()).map(str::to_string))
            .unwrap_or_else(|| "http://localhost:8123".to_string());

        let user = std::env::var("CLICKHOUSE_USER")
            .ok()
            .or_else(|| value.get("user").and_then(|v| v.as_str()).map(str::to_string))
            .unwrap_or_else(|| "benchmark".to_string());

        let password = std::env::var("CLICKHOUSE_PASSWORD")
            .ok()
            .or_else(|| value.get("password").and_then(|v| v.as_str()).map(str::to_string))
            .unwrap_or_else(|| "benchmark".to_string());

        Ok((url, user, password))
    }
}

impl Default for ClickHouseAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl EngineAdapter for ClickHouseAdapter {
    fn name(&self) -> &'static str {
        "clickhouse"
    }

    fn bootstrap(&mut self, _req: BootstrapRequest) -> Result<BootstrapResponse, EngineError> {
        let (url, user, password) = self.load_config()?;

        // Stage 1: connect without selecting the benchmark DB yet
        let admin_client = Client::default()
            .with_url(url.clone())
            .with_user(user.clone())
            .with_password(password.clone());

        let rt = Self::runtime()?;

        rt.block_on(async {
            admin_client
                .query(&format!("CREATE DATABASE IF NOT EXISTS {}", self.database))
                .execute()
                .await
                .map_err(|e| EngineError::Bootstrap(format!("create database failed: {e}")))?;

            Ok::<(), EngineError>(())
        })?;

        // Stage 2: reconnect with the benchmark DB selected
        let client = Client::default()
            .with_url(url)
            .with_user(user)
            .with_password(password)
            .with_database(&self.database);

        self.client = Some(client);
        self.engine_version = "unknown".to_string();

        Ok(BootstrapResponse {
            engine_name: "clickhouse".into(),
            engine_version: self.engine_version.clone(),
            adapter_version: env!("CARGO_PKG_VERSION").into(),
            started_service: false,
            notes: vec!["http client".into()],
        })
    }

    fn prepare_dataset(
        &mut self,
        req: PrepareDatasetRequest,
    ) -> Result<PrepareDatasetResponse, EngineError> {
        let started = Utc::now();
        let t0 = Instant::now();

        let format = req.dataset_format.to_lowercase();

        let data_file = req
            .files
            .first()
            .ok_or_else(|| EngineError::Prepare("no dataset files supplied".into()))?
            .clone();

        let client = self
            .client
            .as_ref()
            .ok_or_else(|| EngineError::Prepare("client not bootstrapped".into()))?
            .clone();

        let table_name = self.table_name.clone();
        let rows = match format.as_str() {
            "csv" => read_clickstream_rows_from_csv(&data_file)?,
            "parquet" => read_clickstream_rows_from_parquet(&data_file)?,
            other => {
                return Err(EngineError::Prepare(format!(
                    "clickhouse adapter currently supports only csv and parquet, got {}",
                    other
                )));
            }
        };

        let rt = Self::runtime()?;

        rt.block_on(async {
            client
                .query(&format!("DROP TABLE IF EXISTS {}", table_name))
                .execute()
                .await
                .map_err(|e| EngineError::Prepare(format!("drop table failed: {e}")))?;

            client
                .query(&format!(
                    r#"
                    CREATE TABLE {} (
                        event_time String,
                        user_id UInt64,
                        session_id UInt64,
                        page_id UInt32,
                        device_type String,
                        country_code String,
                        referrer_domain String,
                        event_type String,
                        revenue Float64,
                        latency_ms UInt32
                    ) ENGINE = MergeTree()
                    ORDER BY (country_code, device_type, user_id)
                    "#,
                    table_name
                ))
                .execute()
                .await
                .map_err(|e| EngineError::Prepare(format!("create table failed: {e}")))?;

            let mut insert = client
                .insert::<ClickstreamInsertRow>(&table_name)
                .await
                .map_err(|e| EngineError::Prepare(format!("insert init failed: {e}")))?;

            for row in &rows {
                insert
                    .write(row)
                    .await
                    .map_err(|e| EngineError::Prepare(format!("insert write failed: {e}")))?;
            }

            insert
                .end()
                .await
                .map_err(|e| EngineError::Prepare(format!("insert finalize failed: {e}")))?;

            Ok::<(), EngineError>(())
        })?;

        Ok(PrepareDatasetResponse {
            setup_started_at: started,
            setup_elapsed_ms: t0.elapsed().as_millis() as u64,
            registered_objects: vec![self.table_name.clone()],
            notes: vec![format!("loaded {} file into clickhouse: {}", format, data_file)],
        })
    }

    fn run_query(&mut self, req: RunQueryRequest) -> Result<QueryExecutionResult, EngineError> {
        let started = Utc::now();
        let t0 = Instant::now();

        let client = self
            .client
            .as_ref()
            .ok_or_else(|| EngineError::Query("client not bootstrapped".into()))?
            .clone();

        let query = req.sql.replace("${table_name}", &self.table_name);
        let rt = Self::runtime()?;

        let outcome = rt.block_on(async {
            // For the current smoke queries we can fetch all as JSONEachRow by typed structs only if known.
            // Simpler MVP path: append FORMAT JSONEachRow and count returned lines using raw text is not available in this client easily,
            // so for now just execute the query and treat success as row_count=0 unless a count query fetches one row.
            client
                .query(&query)
                .execute()
                .await
                .map_err(|e| EngineError::Query(format!("query execute failed: {e}")))?;
            Ok::<u64, EngineError>(0)
        });

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
        Ok(CleanupResponse {
            success: true,
            notes: vec!["clickhouse cleanup skipped".into()],
        })
    }

    fn collect_metadata(&self) -> EngineMetadata {
        EngineMetadata {
            engine_name: "clickhouse".into(),
            engine_version: self.engine_version.clone(),
            adapter_version: env!("CARGO_PKG_VERSION").into(),
            execution_mode: "client-server".into(),
            file_format: Some("csv".into()),
            table_mode: Some("loaded-table".into()),
            notes: vec![],
        }
    }
}