// crates\bm-engine\tests\mock_adapter.rs
use bm_engine::adapter::EngineAdapter;
use bm_engine::error::EngineError;
use bm_engine::request::*;
use bm_engine::response::*;
use chrono::Utc;

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
            elapsed_ms: 42 + req.repetition as u64,
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

#[test]
fn mock_adapter_lifecycle_works() {
    let mut adapter = MockAdapter;

    let boot = adapter.bootstrap(BootstrapRequest {
        run_id: "test-run".into(),
        capture_version: true,
    }).unwrap();

    assert_eq!(boot.engine_name, "mock");

    let prep = adapter.prepare_dataset(PrepareDatasetRequest {
        dataset_manifest_path: "datasets/test/manifest.json".into(),
        dataset_name: "test".into(),
        dataset_family: "clickstream".into(),
        dataset_format: "parquet".into(),
        files: vec!["part-000.parquet".into()],
        mode: "file-backed".into(),
    }).unwrap();

    assert_eq!(prep.registered_objects[0], "mock_table");

    let result = adapter.run_query(RunQueryRequest {
        query_id: "q01".into(),
        query_name: "test_query".into(),
        query_category: "scan_filter".into(),
        sql: "select 1".into(),
        repetition: 1,
        warm_or_cold: "cold".into(),
        capture_plan: false,
    }).unwrap();

    assert!(result.success);

    let cleanup = adapter.cleanup(CleanupRequest {
        run_id: "test-run".into(),
    }).unwrap();

    assert!(cleanup.success);
}