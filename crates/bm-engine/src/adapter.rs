// crates\bm-engine\src\adapter.rs
use crate::error::EngineError;
use crate::request::{BootstrapRequest, CleanupRequest, PrepareDatasetRequest, RunQueryRequest};
use crate::response::{
    BootstrapResponse, CleanupResponse, EngineMetadata, PrepareDatasetResponse, QueryExecutionResult,
};

pub trait EngineAdapter: Send {
    fn name(&self) -> &'static str;

    fn bootstrap(&mut self, req: BootstrapRequest) -> Result<BootstrapResponse, EngineError>;

    fn prepare_dataset(
        &mut self,
        req: PrepareDatasetRequest,
    ) -> Result<PrepareDatasetResponse, EngineError>;

    fn run_query(&mut self, req: RunQueryRequest) -> Result<QueryExecutionResult, EngineError>;

    fn cleanup(&mut self, req: CleanupRequest) -> Result<CleanupResponse, EngineError>;

    fn collect_metadata(&self) -> EngineMetadata;
}