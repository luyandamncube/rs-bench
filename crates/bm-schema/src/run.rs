// crates\bm-schema\src\run.rs
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunConfig {
    pub run_name: String,
    pub engine: String,
    pub workload_path: String,
    pub dataset_manifest_path: String,
    pub repetitions: u32,
    pub capture_plans: bool,
}