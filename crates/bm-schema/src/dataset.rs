// crates\bm-schema\src\dataset.rs
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetManifest {
    pub dataset_name: String,
    pub dataset_family: String,
    pub seed: u64,
    pub rows: u64,
    pub format: String,
    pub config_hash: String,
    pub files: Vec<String>,
    pub partition_columns: Vec<String>,
}