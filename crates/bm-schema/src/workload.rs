// crates\bm-schema\src\workload.rs
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadQuery {
    pub id: String,
    pub name: String,
    pub category: String,
    pub file: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadDefinition {
    pub name: String,
    pub family: String,
    pub description: String,
    pub queries: Vec<WorkloadQuery>,
}