// crates\bm-generator\src\config.rs
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClickstreamGeneratorConfig {
    pub name: String,
    pub family: String,
    pub seed: u64,
    pub rows: u64,
    pub output_dir: String,
    pub formats: Vec<String>,
    pub cardinality: ClickstreamCardinalityConfig,
    pub distributions: ClickstreamDistributionConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClickstreamCardinalityConfig {
    pub users: u64,
    pub sessions: u64,
    pub pages: u32,
    pub countries: u32,
    pub referrers: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClickstreamDistributionConfig {
    pub null_ratio: f64,
    pub skew: String,
}