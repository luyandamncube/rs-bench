//crates\bm-generator\src\lib.rs
pub mod config;
pub mod families;
pub mod manifest;
pub mod materialize;
pub mod writer_arrow;
pub mod writer_avro;
pub mod writer_csv;
pub mod writer_jsonl;
pub mod writer_parquet;

use anyhow::Result;
use std::path::PathBuf;

use config::ClickstreamGeneratorConfig;
use families::clickstream::generate_clickstream_rows;
use materialize::materialize_clickstream_dataset;

pub fn generate_clickstream_dataset(config: ClickstreamGeneratorConfig) -> Result<PathBuf> {
    let rows = generate_clickstream_rows(&config);
    materialize_clickstream_dataset(&config, &rows)
}