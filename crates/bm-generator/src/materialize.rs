// crates\bm-generator\src\materialize.rs
use crate::config::ClickstreamGeneratorConfig;
use crate::families::clickstream::ClickstreamRow;
use crate::manifest::DatasetManifest;
use crate::writer_csv::write_clickstream_csv;
use crate::writer_jsonl::write_clickstream_jsonl;
use crate::writer_parquet::write_clickstream_parquet;
use anyhow::{bail, Result};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};

pub fn materialize_clickstream_dataset(
    config: &ClickstreamGeneratorConfig,
    rows: &[ClickstreamRow],
) -> Result<PathBuf> {
    let root = PathBuf::from(&config.output_dir);
    fs::create_dir_all(&root)?;

    let config_json = serde_json::to_vec_pretty(config)?;
    let mut hasher = Sha256::new();
    hasher.update(&config_json);
    let config_hash = format!("{:x}", hasher.finalize());

    write_shared_metadata(&root, config_json.as_slice())?;

    for format in &config.formats {
        match format.as_str() {
            "csv" => write_csv_materialization(&root, config, rows, &config_hash)?,
            "jsonl" => write_jsonl_materialization(&root, config, rows, &config_hash)?,
            "parquet" => write_parquet_materialization(&root, config, rows, &config_hash)?,
            "avro" => bail!("avro materialization not implemented yet"),
            "arrow" => bail!("arrow materialization not implemented yet"),
            other => bail!("unsupported format: {other}"),
        }
    }

    Ok(root)
}

fn write_shared_metadata(root: &Path, config_json: &[u8]) -> Result<()> {
    fs::write(root.join("generation_config.json"), config_json)?;

    let schema = serde_json::json!({
        "fields": [
            {"name": "event_time", "type": "string"},
            {"name": "user_id", "type": "u64"},
            {"name": "session_id", "type": "u64"},
            {"name": "page_id", "type": "u32"},
            {"name": "device_type", "type": "string"},
            {"name": "country_code", "type": "string"},
            {"name": "referrer_domain", "type": "string"},
            {"name": "event_type", "type": "string"},
            {"name": "revenue", "type": "f64"},
            {"name": "latency_ms", "type": "u32"}
        ]
    });

    fs::write(root.join("schema.json"), serde_json::to_vec_pretty(&schema)?)?;
    Ok(())
}

fn write_csv_materialization(
    root: &Path,
    config: &ClickstreamGeneratorConfig,
    rows: &[ClickstreamRow],
    config_hash: &str,
) -> Result<()> {
    let dir = root.join("csv");
    fs::create_dir_all(&dir)?;

    let filename = format!("{}.csv", config.name);
    let data_path = dir.join(&filename);

    write_clickstream_csv(&data_path, rows)?;

    let manifest = DatasetManifest {
        dataset_name: config.name.clone(),
        dataset_family: config.family.clone(),
        seed: config.seed,
        rows: config.rows,
        format: "csv".to_string(),
        config_hash: config_hash.to_string(),
        files: vec![filename],
        partition_columns: vec![],
    };

    fs::write(
        dir.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;

    Ok(())
}

fn write_jsonl_materialization(
    root: &Path,
    config: &ClickstreamGeneratorConfig,
    rows: &[ClickstreamRow],
    config_hash: &str,
) -> Result<()> {
    let dir = root.join("jsonl");
    std::fs::create_dir_all(&dir)?;

    let filename = format!("{}.jsonl", config.name);
    let data_path = dir.join(&filename);

    write_clickstream_jsonl(&data_path, rows)?;

    let manifest = DatasetManifest {
        dataset_name: config.name.clone(),
        dataset_family: config.family.clone(),
        seed: config.seed,
        rows: config.rows,
        format: "jsonl".to_string(),
        config_hash: config_hash.to_string(),
        files: vec![filename],
        partition_columns: vec![],
    };

    std::fs::write(
        dir.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;

    Ok(())
}

fn write_parquet_materialization(
    root: &Path,
    config: &ClickstreamGeneratorConfig,
    rows: &[ClickstreamRow],
    config_hash: &str,
) -> Result<()> {
    let dir = root.join("parquet");
    std::fs::create_dir_all(&dir)?;

    let filename = format!("{}.parquet", config.name);
    let data_path = dir.join(&filename);

    write_clickstream_parquet(&data_path, rows)?;

    let manifest = DatasetManifest {
        dataset_name: config.name.clone(),
        dataset_family: config.family.clone(),
        seed: config.seed,
        rows: config.rows,
        format: "parquet".to_string(),
        config_hash: config_hash.to_string(),
        files: vec![filename],
        partition_columns: vec![],
    };

    std::fs::write(
        dir.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;

    Ok(())
}