// crates\bm-generator\src\writer_jsonl.rs
use crate::families::clickstream::ClickstreamRow;
use anyhow::Result;
use std::fs::File;
use std::io::Write;
use std::path::Path;

pub fn write_clickstream_jsonl(path: &Path, rows: &[ClickstreamRow]) -> Result<()> {
    let mut file = File::create(path)?;

    for row in rows {
        let line = serde_json::to_string(row)?;
        writeln!(file, "{line}")?;
    }

    Ok(())
}