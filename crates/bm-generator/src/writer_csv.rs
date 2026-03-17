// crates\bm-generator\src\writer_csv.rs
use crate::families::clickstream::ClickstreamRow;
use anyhow::Result;
use csv::Writer;
use std::fs::File;
use std::path::Path;

pub fn write_clickstream_csv(path: &Path, rows: &[ClickstreamRow]) -> Result<()> {
    let file = File::create(path)?;
    let mut writer = Writer::from_writer(file);

    for row in rows {
        writer.serialize(row)?;
    }

    writer.flush()?;
    Ok(())
}