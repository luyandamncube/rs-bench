// crates\bm-generator\src\writer_parquet.rs
use crate::families::clickstream::ClickstreamRow;
use anyhow::Result;
use arrow_array::{
    ArrayRef, Float64Array, RecordBatch, StringArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

pub fn write_clickstream_parquet(path: &Path, rows: &[ClickstreamRow]) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("event_time", DataType::Utf8, false),
        Field::new("user_id", DataType::UInt64, false),
        Field::new("session_id", DataType::UInt64, false),
        Field::new("page_id", DataType::UInt32, false),
        Field::new("device_type", DataType::Utf8, false),
        Field::new("country_code", DataType::Utf8, false),
        Field::new("referrer_domain", DataType::Utf8, false),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("revenue", DataType::Float64, false),
        Field::new("latency_ms", DataType::UInt32, false),
    ]));

    let event_time: Vec<&str> = rows.iter().map(|r| r.event_time.as_str()).collect();
    let user_id: Vec<u64> = rows.iter().map(|r| r.user_id).collect();
    let session_id: Vec<u64> = rows.iter().map(|r| r.session_id).collect();
    let page_id: Vec<u32> = rows.iter().map(|r| r.page_id).collect();
    let device_type: Vec<&str> = rows.iter().map(|r| r.device_type.as_str()).collect();
    let country_code: Vec<&str> = rows.iter().map(|r| r.country_code.as_str()).collect();
    let referrer_domain: Vec<&str> = rows.iter().map(|r| r.referrer_domain.as_str()).collect();
    let event_type: Vec<&str> = rows.iter().map(|r| r.event_type.as_str()).collect();
    let revenue: Vec<f64> = rows.iter().map(|r| r.revenue).collect();
    let latency_ms: Vec<u32> = rows.iter().map(|r| r.latency_ms).collect();

    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(event_time)),
        Arc::new(UInt64Array::from(user_id)),
        Arc::new(UInt64Array::from(session_id)),
        Arc::new(UInt32Array::from(page_id)),
        Arc::new(StringArray::from(device_type)),
        Arc::new(StringArray::from(country_code)),
        Arc::new(StringArray::from(referrer_domain)),
        Arc::new(StringArray::from(event_type)),
        Arc::new(Float64Array::from(revenue)),
        Arc::new(UInt32Array::from(latency_ms)),
    ];

    let batch = RecordBatch::try_new(schema.clone(), columns)?;

    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}
