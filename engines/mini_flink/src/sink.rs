use crate::record::SinkRecord;
use bm_schema::streaming::StreamingExpectedGroupTotal;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use tokio::sync::mpsc::Receiver;

pub async fn run_file_sink(
    mut rx: Receiver<SinkRecord>,
    output_path: PathBuf,
) -> std::io::Result<Vec<SinkRecord>> {
    let mut rows = Vec::new();

    while let Some(row) = rx.recv().await {
        rows.push(row);
    }

    sort_rows(&mut rows);

    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut file = File::create(output_path)?;
    for row in &rows {
        let line = serde_json::to_string(&row)?;
        writeln!(file, "{line}")?;
    }

    Ok(rows)
}

pub async fn run_stdout_sink(mut rx: Receiver<SinkRecord>) -> std::io::Result<Vec<SinkRecord>> {
    let mut rows = Vec::new();

    while let Some(row) = rx.recv().await {
        rows.push(row);
    }

    sort_rows(&mut rows);

    for row in &rows {
        println!(
            "window=[{}-{}] key={} count={} sum={} avg={}",
            row.window_start_ms,
            row.window_end_ms,
            row.key,
            row.event_count,
            row.value_sum,
            row.value_avg
                .map(|avg| format!("{avg:.2}"))
                .unwrap_or_else(|| "-".to_string())
        );
    }

    Ok(rows)
}

pub async fn run_live_stdout_sink(
    mut rx: Receiver<SinkRecord>,
) -> std::io::Result<Vec<SinkRecord>> {
    let mut rows = Vec::new();

    while let Some(row) = rx.recv().await {
        let stdout = io::stdout();
        let mut handle = stdout.lock();
        writeln!(
            handle,
            "window=[{}-{}] key={} count={} sum={} avg={}",
            row.window_start_ms,
            row.window_end_ms,
            row.key,
            row.event_count,
            row.value_sum,
            row.value_avg
                .map(|avg| format!("{avg:.2}"))
                .unwrap_or_else(|| "-".to_string())
        )?;
        handle.flush()?;
        rows.push(row);
    }

    Ok(rows)
}

pub fn validate_sink_output(
    output_path: &Path,
    expected: &[StreamingExpectedGroupTotal],
) -> Result<(), String> {
    let file = File::open(output_path)
        .map_err(|e| format!("failed to open sink output {}: {e}", output_path.display()))?;
    let reader = BufReader::new(file);

    let mut rows = Vec::new();
    for line in reader.lines() {
        let line = line.map_err(|e| format!("failed reading sink output: {e}"))?;
        if line.trim().is_empty() {
            continue;
        }
        let row: SinkRecord =
            serde_json::from_str(&line).map_err(|e| format!("invalid sink row JSON: {e}"))?;
        rows.push(row);
    }

    validate_sink_rows(&rows, expected)
}

pub fn validate_sink_rows(
    rows: &[SinkRecord],
    expected: &[StreamingExpectedGroupTotal],
) -> Result<(), String> {
    let mut actual: BTreeMap<String, (u64, u64)> = BTreeMap::new();

    for row in rows {
        let entry = actual.entry(row.key.clone()).or_insert((0, 0));
        entry.0 += row.event_count;
        entry.1 += row.value_sum;
    }

    for group in expected {
        match actual.get(&group.key) {
            Some((count, sum)) if *count == group.event_count && *sum == group.value_sum => {}
            Some((count, sum)) => {
                return Err(format!(
                    "sink aggregate mismatch for {}: expected ({}, {}), got ({}, {})",
                    group.key, group.event_count, group.value_sum, count, sum
                ));
            }
            None => return Err(format!("sink output missing key {}", group.key)),
        }
    }

    Ok(())
}

fn sort_rows(rows: &mut [SinkRecord]) {
    rows.sort_by(|a, b| {
        a.window_start_ms
            .cmp(&b.window_start_ms)
            .then_with(|| a.key.cmp(&b.key))
            .then_with(|| a.worker_partition.cmp(&b.worker_partition))
    });
}

#[cfg(test)]
mod tests {
    use super::{validate_sink_output, validate_sink_rows};
    use crate::record::SinkRecord;
    use bm_schema::streaming::StreamingExpectedGroupTotal;
    use std::fs::{self, File};
    use std::io::Write;

    #[test]
    fn sink_validation_matches_expected_totals() {
        let path = std::env::temp_dir().join("mini_flink_sink_validation.jsonl");
        let _ = fs::remove_file(&path);
        let mut file = File::create(&path).unwrap();
        writeln!(
            file,
            "{}",
            serde_json::to_string(&SinkRecord {
                worker_partition: 0,
                key: "mobile".into(),
                window_start_ms: 0,
                window_end_ms: 1000,
                event_count: 2,
                value_sum: 5,
                value_avg: None,
            })
            .unwrap()
        )
        .unwrap();

        let expected = vec![StreamingExpectedGroupTotal {
            key: "mobile".into(),
            event_count: 2,
            value_sum: 5,
        }];

        assert!(validate_sink_output(&path, &expected).is_ok());
    }

    #[test]
    fn row_validation_matches_expected_totals() {
        let rows = vec![SinkRecord {
            worker_partition: 0,
            key: "mobile".into(),
            window_start_ms: 0,
            window_end_ms: 1000,
            event_count: 2,
            value_sum: 5,
            value_avg: None,
        }];

        let expected = vec![StreamingExpectedGroupTotal {
            key: "mobile".into(),
            event_count: 2,
            value_sum: 5,
        }];

        assert!(validate_sink_rows(&rows, &expected).is_ok());
    }
}
