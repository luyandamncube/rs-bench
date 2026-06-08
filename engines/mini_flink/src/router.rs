use crate::record::{ParsedRecord, SourceRecord};
use crate::runtime::RuntimeMetrics;
use bm_schema::streaming::StreamingFilterSpec;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};

pub async fn run_router(
    mut source_rx: Receiver<SourceRecord>,
    worker_txs: Vec<Sender<ParsedRecord>>,
    filter: Option<StreamingFilterSpec>,
    metrics: Arc<RuntimeMetrics>,
) {
    while let Some(source_record) = source_rx.recv().await {
        let parsed = ParsedRecord {
            key: source_record.key,
            value: source_record.value,
            event_type: source_record.event_type,
            created_at: source_record.created_at,
            processing_time_ms: source_record.processing_time_ms,
        };

        if !passes_filter(&parsed, filter.as_ref()) {
            metrics.increment_dropped();
            continue;
        }

        let worker_idx = partition_for_key(&parsed.key, worker_txs.len());
        metrics.increment_processed();

        if worker_txs[worker_idx].send(parsed).await.is_err() {
            break;
        }
    }
}

fn passes_filter(record: &ParsedRecord, filter: Option<&StreamingFilterSpec>) -> bool {
    match filter {
        Some(spec) => {
            let op = spec.op.as_deref().unwrap_or("eq");
            match spec.field.as_str() {
                "event_type" => compare_str(
                    &record.event_type,
                    op,
                    spec.equals.as_deref(),
                    spec.values.as_deref(),
                ),
                "key" | "device_type" => compare_str(
                    &record.key,
                    op,
                    spec.equals.as_deref(),
                    spec.values.as_deref(),
                ),
                "value" => compare_u64(
                    record.value,
                    op,
                    spec.equals.as_deref(),
                    spec.values.as_deref(),
                ),
                _ => true,
            }
        }
        None => true,
    }
}

fn compare_str(actual: &str, op: &str, value: Option<&str>, values: Option<&[String]>) -> bool {
    match op {
        "eq" => value.is_some_and(|expected| actual == expected),
        "ne" => value.is_some_and(|expected| actual != expected),
        "in" => values.is_some_and(|expected| expected.iter().any(|item| item == actual)),
        _ => false,
    }
}

fn compare_u64(actual: u64, op: &str, value: Option<&str>, values: Option<&[String]>) -> bool {
    match op {
        "in" => values.is_some_and(|expected| {
            expected
                .iter()
                .filter_map(|item| item.parse::<u64>().ok())
                .any(|item| item == actual)
        }),
        "eq" | "ne" | "gt" | "gte" | "lt" | "lte" => {
            let Some(expected) = value.and_then(|item| item.parse::<u64>().ok()) else {
                return false;
            };
            match op {
                "eq" => actual == expected,
                "ne" => actual != expected,
                "gt" => actual > expected,
                "gte" => actual >= expected,
                "lt" => actual < expected,
                "lte" => actual <= expected,
                _ => false,
            }
        }
        _ => false,
    }
}

pub fn partition_for_key(key: &str, parallelism: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() as usize) % parallelism.max(1)
}

#[cfg(test)]
mod tests {
    use super::{partition_for_key, passes_filter};
    use crate::record::ParsedRecord;
    use bm_schema::streaming::StreamingFilterSpec;
    use std::time::Instant;

    #[test]
    fn same_key_routes_to_same_partition() {
        let left = partition_for_key("mobile", 4);
        let right = partition_for_key("mobile", 4);
        assert_eq!(left, right);
    }

    #[test]
    fn supports_numeric_filter_ops() {
        let record = ParsedRecord {
            key: "mobile".into(),
            value: 12,
            event_type: "page_view".into(),
            created_at: Instant::now(),
            processing_time_ms: 0,
        };

        let filter = StreamingFilterSpec {
            field: "value".into(),
            op: Some("gte".into()),
            equals: Some("10".into()),
            values: None,
        };

        assert!(passes_filter(&record, Some(&filter)));
    }
}
