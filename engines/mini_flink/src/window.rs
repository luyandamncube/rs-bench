use crate::record::{ParsedRecord, SinkRecord, WindowState};
use std::collections::HashMap;

pub fn window_bounds(processing_time_ms: u64, window_size_ms: u64) -> (u64, u64) {
    let start = (processing_time_ms / window_size_ms) * window_size_ms;
    let end = start + window_size_ms;
    (start, end)
}

pub fn apply_record(
    state_by_key: &mut HashMap<String, WindowState>,
    record: &ParsedRecord,
    window_size_ms: u64,
    worker_partition: usize,
    aggregate_functions: &[String],
) -> Option<SinkRecord> {
    let (window_start_ms, window_end_ms) = window_bounds(record.processing_time_ms, window_size_ms);

    match state_by_key.get_mut(&record.key) {
        Some(state) if state.window_start_ms == window_start_ms => {
            state.event_count += 1;
            state.value_sum += record.value;
            state.last_seen_at_ms = record.processing_time_ms;
            None
        }
        Some(state) => {
            let closed = SinkRecord {
                worker_partition,
                key: record.key.clone(),
                window_start_ms: state.window_start_ms,
                window_end_ms: state.window_end_ms,
                event_count: state.event_count,
                value_sum: state.value_sum,
                value_avg: aggregate_avg(state.event_count, state.value_sum, aggregate_functions),
            };

            *state = WindowState {
                event_count: 1,
                value_sum: record.value,
                window_start_ms,
                window_end_ms,
                last_seen_at_ms: record.processing_time_ms,
            };

            Some(closed)
        }
        None => {
            state_by_key.insert(
                record.key.clone(),
                WindowState {
                    event_count: 1,
                    value_sum: record.value,
                    window_start_ms,
                    window_end_ms,
                    last_seen_at_ms: record.processing_time_ms,
                },
            );
            None
        }
    }
}

pub fn close_due_windows(
    state_by_key: &mut HashMap<String, WindowState>,
    now_ms: u64,
    worker_partition: usize,
    aggregate_functions: &[String],
) -> Vec<SinkRecord> {
    let mut due_keys = Vec::new();

    for (key, state) in state_by_key.iter() {
        if now_ms >= state.window_end_ms {
            due_keys.push(key.clone());
        }
    }

    let mut closed = Vec::with_capacity(due_keys.len());
    for key in due_keys {
        if let Some(state) = state_by_key.remove(&key) {
            closed.push(SinkRecord {
                worker_partition,
                key,
                window_start_ms: state.window_start_ms,
                window_end_ms: state.window_end_ms,
                event_count: state.event_count,
                value_sum: state.value_sum,
                value_avg: aggregate_avg(state.event_count, state.value_sum, aggregate_functions),
            });
        }
    }

    closed
}

pub fn flush_all_windows(
    state_by_key: &mut HashMap<String, WindowState>,
    worker_partition: usize,
    aggregate_functions: &[String],
) -> Vec<SinkRecord> {
    let mut closed = Vec::with_capacity(state_by_key.len());

    for (key, state) in state_by_key.drain() {
        closed.push(SinkRecord {
            worker_partition,
            key,
            window_start_ms: state.window_start_ms,
            window_end_ms: state.window_end_ms,
            event_count: state.event_count,
            value_sum: state.value_sum,
            value_avg: aggregate_avg(state.event_count, state.value_sum, aggregate_functions),
        });
    }

    closed
}

fn aggregate_avg(event_count: u64, value_sum: u64, aggregate_functions: &[String]) -> Option<f64> {
    if !aggregate_functions.iter().any(|function| function == "avg") || event_count == 0 {
        return None;
    }

    Some(value_sum as f64 / event_count as f64)
}

#[cfg(test)]
mod tests {
    use super::{apply_record, close_due_windows};
    use crate::record::ParsedRecord;
    use std::collections::HashMap;
    use std::time::Instant;

    #[test]
    fn window_closes_on_due_time() {
        let mut state = HashMap::new();
        let created_at = Instant::now();
        let record = ParsedRecord {
            key: "mobile".into(),
            value: 3,
            event_type: "page_view".into(),
            created_at,
            processing_time_ms: 50,
        };

        let closed = apply_record(&mut state, &record, 100, 0, &["count".into(), "avg".into()]);
        assert!(closed.is_none());

        let due = close_due_windows(&mut state, 100, 0, &["count".into(), "avg".into()]);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].event_count, 1);
        assert_eq!(due[0].value_sum, 3);
        assert_eq!(due[0].value_avg, Some(3.0));
    }
}
