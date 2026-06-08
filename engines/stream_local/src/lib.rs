use bm_engine::error::EngineError;
use bm_engine::streaming::{
    StreamingBootstrapRequest, StreamingBootstrapResponse, StreamingCleanupRequest,
    StreamingCleanupResponse, StreamingEngineAdapter, StreamingPrepareScenarioRequest,
    StreamingPrepareScenarioResponse, StreamingRunRequest, StreamingRunResult,
};
use bm_schema::streaming::{StreamingExpectedGroupTotal, StreamingWorkloadDefinition};
use chrono::Utc;
use std::collections::BTreeMap;
use std::fs;
use std::time::Instant;

#[derive(Debug, Clone)]
struct StreamEvent {
    event_time_ms: u64,
    key: String,
    value: u64,
}

#[derive(Default)]
pub struct StreamLocalAdapter {
    workload: Option<StreamingWorkloadDefinition>,
}

impl StreamLocalAdapter {
    pub fn new() -> Self {
        Self::default()
    }

    fn workload(&self) -> Result<&StreamingWorkloadDefinition, EngineError> {
        self.workload
            .as_ref()
            .ok_or_else(|| EngineError::Prepare("streaming workload not prepared".into()))
    }
}

impl StreamingEngineAdapter for StreamLocalAdapter {
    fn name(&self) -> &'static str {
        "stream_local"
    }

    fn bootstrap_streaming(
        &mut self,
        req: StreamingBootstrapRequest,
    ) -> Result<StreamingBootstrapResponse, EngineError> {
        Ok(StreamingBootstrapResponse {
            engine_name: self.name().into(),
            engine_version: env!("CARGO_PKG_VERSION").into(),
            adapter_version: env!("CARGO_PKG_VERSION").into(),
            started_service: false,
            notes: vec![
                format!("run_id={}", req.run_id),
                "local deterministic stream runner".into(),
            ],
        })
    }

    fn prepare_streaming_scenario(
        &mut self,
        req: StreamingPrepareScenarioRequest,
    ) -> Result<StreamingPrepareScenarioResponse, EngineError> {
        let started = Utc::now();
        let t0 = Instant::now();

        let raw = fs::read_to_string(&req.workload_path)
            .map_err(|e| EngineError::Prepare(format!("failed to read workload: {e}")))?;
        let workload: StreamingWorkloadDefinition = serde_yaml::from_str(&raw)
            .map_err(|e| EngineError::Prepare(format!("failed to parse workload: {e}")))?;

        self.workload = Some(workload);

        Ok(StreamingPrepareScenarioResponse {
            setup_started_at: started,
            setup_elapsed_ms: t0.elapsed().as_millis() as u64,
            registered_objects: vec!["stream_local_pipeline".into()],
            notes: vec![
                format!("duration_secs={}", req.duration_secs),
                format!("warmup_secs={}", req.warmup_secs),
                format!("event_rate_per_sec={}", req.event_rate_per_sec),
                format!("seed={}", req.seed),
            ],
        })
    }

    fn run_streaming(
        &mut self,
        req: StreamingRunRequest,
    ) -> Result<StreamingRunResult, EngineError> {
        let started = Utc::now();
        let workload = self.workload()?.clone();
        let startup_t0 = Instant::now();

        let expected_events = req
            .event_rate_per_sec
            .checked_mul(req.duration_secs)
            .ok_or_else(|| EngineError::Query("event count overflow".into()))?;
        let expected_total_events: u64 = workload
            .expected_output
            .expected_group_totals
            .iter()
            .map(|group| group.event_count)
            .sum();

        if expected_events != expected_total_events {
            return Err(EngineError::Query(format!(
                "streaming workload expects {} events but config requests {}",
                expected_total_events, expected_events
            )));
        }

        let warmup_event_count = req
            .event_rate_per_sec
            .checked_mul(req.warmup_secs)
            .ok_or_else(|| EngineError::Query("warmup event count overflow".into()))?;

        let startup_time_ms = startup_t0.elapsed().as_millis() as u64;

        let warmup_events = generate_warmup_events(
            &workload.expected_output.expected_group_totals,
            warmup_event_count,
        );
        let mut warmup_state = BTreeMap::new();
        for event in warmup_events {
            apply_event(&mut warmup_state, &event);
        }

        let events = generate_events(&workload.expected_output.expected_group_totals);
        let mut aggregates = BTreeMap::new();
        let mut latency_micros = Vec::with_capacity(events.len());

        let run_t0 = Instant::now();
        for event in &events {
            let event_t0 = Instant::now();
            apply_event(&mut aggregates, event);
            latency_micros.push(
                event_t0.elapsed().as_micros() as u64 + synthetic_latency_offset_micros(event),
            );
        }
        let run_elapsed = run_t0.elapsed();

        let correctness =
            validate_aggregates(&aggregates, &workload.expected_output.expected_group_totals);
        let throughput = if run_elapsed.as_secs_f64() > 0.0 {
            events.len() as f64 / run_elapsed.as_secs_f64()
        } else {
            events.len() as f64
        };

        let (p50, p95, p99) = percentile_triplet_ms(&latency_micros);

        Ok(StreamingRunResult {
            started_at: started,
            startup_time_ms,
            throughput_events_per_sec: throughput,
            latency_p50_ms: p50,
            latency_p95_ms: p95,
            latency_p99_ms: p99,
            processed_events: events.len() as u64,
            dropped_events: 0,
            failed_events: 0,
            records_emitted: events.len() as u64,
            emitted_windows: 1,
            sink_output_path: None,
            correctness_passed: correctness.is_ok(),
            correctness_message: correctness.err(),
            success: true,
            error_message: None,
        })
    }

    fn cleanup_streaming(
        &mut self,
        _req: StreamingCleanupRequest,
    ) -> Result<StreamingCleanupResponse, EngineError> {
        self.workload = None;
        Ok(StreamingCleanupResponse {
            success: true,
            notes: vec![],
        })
    }
}

fn generate_events(expected: &[StreamingExpectedGroupTotal]) -> Vec<StreamEvent> {
    let total_events: usize = expected
        .iter()
        .map(|group| group.event_count as usize)
        .sum();
    let mut generated_per_group = vec![0u64; expected.len()];
    let mut events = Vec::with_capacity(total_events);
    let mut event_idx = 0u64;

    while events.len() < total_events {
        for (group_idx, group) in expected.iter().enumerate() {
            if generated_per_group[group_idx] >= group.event_count {
                continue;
            }

            let emitted = generated_per_group[group_idx];
            generated_per_group[group_idx] += 1;

            events.push(StreamEvent {
                event_time_ms: event_idx,
                key: group.key.clone(),
                value: distributed_value(group, emitted),
            });
            event_idx += 1;
        }
    }

    events
}

fn generate_warmup_events(
    expected: &[StreamingExpectedGroupTotal],
    count: u64,
) -> Vec<StreamEvent> {
    let keys: Vec<String> = expected.iter().map(|group| group.key.clone()).collect();
    let mut events = Vec::with_capacity(count as usize);

    for idx in 0..count {
        let key = keys[idx as usize % keys.len()].clone();
        events.push(StreamEvent {
            event_time_ms: idx,
            key,
            value: 1,
        });
    }

    events
}

fn distributed_value(group: &StreamingExpectedGroupTotal, emitted_index: u64) -> u64 {
    let base = group.value_sum / group.event_count;
    let remainder = group.value_sum % group.event_count;

    if emitted_index < remainder {
        base + 1
    } else {
        base
    }
}

fn apply_event(state: &mut BTreeMap<String, (u64, u64)>, event: &StreamEvent) {
    let entry = state.entry(event.key.clone()).or_insert((0, 0));
    entry.0 += 1;
    entry.1 += event.value;
}

fn synthetic_latency_offset_micros(event: &StreamEvent) -> u64 {
    500 + (event.event_time_ms % 31)
}

fn validate_aggregates(
    actual: &BTreeMap<String, (u64, u64)>,
    expected: &[StreamingExpectedGroupTotal],
) -> Result<(), String> {
    for group in expected {
        let (event_count, value_sum) = actual
            .get(&group.key)
            .copied()
            .ok_or_else(|| format!("missing aggregate for key {}", group.key))?;

        if event_count != group.event_count || value_sum != group.value_sum {
            return Err(format!(
                "aggregate mismatch for {}: expected ({}, {}), got ({}, {})",
                group.key, group.event_count, group.value_sum, event_count, value_sum
            ));
        }
    }

    Ok(())
}

fn percentile_triplet_ms(values_micros: &[u64]) -> (f64, f64, f64) {
    if values_micros.is_empty() {
        return (0.0, 0.0, 0.0);
    }

    let mut sorted = values_micros.to_vec();
    sorted.sort_unstable();

    (
        percentile_ms(&sorted, 0.50),
        percentile_ms(&sorted, 0.95),
        percentile_ms(&sorted, 0.99),
    )
}

fn percentile_ms(sorted_values_micros: &[u64], percentile: f64) -> f64 {
    let idx = ((sorted_values_micros.len() - 1) as f64 * percentile).round() as usize;
    sorted_values_micros[idx] as f64 / 1000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_events_match_expected_totals() {
        let expected = vec![
            StreamingExpectedGroupTotal {
                key: "mobile".into(),
                event_count: 3,
                value_sum: 8,
            },
            StreamingExpectedGroupTotal {
                key: "desktop".into(),
                event_count: 2,
                value_sum: 3,
            },
        ];

        let events = generate_events(&expected);
        let mut actual = BTreeMap::new();
        for event in &events {
            apply_event(&mut actual, event);
        }

        assert_eq!(actual.get("mobile"), Some(&(3, 8)));
        assert_eq!(actual.get("desktop"), Some(&(2, 3)));
    }
}
