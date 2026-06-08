use crate::record::{ParsedRecord, SinkRecord, WindowState};
use crate::runtime::RuntimeMetrics;
use crate::timer::worker_tick_interval;
use crate::window::{apply_record, close_due_windows, flush_all_windows};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc::{Receiver, Sender};

pub async fn run_worker(
    worker_partition: usize,
    mut input_rx: Receiver<ParsedRecord>,
    sink_tx: Sender<SinkRecord>,
    window_size_ms: u64,
    aggregate_functions: Vec<String>,
    runtime_started_at: Instant,
    metrics: Arc<RuntimeMetrics>,
) {
    let mut state_by_key: HashMap<String, WindowState> = HashMap::new();
    let mut tick = worker_tick_interval();

    loop {
        tokio::select! {
            biased;
            maybe_record = input_rx.recv() => {
                match maybe_record {
                    Some(record) => {
                        let process_started = Instant::now();

                        if let Some(closed) = apply_record(
                            &mut state_by_key,
                            &record,
                            window_size_ms,
                            worker_partition,
                            &aggregate_functions,
                        ) {
                            metrics.increment_emitted_windows();
                            if sink_tx.send(closed).await.is_err() {
                                break;
                            }
                        }

                        metrics.record_latency(process_started.elapsed().as_micros() as u64 + record.created_at.elapsed().as_micros() as u64);
                        metrics.increment_records_emitted();
                    }
                    None => break,
                }
            }
            _ = tick.tick() => {
                let now_ms = runtime_started_at.elapsed().as_millis() as u64;
                let closed = close_due_windows(
                    &mut state_by_key,
                    now_ms,
                    worker_partition,
                    &aggregate_functions,
                );
                if !closed.is_empty() {
                    for row in closed {
                        metrics.increment_emitted_windows();
                        if sink_tx.send(row).await.is_err() {
                            return;
                        }
                    }
                }
            }
        }
    }

    for row in flush_all_windows(&mut state_by_key, worker_partition, &aggregate_functions) {
        metrics.increment_emitted_windows();
        if sink_tx.send(row).await.is_err() {
            return;
        }
    }
}
