use crate::record::{LiveInputEvent, SourceRecord};
use bm_schema::streaming::StreamingExpectedGroupTotal;
use std::io;
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::Sender;
use tokio::time::{sleep, Duration};

pub async fn run_source(
    tx: Sender<SourceRecord>,
    expected: &[StreamingExpectedGroupTotal],
    event_rate_per_sec: u64,
    duration_secs: u64,
    filter_value: Option<String>,
) {
    let templates = build_source_plan(expected);
    let start = Instant::now();
    let interval_ns = if event_rate_per_sec == 0 {
        0
    } else {
        1_000_000_000u64 / event_rate_per_sec
    };

    for (idx, (key, value)) in templates.into_iter().enumerate() {
        let created_at = Instant::now();
        let processing_time_ms = start.elapsed().as_millis() as u64;
        let record = SourceRecord {
            key,
            value,
            event_type: filter_value
                .clone()
                .unwrap_or_else(|| "page_view".to_string()),
            created_at,
            processing_time_ms,
        };

        if tx.send(record).await.is_err() {
            break;
        }

        if interval_ns > 0 && (idx as u64) + 1 < event_rate_per_sec.saturating_mul(duration_secs) {
            sleep(Duration::from_nanos(interval_ns)).await;
        }
    }
}

pub fn build_source_plan(expected: &[StreamingExpectedGroupTotal]) -> Vec<(String, u64)> {
    let total_events: usize = expected
        .iter()
        .map(|group| group.event_count as usize)
        .sum();
    let mut generated_per_group = vec![0u64; expected.len()];
    let mut out = Vec::with_capacity(total_events);

    while out.len() < total_events {
        for (idx, group) in expected.iter().enumerate() {
            if generated_per_group[idx] >= group.event_count {
                continue;
            }

            let emitted_index = generated_per_group[idx];
            generated_per_group[idx] += 1;
            out.push((group.key.clone(), distributed_value(group, emitted_index)));
        }
    }

    out
}

pub async fn run_live_tcp_source(
    listen_addr: &str,
    key_field: &str,
    tx: Sender<SourceRecord>,
) -> io::Result<()> {
    let listener = TcpListener::bind(listen_addr).await?;
    println!("mini_flink live source listening on {listen_addr}");

    let (socket, peer_addr) = listener.accept().await?;
    println!("mini_flink live source accepted producer connection from {peer_addr}");

    stream_live_socket(socket, key_field, tx).await
}

pub async fn run_live_tcp_client_source(
    connect_addr: &str,
    key_field: &str,
    tx: Sender<SourceRecord>,
) -> io::Result<()> {
    let socket = TcpStream::connect(connect_addr).await?;
    println!("mini_flink live source connected to feed at {connect_addr}");

    stream_live_socket(socket, key_field, tx).await
}

async fn stream_live_socket(
    socket: TcpStream,
    key_field: &str,
    tx: Sender<SourceRecord>,
) -> io::Result<()> {
    let reader = BufReader::new(socket);
    let mut lines = reader.lines();
    let start = Instant::now();

    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }

        let event: LiveInputEvent = serde_json::from_str(&line).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid live input event JSON: {error}"),
            )
        })?;

        let key = event.key_for_field(key_field).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("live input event missing key field {key_field}"),
            )
        })?;

        let record = SourceRecord {
            key,
            value: event.value,
            event_type: event.event_type,
            created_at: Instant::now(),
            processing_time_ms: event
                .event_time_ms
                .unwrap_or_else(|| start.elapsed().as_millis() as u64),
        };

        if tx.send(record).await.is_err() {
            break;
        }
    }

    println!("mini_flink live source connection closed");
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::build_source_plan;
    use crate::record::LiveInputEvent;
    use bm_schema::streaming::StreamingExpectedGroupTotal;

    #[test]
    fn source_plan_distributes_expected_counts() {
        let expected = vec![
            StreamingExpectedGroupTotal {
                key: "mobile".into(),
                event_count: 2,
                value_sum: 5,
            },
            StreamingExpectedGroupTotal {
                key: "desktop".into(),
                event_count: 1,
                value_sum: 4,
            },
        ];

        let plan = build_source_plan(&expected);
        assert_eq!(plan.len(), 3);
        assert_eq!(plan[0].0, "mobile");
        assert_eq!(plan[1].0, "desktop");
        assert_eq!(plan[2].0, "mobile");
    }

    #[test]
    fn live_event_resolves_supported_key_fields() {
        let event = LiveInputEvent {
            event_time_ms: Some(42),
            user_id: Some(7),
            session_id: Some(9),
            device_type: Some("mobile".into()),
            event_type: "page_view".into(),
            value: 3,
            key: Some("explicit".into()),
        };

        assert_eq!(
            event.key_for_field("device_type").as_deref(),
            Some("mobile")
        );
        assert_eq!(event.key_for_field("user_id").as_deref(), Some("7"));
        assert_eq!(event.key_for_field("key").as_deref(), Some("explicit"));
    }
}
