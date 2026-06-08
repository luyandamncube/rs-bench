use serde::{Deserialize, Serialize};
use std::time::Instant;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveInputEvent {
    pub event_time_ms: Option<u64>,
    pub user_id: Option<u64>,
    pub session_id: Option<u64>,
    pub device_type: Option<String>,
    pub event_type: String,
    pub value: u64,
    pub key: Option<String>,
}

impl LiveInputEvent {
    pub fn key_for_field(&self, field: &str) -> Option<String> {
        match field {
            "key" => self.key.clone(),
            "device_type" => self.device_type.clone(),
            "user_id" => self.user_id.map(|value| value.to_string()),
            "session_id" => self.session_id.map(|value| value.to_string()),
            "event_type" => Some(self.event_type.clone()),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SourceRecord {
    pub key: String,
    pub value: u64,
    pub event_type: String,
    pub created_at: Instant,
    pub processing_time_ms: u64,
}

#[derive(Debug, Clone)]
pub struct ParsedRecord {
    pub key: String,
    pub value: u64,
    pub event_type: String,
    pub created_at: Instant,
    pub processing_time_ms: u64,
}

#[derive(Debug, Clone)]
pub struct WindowState {
    pub event_count: u64,
    pub value_sum: u64,
    pub window_start_ms: u64,
    pub window_end_ms: u64,
    pub last_seen_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkRecord {
    pub worker_partition: usize,
    pub key: String,
    pub window_start_ms: u64,
    pub window_end_ms: u64,
    pub event_count: u64,
    pub value_sum: u64,
    pub value_avg: Option<f64>,
}
