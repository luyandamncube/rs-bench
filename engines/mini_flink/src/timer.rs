use tokio::time::{interval, Duration, Interval};

pub const TIMER_TICK_MS: u64 = 25;

pub fn worker_tick_interval() -> Interval {
    interval(Duration::from_millis(TIMER_TICK_MS))
}
