use super::StreamingGraphSpec;
use bm_engine_mini_flink::MiniFlinkGraph;

pub const SPEC: StreamingGraphSpec = StreamingGraphSpec {
    name: "device_avg_demo",
    description: "Stdout mini_flink demo that emits count, sum, and avg per device window.",
    default_output: "workloads/streaming/jobs/device_avg_demo.yaml",
    default_config: "configs/streaming/mini_flink_device_avg_demo.toml",
    build,
};

pub fn build() -> MiniFlinkGraph {
    MiniFlinkGraph::source("clickstream")
        .named("mini_flink_device_avg_demo")
        .family("clickstream_streaming_demo")
        .description("Terminal demo with average value per device in a 1-second window.")
        .map("synthetic_clickstream_v1")
        .filter_eq("event_type", "page_view")
        .key_by("device_type")
        .window_tumbling_secs(1)
        .aggregate(
            vec!["count".to_string(), "sum".to_string(), "avg".to_string()],
            Some("event_count".to_string()),
            Some("value".to_string()),
            Some("value_sum".to_string()),
        )
        .sink_stdout()
        .expected_group_totals_from_tuples(vec![
            ("mobile", 2, 6),
            ("desktop", 2, 8),
            ("tablet", 2, 4),
            ("tv", 2, 10),
        ])
}

#[cfg(test)]
mod tests {
    use super::SPEC;

    #[test]
    fn device_avg_demo_spec_smoke_test() {
        super::super::assert_graph_spec_smoke(&SPEC);
    }
}
