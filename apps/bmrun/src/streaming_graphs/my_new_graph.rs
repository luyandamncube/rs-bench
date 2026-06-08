use super::StreamingGraphSpec;
use bm_engine_mini_flink::MiniFlinkGraph;

pub const SPEC: StreamingGraphSpec = StreamingGraphSpec {
    name: "my_new_graph",
    description: "My starter stream processor",
    default_output: "workloads/streaming/jobs/my_new_graph.yaml",
    default_config: "configs/streaming/mini_flink_my_new_graph.toml",
    build,
};

pub fn build() -> MiniFlinkGraph {
    MiniFlinkGraph::source("clickstream")
        .named("mini_flink_my_new_graph")
        .family("clickstream_streaming_demo")
        .description("My starter stream processor")
        .map("synthetic_clickstream_v1")
        .filter_eq("event_type", "page_view")
        .key_by("device_type")
        .window_tumbling_secs(1)
        .aggregate_count_sum("event_count", "value", "value_sum")
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
    fn my_new_graph_spec_smoke_test() {
        super::super::assert_graph_spec_smoke(&SPEC);
    }
}
