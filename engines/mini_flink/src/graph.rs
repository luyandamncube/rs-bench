use crate::record::{ParsedRecord, SinkRecord, SourceRecord};
use bm_schema::streaming::{
    StreamingEventSchemaField, StreamingExpectedGroupTotal, StreamingExpectedOutput,
    StreamingFilterSpec, StreamingParseSpec, StreamingPipelineOperator, StreamingScenario,
    StreamingSourceShape, StreamingWindowSpec, StreamingWorkloadDefinition,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use tokio::sync::mpsc::{self, Receiver, Sender};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pipeline {
    pub nodes: Vec<OperatorNode>,
    pub edges: Vec<Edge>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorNode {
    pub id: String,
    pub kind: OperatorKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum OperatorKind {
    Source {
        family: String,
    },
    Map {
        mode: String,
    },
    Filter {
        field: String,
        op: String,
        value: String,
    },
    KeyBy {
        field: String,
    },
    WindowTumbling {
        size_secs: u64,
    },
    Aggregate {
        functions: Vec<String>,
        count_as: Option<String>,
        sum_field: Option<String>,
        sum_as: Option<String>,
    },
    Sink {
        mode: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Edge {
    pub from: String,
    pub to: String,
}

impl Pipeline {
    pub fn ordered_nodes(&self) -> Result<Vec<&OperatorNode>, String> {
        if self.nodes.is_empty() {
            return Err("pipeline graph cannot be empty".to_string());
        }

        let node_by_id: HashMap<&str, &OperatorNode> = self
            .nodes
            .iter()
            .map(|node| (node.id.as_str(), node))
            .collect();
        let mut incoming: HashMap<&str, usize> = self
            .nodes
            .iter()
            .map(|node| (node.id.as_str(), 0_usize))
            .collect();
        let mut outgoing: HashMap<&str, usize> = self
            .nodes
            .iter()
            .map(|node| (node.id.as_str(), 0_usize))
            .collect();
        let mut next_by_id: HashMap<&str, &str> = HashMap::new();

        for edge in &self.edges {
            let from = edge.from.as_str();
            let to = edge.to.as_str();
            if !node_by_id.contains_key(from) {
                return Err(format!("edge references missing source node {}", edge.from));
            }
            if !node_by_id.contains_key(to) {
                return Err(format!(
                    "edge references missing destination node {}",
                    edge.to
                ));
            }

            *incoming.get_mut(to).unwrap() += 1;
            *outgoing.get_mut(from).unwrap() += 1;

            if next_by_id.insert(from, to).is_some() {
                return Err(format!(
                    "mini_flink graph only supports a single outgoing edge per node, found branch at {}",
                    edge.from
                ));
            }
        }

        let sources: Vec<&OperatorNode> = self
            .nodes
            .iter()
            .filter(|node| incoming.get(node.id.as_str()).copied().unwrap_or_default() == 0)
            .collect();
        if sources.len() != 1 {
            return Err(format!(
                "mini_flink graph must have exactly one source node, found {}",
                sources.len()
            ));
        }

        for node in &self.nodes {
            let in_degree = incoming.get(node.id.as_str()).copied().unwrap_or_default();
            let out_degree = outgoing.get(node.id.as_str()).copied().unwrap_or_default();
            if in_degree > 1 {
                return Err(format!(
                    "mini_flink graph only supports a single incoming edge per node, found fan-in at {}",
                    node.id
                ));
            }
            if out_degree > 1 {
                return Err(format!(
                    "mini_flink graph only supports a single outgoing edge per node, found fan-out at {}",
                    node.id
                ));
            }
        }

        let mut ordered = Vec::with_capacity(self.nodes.len());
        let mut current = sources[0];
        ordered.push(current);
        while let Some(next_id) = next_by_id.get(current.id.as_str()) {
            let next = node_by_id
                .get(next_id)
                .copied()
                .ok_or_else(|| format!("graph traversal references missing node {}", next_id))?;
            if ordered.iter().any(|node| node.id == next.id) {
                return Err("mini_flink graph contains a cycle".to_string());
            }
            ordered.push(next);
            current = next;
        }

        if ordered.len() != self.nodes.len() {
            return Err("mini_flink graph must be a single connected linear chain".to_string());
        }

        Ok(ordered)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphValidationReport {
    pub operator_sequence: Vec<String>,
    pub source_family: String,
    pub key_field: String,
    pub requires_keyed_state: bool,
    pub has_windowing: bool,
}

#[derive(Debug, Clone)]
struct GraphExecutionPlan {
    filter: Option<FilterConfig>,
    map_mode: Option<String>,
    window_size_ms: u64,
    sink_mode: String,
    aggregate_functions: Vec<String>,
    report: GraphValidationReport,
}

#[derive(Debug, Clone)]
pub struct CompiledPipeline {
    pub pipeline: Pipeline,
    pub validation_report: GraphValidationReport,
    pub filter: Option<FilterConfig>,
    pub map_mode: Option<String>,
    pub window_size_ms: u64,
    pub sink_mode: String,
    pub aggregate_functions: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct FilterConfig {
    pub field: String,
    pub op: String,
    pub value: Option<String>,
    pub values: Vec<String>,
}

pub struct PipelineBuilder {
    pipeline: Pipeline,
    last_id: Option<String>,
}

impl PipelineBuilder {
    pub fn source(family: impl Into<String>) -> Self {
        let mut builder = Self {
            pipeline: Pipeline {
                nodes: Vec::new(),
                edges: Vec::new(),
            },
            last_id: None,
        };
        builder.push(
            "source",
            OperatorKind::Source {
                family: family.into(),
            },
        );
        builder
    }

    pub fn map(mut self, mode: impl Into<String>) -> Self {
        self.push("map", OperatorKind::Map { mode: mode.into() });
        self
    }

    pub fn filter(
        mut self,
        field: impl Into<String>,
        op: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.push(
            "filter",
            OperatorKind::Filter {
                field: field.into(),
                op: op.into(),
                value: value.into(),
            },
        );
        self
    }

    pub fn key_by(mut self, field: impl Into<String>) -> Self {
        self.push(
            "key_by",
            OperatorKind::KeyBy {
                field: field.into(),
            },
        );
        self
    }

    pub fn window_tumbling_secs(mut self, size_secs: u64) -> Self {
        self.push("window", OperatorKind::WindowTumbling { size_secs });
        self
    }

    pub fn aggregate(
        mut self,
        functions: Vec<String>,
        count_as: Option<String>,
        sum_field: Option<String>,
        sum_as: Option<String>,
    ) -> Self {
        self.push(
            "aggregate",
            OperatorKind::Aggregate {
                functions,
                count_as,
                sum_field,
                sum_as,
            },
        );
        self
    }

    pub fn sink_file(mut self) -> Self {
        self.push(
            "sink",
            OperatorKind::Sink {
                mode: "file".to_string(),
            },
        );
        self
    }

    pub fn sink_stdout(mut self) -> Self {
        self.push(
            "sink",
            OperatorKind::Sink {
                mode: "stdout".to_string(),
            },
        );
        self
    }

    pub fn build(self) -> Pipeline {
        self.pipeline
    }

    fn push(&mut self, id: &str, kind: OperatorKind) {
        let next_id = format!("{id}_{}", self.pipeline.nodes.len());
        if let Some(prev) = &self.last_id {
            self.pipeline.edges.push(Edge {
                from: prev.clone(),
                to: next_id.clone(),
            });
        }
        self.pipeline.nodes.push(OperatorNode {
            id: next_id.clone(),
            kind,
        });
        self.last_id = Some(next_id);
    }
}

pub struct WorkloadAuthoringBuilder {
    name: String,
    family: String,
    description: String,
    source_family: String,
    event_time_field: String,
    key_field: String,
    parse_mode: Option<String>,
    filter_field: Option<String>,
    filter_op: Option<String>,
    filter_value: Option<String>,
    filter_values: Vec<String>,
    window_size_secs: u64,
    aggregate_functions: Vec<String>,
    aggregate_count_as: Option<String>,
    aggregate_sum_field: Option<String>,
    aggregate_sum_as: Option<String>,
    sink_mode: String,
    expected_group_totals: Vec<StreamingExpectedGroupTotal>,
}

impl WorkloadAuthoringBuilder {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            family: "clickstream_streaming".to_string(),
            description: "Generated mini_flink workload".to_string(),
            source_family: "clickstream".to_string(),
            event_time_field: "event_time".to_string(),
            key_field: "device_type".to_string(),
            parse_mode: None,
            filter_field: None,
            filter_op: None,
            filter_value: None,
            filter_values: Vec::new(),
            window_size_secs: 3,
            aggregate_functions: vec!["count".to_string(), "sum".to_string()],
            aggregate_count_as: Some("event_count".to_string()),
            aggregate_sum_field: Some("value".to_string()),
            aggregate_sum_as: Some("value_sum".to_string()),
            sink_mode: "file".to_string(),
            expected_group_totals: Vec::new(),
        }
    }

    pub fn family(mut self, family: impl Into<String>) -> Self {
        self.family = family.into();
        self
    }

    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    pub fn source(mut self, family: impl Into<String>) -> Self {
        self.source_family = family.into();
        self
    }

    pub fn event_time_field(mut self, field: impl Into<String>) -> Self {
        self.event_time_field = field.into();
        self
    }

    pub fn map(mut self, mode: impl Into<String>) -> Self {
        self.parse_mode = Some(mode.into());
        self
    }

    pub fn filter(mut self, field: impl Into<String>, value: impl Into<String>) -> Self {
        self.filter_field = Some(field.into());
        self.filter_op = Some("eq".to_string());
        self.filter_value = Some(value.into());
        self.filter_values.clear();
        self
    }

    pub fn filter_with_op(
        mut self,
        field: impl Into<String>,
        op: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.filter_field = Some(field.into());
        self.filter_op = Some(op.into());
        self.filter_value = Some(value.into());
        self.filter_values.clear();
        self
    }

    pub fn filter_in(mut self, field: impl Into<String>, values: Vec<String>) -> Self {
        self.filter_field = Some(field.into());
        self.filter_op = Some("in".to_string());
        self.filter_value = None;
        self.filter_values = values;
        self
    }

    pub fn key_by(mut self, field: impl Into<String>) -> Self {
        self.key_field = field.into();
        self
    }

    pub fn window_tumbling_secs(mut self, size_secs: u64) -> Self {
        self.window_size_secs = size_secs;
        self
    }

    pub fn aggregate(
        mut self,
        functions: Vec<String>,
        count_as: Option<String>,
        sum_field: Option<String>,
        sum_as: Option<String>,
    ) -> Self {
        self.aggregate_functions = functions;
        self.aggregate_count_as = count_as;
        self.aggregate_sum_field = sum_field;
        self.aggregate_sum_as = sum_as;
        self
    }

    pub fn sink_file(mut self) -> Self {
        self.sink_mode = "file".to_string();
        self
    }

    pub fn sink_stdout(mut self) -> Self {
        self.sink_mode = "stdout".to_string();
        self
    }

    pub fn expected_group_totals(mut self, totals: Vec<StreamingExpectedGroupTotal>) -> Self {
        self.expected_group_totals = totals;
        self
    }

    pub fn expected_group_totals_from_tuples(
        mut self,
        totals: Vec<(impl Into<String>, u64, u64)>,
    ) -> Self {
        self.expected_group_totals = totals
            .into_iter()
            .map(
                |(key, event_count, value_sum)| StreamingExpectedGroupTotal {
                    key: key.into(),
                    event_count,
                    value_sum,
                },
            )
            .collect();
        self
    }

    pub fn build(self) -> StreamingWorkloadDefinition {
        let mut pipeline = vec![StreamingPipelineOperator {
            kind: "source".to_string(),
            family: Some(self.source_family.clone()),
            mode: None,
            field: None,
            op: None,
            equals: None,
            values: None,
            size_secs: None,
            count_as: None,
            sum_field: None,
            sum_as: None,
            aggregate_functions: None,
            sink_mode: None,
        }];

        if let Some(mode) = &self.parse_mode {
            pipeline.push(StreamingPipelineOperator {
                kind: "map".to_string(),
                family: None,
                mode: Some(mode.clone()),
                field: None,
                op: None,
                equals: None,
                values: None,
                size_secs: None,
                count_as: None,
                sum_field: None,
                sum_as: None,
                aggregate_functions: None,
                sink_mode: None,
            });
        }

        if let Some(field) = &self.filter_field {
            pipeline.push(StreamingPipelineOperator {
                kind: "filter".to_string(),
                family: None,
                mode: None,
                field: Some(field.clone()),
                op: self.filter_op.clone(),
                equals: self.filter_value.clone(),
                values: if self.filter_values.is_empty() {
                    None
                } else {
                    Some(self.filter_values.clone())
                },
                size_secs: None,
                count_as: None,
                sum_field: None,
                sum_as: None,
                aggregate_functions: None,
                sink_mode: None,
            });
        }

        pipeline.push(StreamingPipelineOperator {
            kind: "key_by".to_string(),
            family: None,
            mode: None,
            field: Some(self.key_field.clone()),
            op: None,
            equals: None,
            values: None,
            size_secs: None,
            count_as: None,
            sum_field: None,
            sum_as: None,
            aggregate_functions: None,
            sink_mode: None,
        });
        pipeline.push(StreamingPipelineOperator {
            kind: "window".to_string(),
            family: None,
            mode: None,
            field: None,
            op: None,
            equals: None,
            values: None,
            size_secs: Some(self.window_size_secs),
            count_as: None,
            sum_field: None,
            sum_as: None,
            aggregate_functions: None,
            sink_mode: None,
        });
        pipeline.push(StreamingPipelineOperator {
            kind: "aggregate".to_string(),
            family: None,
            mode: None,
            field: None,
            op: None,
            equals: None,
            values: None,
            size_secs: None,
            count_as: self.aggregate_count_as.clone(),
            sum_field: self.aggregate_sum_field.clone(),
            sum_as: self.aggregate_sum_as.clone(),
            aggregate_functions: Some(self.aggregate_functions.clone()),
            sink_mode: None,
        });
        pipeline.push(StreamingPipelineOperator {
            kind: "sink".to_string(),
            family: None,
            mode: None,
            field: None,
            op: None,
            equals: None,
            values: None,
            size_secs: None,
            count_as: None,
            sum_field: None,
            sum_as: None,
            aggregate_functions: None,
            sink_mode: Some(self.sink_mode.clone()),
        });

        let filter = self.filter_field.map(|field| StreamingFilterSpec {
            field,
            op: self.filter_op,
            equals: self.filter_value,
            values: if self.filter_values.is_empty() {
                None
            } else {
                Some(self.filter_values)
            },
        });

        StreamingWorkloadDefinition {
            name: self.name.clone(),
            family: self.family,
            description: self.description,
            source: StreamingSourceShape {
                family: self.source_family,
                event_time_field: self.event_time_field,
                key_field: self.key_field.clone(),
            },
            parse: self.parse_mode.map(|mode| StreamingParseSpec { mode }),
            filter,
            schema: default_clickstream_schema(),
            scenario: StreamingScenario {
                name: format!("{}_pipeline", self.name),
                operation: "tumbling_window_grouped_sum".to_string(),
                group_by: vec![self.key_field.clone()],
                aggregate_count_as: self.aggregate_count_as.clone(),
                aggregate_sum_field: self.aggregate_sum_field.clone(),
                aggregate_sum_as: self.aggregate_sum_as.clone(),
            },
            window: StreamingWindowSpec {
                window_type: "tumbling".to_string(),
                size_secs: self.window_size_secs,
                slide_secs: None,
            },
            pipeline: Some(pipeline),
            expected_output: StreamingExpectedOutput {
                aggregate_by: self.key_field,
                value_field: self
                    .aggregate_sum_field
                    .unwrap_or_else(|| "value".to_string()),
                expected_group_totals: self.expected_group_totals,
            },
        }
    }

    pub fn to_yaml_string(self) -> Result<String, serde_yaml::Error> {
        serde_yaml::to_string(&self.build())
    }

    pub fn write_yaml(self, path: impl AsRef<Path>) -> Result<(), Box<dyn std::error::Error>> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, self.to_yaml_string()?)?;
        Ok(())
    }
}

pub struct MiniFlinkGraph {
    inner: WorkloadAuthoringBuilder,
}

impl MiniFlinkGraph {
    pub fn source(family: impl Into<String>) -> Self {
        Self {
            inner: WorkloadAuthoringBuilder::new("streaming_pipeline").source(family),
        }
    }

    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.inner = self.inner.name(name);
        self
    }

    pub fn family(mut self, family: impl Into<String>) -> Self {
        self.inner = self.inner.family(family);
        self
    }

    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.inner = self.inner.description(description);
        self
    }

    pub fn event_time_field(mut self, field: impl Into<String>) -> Self {
        self.inner = self.inner.event_time_field(field);
        self
    }

    pub fn map(mut self, mode: impl Into<String>) -> Self {
        self.inner = self.inner.map(mode);
        self
    }

    pub fn filter_eq(mut self, field: impl Into<String>, value: impl Into<String>) -> Self {
        self.inner = self.inner.filter_with_op(field, "eq", value);
        self
    }

    pub fn filter_ne(mut self, field: impl Into<String>, value: impl Into<String>) -> Self {
        self.inner = self.inner.filter_with_op(field, "ne", value);
        self
    }

    pub fn filter_gt(mut self, field: impl Into<String>, value: impl Into<String>) -> Self {
        self.inner = self.inner.filter_with_op(field, "gt", value);
        self
    }

    pub fn filter_gte(mut self, field: impl Into<String>, value: impl Into<String>) -> Self {
        self.inner = self.inner.filter_with_op(field, "gte", value);
        self
    }

    pub fn filter_lt(mut self, field: impl Into<String>, value: impl Into<String>) -> Self {
        self.inner = self.inner.filter_with_op(field, "lt", value);
        self
    }

    pub fn filter_lte(mut self, field: impl Into<String>, value: impl Into<String>) -> Self {
        self.inner = self.inner.filter_with_op(field, "lte", value);
        self
    }

    pub fn filter_in(mut self, field: impl Into<String>, values: Vec<String>) -> Self {
        self.inner = self.inner.filter_in(field, values);
        self
    }

    pub fn key_by(mut self, field: impl Into<String>) -> Self {
        self.inner = self.inner.key_by(field);
        self
    }

    pub fn window_tumbling_secs(mut self, size_secs: u64) -> Self {
        self.inner = self.inner.window_tumbling_secs(size_secs);
        self
    }

    pub fn aggregate(
        mut self,
        functions: Vec<String>,
        count_as: Option<String>,
        sum_field: Option<String>,
        sum_as: Option<String>,
    ) -> Self {
        self.inner = self.inner.aggregate(functions, count_as, sum_field, sum_as);
        self
    }

    pub fn aggregate_count_sum(
        mut self,
        count_as: impl Into<String>,
        sum_field: impl Into<String>,
        sum_as: impl Into<String>,
    ) -> Self {
        self.inner = self.inner.aggregate(
            vec!["count".to_string(), "sum".to_string()],
            Some(count_as.into()),
            Some(sum_field.into()),
            Some(sum_as.into()),
        );
        self
    }

    pub fn sink_file(mut self) -> Self {
        self.inner = self.inner.sink_file();
        self
    }

    pub fn sink_stdout(mut self) -> Self {
        self.inner = self.inner.sink_stdout();
        self
    }

    pub fn expected_group_totals(mut self, totals: Vec<StreamingExpectedGroupTotal>) -> Self {
        self.inner = self.inner.expected_group_totals(totals);
        self
    }

    pub fn expected_group_totals_from_tuples(
        mut self,
        totals: Vec<(impl Into<String>, u64, u64)>,
    ) -> Self {
        self.inner = self.inner.expected_group_totals_from_tuples(totals);
        self
    }

    pub fn build_workload(self) -> StreamingWorkloadDefinition {
        self.inner.build()
    }

    pub fn compile(self) -> Result<CompiledPipeline, String> {
        let workload = self.build_workload();
        compile_pipeline(&workload, "file")
    }

    pub fn render(self) -> Result<String, String> {
        self.compile()
            .map(|compiled| render_pipeline(&compiled.pipeline))
    }

    pub fn to_yaml_string(self) -> Result<String, serde_yaml::Error> {
        serde_yaml::to_string(&self.build_workload())
    }

    pub fn write_yaml(self, path: impl AsRef<Path>) -> Result<(), Box<dyn std::error::Error>> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, self.to_yaml_string()?)?;
        Ok(())
    }
}

fn default_clickstream_schema() -> Vec<StreamingEventSchemaField> {
    vec![
        StreamingEventSchemaField {
            name: "event_time".to_string(),
            data_type: "timestamp_ms".to_string(),
        },
        StreamingEventSchemaField {
            name: "user_id".to_string(),
            data_type: "u64".to_string(),
        },
        StreamingEventSchemaField {
            name: "session_id".to_string(),
            data_type: "u64".to_string(),
        },
        StreamingEventSchemaField {
            name: "device_type".to_string(),
            data_type: "string".to_string(),
        },
        StreamingEventSchemaField {
            name: "event_type".to_string(),
            data_type: "string".to_string(),
        },
        StreamingEventSchemaField {
            name: "value".to_string(),
            data_type: "u64".to_string(),
        },
    ]
}

pub fn compile_pipeline(
    workload: &StreamingWorkloadDefinition,
    sink_mode: &str,
) -> Result<CompiledPipeline, String> {
    let pipeline = if let Some(operators) = &workload.pipeline {
        build_pipeline_from_operators(workload, operators, sink_mode)?
    } else {
        build_inferred_pipeline(workload)
    };

    let plan = validate_pipeline(&pipeline, workload, sink_mode)?;

    Ok(CompiledPipeline {
        pipeline,
        validation_report: plan.report.clone(),
        filter: plan.filter,
        map_mode: plan.map_mode,
        window_size_ms: plan.window_size_ms,
        sink_mode: plan.sink_mode,
        aggregate_functions: plan.aggregate_functions,
    })
}

fn build_inferred_pipeline(workload: &StreamingWorkloadDefinition) -> Pipeline {
    let mut builder = PipelineBuilder::source(workload.source.family.clone());

    if let Some(parse) = &workload.parse {
        builder = builder.map(parse.mode.clone());
    }
    if let Some(filter) = &workload.filter {
        let op = filter.op.clone().unwrap_or_else(|| "eq".to_string());
        if op == "in" {
            builder = builder.filter(
                filter.field.clone(),
                op.clone(),
                format!("[{}]", filter.values.clone().unwrap_or_default().join(",")),
            );
        } else {
            builder = builder.filter(
                filter.field.clone(),
                op.clone(),
                filter.equals.clone().unwrap_or_default(),
            );
        }
    }

    let aggregate_functions = infer_aggregate_functions(
        workload.scenario.aggregate_count_as.is_some(),
        workload.scenario.aggregate_sum_field.is_some(),
        false,
    );

    builder
        .key_by(workload.source.key_field.clone())
        .window_tumbling_secs(workload.window.size_secs)
        .aggregate(
            aggregate_functions,
            workload.scenario.aggregate_count_as.clone(),
            workload.scenario.aggregate_sum_field.clone(),
            workload.scenario.aggregate_sum_as.clone(),
        )
        .sink_file()
        .build()
}

fn build_pipeline_from_operators(
    workload: &StreamingWorkloadDefinition,
    operators: &[StreamingPipelineOperator],
    default_sink_mode: &str,
) -> Result<Pipeline, String> {
    if operators.is_empty() {
        return Err("pipeline cannot be empty".to_string());
    }

    let mut builder: Option<PipelineBuilder> = None;

    for (idx, operator) in operators.iter().enumerate() {
        match operator.kind.as_str() {
            "source" => {
                if idx != 0 {
                    return Err("source must be the first operator".to_string());
                }
                let family = operator
                    .family
                    .clone()
                    .unwrap_or_else(|| workload.source.family.clone());
                builder = Some(PipelineBuilder::source(family));
            }
            "map" => {
                let mode = operator
                    .mode
                    .clone()
                    .ok_or_else(|| "map operator requires mode".to_string())?;
                builder = Some(
                    builder
                        .ok_or_else(|| "map requires a preceding source".to_string())?
                        .map(mode),
                );
            }
            "filter" => {
                let field = operator
                    .field
                    .clone()
                    .ok_or_else(|| "filter operator requires field".to_string())?;
                let op = operator.op.clone().unwrap_or_else(|| "eq".to_string());
                let value = operator.equals.clone();
                let values = operator.values.clone().unwrap_or_default();
                if op == "in" && values.is_empty() {
                    return Err("filter operator with op=in requires values".to_string());
                }
                if op != "in" && value.is_none() {
                    return Err(format!("filter operator with op={} requires equals", op));
                }
                builder = Some(
                    builder
                        .ok_or_else(|| "filter requires a preceding operator".to_string())?
                        .filter(
                            field,
                            op,
                            if values.is_empty() {
                                value.unwrap_or_default()
                            } else {
                                format!("[{}]", values.join(","))
                            },
                        ),
                );
            }
            "key_by" => {
                let field = operator
                    .field
                    .clone()
                    .ok_or_else(|| "key_by operator requires field".to_string())?;
                builder = Some(
                    builder
                        .ok_or_else(|| "key_by requires a preceding operator".to_string())?
                        .key_by(field),
                );
            }
            "window" => {
                let size_secs = operator
                    .size_secs
                    .ok_or_else(|| "window operator requires size_secs".to_string())?;
                builder = Some(
                    builder
                        .ok_or_else(|| "window requires a preceding operator".to_string())?
                        .window_tumbling_secs(size_secs),
                );
            }
            "aggregate" => {
                let aggregate_functions =
                    operator.aggregate_functions.clone().unwrap_or_else(|| {
                        infer_aggregate_functions(
                            operator.count_as.is_some(),
                            operator.sum_field.is_some(),
                            false,
                        )
                    });
                validate_aggregate_functions(&aggregate_functions)?;
                builder = Some(
                    builder
                        .ok_or_else(|| "aggregate requires a preceding operator".to_string())?
                        .aggregate(
                            aggregate_functions,
                            operator.count_as.clone(),
                            operator.sum_field.clone(),
                            operator.sum_as.clone(),
                        ),
                );
            }
            "sink" => {
                let resolved_sink_mode = operator
                    .sink_mode
                    .clone()
                    .unwrap_or_else(|| default_sink_mode.to_string());
                match resolved_sink_mode.as_str() {
                    "file" => {
                        builder = Some(
                            builder
                                .ok_or_else(|| "sink requires a preceding operator".to_string())?
                                .sink_file(),
                        );
                    }
                    "stdout" => {
                        builder = Some(
                            builder
                                .ok_or_else(|| "sink requires a preceding operator".to_string())?
                                .sink_stdout(),
                        );
                    }
                    other => return Err(format!("unsupported sink mode {}", other)),
                }
            }
            other => return Err(format!("unsupported operator kind {}", other)),
        }
    }

    builder
        .ok_or_else(|| "pipeline must start with source".to_string())
        .map(PipelineBuilder::build)
}

fn validate_pipeline(
    pipeline: &Pipeline,
    workload: &StreamingWorkloadDefinition,
    default_sink_mode: &str,
) -> Result<GraphExecutionPlan, String> {
    let ordered = pipeline.ordered_nodes()?;
    let mut map_mode = None;
    let mut filter = None;
    let mut key_field = None;
    let mut window_size_ms = None;
    let mut sink_mode = None;
    let mut aggregate_functions = Vec::new();
    let mut source_family = None;
    let mut saw_key_by = false;
    let mut saw_window = false;
    let mut saw_aggregate = false;
    let mut saw_sink = false;
    let mut expected_stage = 0_usize;
    let operator_sequence = ordered
        .iter()
        .map(|node| match &node.kind {
            OperatorKind::Source { .. } => "source".to_string(),
            OperatorKind::Map { .. } => "map".to_string(),
            OperatorKind::Filter { .. } => "filter".to_string(),
            OperatorKind::KeyBy { .. } => "key_by".to_string(),
            OperatorKind::WindowTumbling { .. } => "window".to_string(),
            OperatorKind::Aggregate { .. } => "aggregate".to_string(),
            OperatorKind::Sink { .. } => "sink".to_string(),
        })
        .collect::<Vec<_>>();

    for node in ordered {
        match &node.kind {
            OperatorKind::Source { family } => {
                if expected_stage != 0 {
                    return Err("source must be the first operator".to_string());
                }
                source_family = Some(family.clone());
                expected_stage = 1;
            }
            OperatorKind::Map { mode } => {
                if expected_stage > 2 {
                    return Err("map must appear before key_by/window/aggregate".to_string());
                }
                map_mode = Some(mode.clone());
                expected_stage = 2;
            }
            OperatorKind::Filter { field, op, value } => {
                if expected_stage > 2 {
                    return Err("filter must appear before key_by/window/aggregate".to_string());
                }
                let values = if op == "in" {
                    parse_list_value(value)
                } else {
                    Vec::new()
                };
                if op == "in" && values.is_empty() {
                    return Err("filter operator with op=in requires values".to_string());
                }
                if op != "in" && value.is_empty() {
                    return Err(format!("filter operator with op={} requires a value", op));
                }
                filter = Some(FilterConfig {
                    field: field.clone(),
                    op: op.clone(),
                    value: if op == "in" {
                        None
                    } else {
                        Some(value.clone())
                    },
                    values,
                });
                expected_stage = 2;
            }
            OperatorKind::KeyBy { field } => {
                if expected_stage > 3 {
                    return Err("key_by must appear before window/aggregate/sink".to_string());
                }
                if field != &workload.source.key_field {
                    return Err(format!(
                        "key_by field {} does not match source.key_field {}",
                        field, workload.source.key_field
                    ));
                }
                key_field = Some(field.clone());
                saw_key_by = true;
                expected_stage = 4;
            }
            OperatorKind::WindowTumbling { size_secs } => {
                if !saw_key_by {
                    return Err("window requires a preceding key_by".to_string());
                }
                if saw_window {
                    return Err("mini_flink supports only one window operator".to_string());
                }
                window_size_ms = Some(size_secs.saturating_mul(1000));
                saw_window = true;
                expected_stage = 5;
            }
            OperatorKind::Aggregate {
                functions,
                count_as,
                sum_field,
                ..
            } => {
                if !saw_key_by {
                    return Err("aggregate requires a preceding key_by".to_string());
                }
                if !saw_window {
                    return Err("aggregate requires a preceding window".to_string());
                }
                if saw_aggregate {
                    return Err("mini_flink supports only one aggregate operator".to_string());
                }
                let resolved_functions = if functions.is_empty() {
                    infer_aggregate_functions(count_as.is_some(), sum_field.is_some(), false)
                } else {
                    functions.clone()
                };
                validate_aggregate_functions(&resolved_functions)?;
                aggregate_functions = resolved_functions;
                saw_aggregate = true;
                expected_stage = 6;
            }
            OperatorKind::Sink { mode } => {
                if !saw_aggregate {
                    return Err("sink requires a preceding aggregate".to_string());
                }
                if saw_sink {
                    return Err("mini_flink supports only one sink operator".to_string());
                }
                if mode != "file" && mode != "stdout" {
                    return Err(format!("unsupported sink mode {}", mode));
                }
                sink_mode = Some(mode.clone());
                saw_sink = true;
                expected_stage = 7;
            }
        }
    }

    if !saw_key_by {
        return Err("pipeline must contain key_by".to_string());
    }
    if !saw_window {
        return Err("pipeline must contain window".to_string());
    }
    if !saw_aggregate {
        return Err("pipeline must contain aggregate".to_string());
    }
    if !saw_sink {
        return Err("pipeline must end with sink".to_string());
    }

    let source_family = source_family.unwrap_or_else(|| workload.source.family.clone());
    let key_field = key_field.unwrap_or_else(|| workload.source.key_field.clone());
    let report = GraphValidationReport {
        operator_sequence,
        source_family: source_family.clone(),
        key_field: key_field.clone(),
        requires_keyed_state: saw_key_by,
        has_windowing: saw_window,
    };

    Ok(GraphExecutionPlan {
        filter,
        map_mode,
        window_size_ms: window_size_ms
            .unwrap_or_else(|| workload.window.size_secs.saturating_mul(1000)),
        sink_mode: sink_mode.unwrap_or_else(|| default_sink_mode.to_string()),
        aggregate_functions,
        report,
    })
}

pub fn render_pipeline(pipeline: &Pipeline) -> String {
    let mut lines = Vec::new();
    let ordered = pipeline
        .ordered_nodes()
        .map(|nodes| nodes.into_iter().collect::<Vec<_>>())
        .unwrap_or_else(|_| pipeline.nodes.iter().collect::<Vec<_>>());

    for (idx, node) in ordered.into_iter().enumerate() {
        let prefix = if idx == 0 { "" } else { "  ." };
        let line = match &node.kind {
            OperatorKind::Source { family } => format!("{prefix}source({family})"),
            OperatorKind::Map { mode } => format!("{prefix}map({mode})"),
            OperatorKind::Filter { field, op, value } => {
                format!("{prefix}filter({field} {op} {value})")
            }
            OperatorKind::KeyBy { field } => format!("{prefix}key_by({field})"),
            OperatorKind::WindowTumbling { size_secs } => {
                format!("{prefix}window(tumbling_{size_secs}s)")
            }
            OperatorKind::Aggregate {
                functions,
                count_as,
                sum_field,
                sum_as,
            } => format!(
                "{prefix}aggregate({:?}, count={}, sum({})={})",
                functions,
                count_as.clone().unwrap_or_else(|| "count".to_string()),
                sum_field.clone().unwrap_or_else(|| "value".to_string()),
                sum_as.clone().unwrap_or_else(|| "sum".to_string())
            ),
            OperatorKind::Sink { mode } => format!("{prefix}sink({mode})"),
        };
        lines.push(line);
    }

    lines.join("\n")
}

pub struct RuntimeGraph {
    pub source_tx: Sender<SourceRecord>,
    pub source_rx: Receiver<SourceRecord>,
    pub worker_txs: Vec<Sender<ParsedRecord>>,
    pub worker_rxs: Vec<Receiver<ParsedRecord>>,
    pub sink_tx: Sender<SinkRecord>,
    pub sink_rx: Receiver<SinkRecord>,
}

pub fn build_runtime_graph(keyed_parallelism: usize, channel_capacity: usize) -> RuntimeGraph {
    let (source_tx, source_rx) = mpsc::channel(channel_capacity);
    let (sink_tx, sink_rx) = mpsc::channel(channel_capacity);

    let mut worker_txs = Vec::with_capacity(keyed_parallelism);
    let mut worker_rxs = Vec::with_capacity(keyed_parallelism);

    for _ in 0..keyed_parallelism {
        let (tx, rx) = mpsc::channel(channel_capacity);
        worker_txs.push(tx);
        worker_rxs.push(rx);
    }

    RuntimeGraph {
        source_tx,
        source_rx,
        worker_txs,
        worker_rxs,
        sink_tx,
        sink_rx,
    }
}

fn infer_aggregate_functions(has_count: bool, has_sum: bool, has_avg: bool) -> Vec<String> {
    let mut functions = Vec::new();
    if has_count {
        functions.push("count".to_string());
    }
    if has_sum {
        functions.push("sum".to_string());
    }
    if has_avg {
        functions.push("avg".to_string());
    }
    functions
}

fn parse_list_value(value: &str) -> Vec<String> {
    value
        .trim()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn validate_aggregate_functions(functions: &[String]) -> Result<(), String> {
    if functions.is_empty() {
        return Err("aggregate operator requires at least one aggregate function".to_string());
    }
    for function in functions {
        match function.as_str() {
            "count" | "sum" | "avg" => {}
            other => return Err(format!("unsupported aggregate function {}", other)),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        compile_pipeline, render_pipeline, Edge, MiniFlinkGraph, OperatorKind, Pipeline,
        PipelineBuilder, WorkloadAuthoringBuilder,
    };
    use bm_schema::streaming::{
        StreamingExpectedOutput, StreamingPipelineOperator, StreamingScenario,
        StreamingSourceShape, StreamingWindowSpec, StreamingWorkloadDefinition,
    };

    #[test]
    fn compiled_pipeline_renders_expected_chain() {
        let workload = StreamingWorkloadDefinition {
            name: "w".into(),
            family: "f".into(),
            description: "d".into(),
            source: StreamingSourceShape {
                family: "clickstream".into(),
                event_time_field: "event_time".into(),
                key_field: "device_type".into(),
            },
            parse: Some(bm_schema::streaming::StreamingParseSpec {
                mode: "synthetic_clickstream_v1".into(),
            }),
            filter: Some(bm_schema::streaming::StreamingFilterSpec {
                field: "event_type".into(),
                op: Some("eq".into()),
                equals: Some("page_view".into()),
                values: None,
            }),
            schema: vec![],
            scenario: StreamingScenario {
                name: "agg".into(),
                operation: "tumbling_window_grouped_sum".into(),
                group_by: vec!["device_type".into()],
                aggregate_count_as: Some("event_count".into()),
                aggregate_sum_field: Some("value".into()),
                aggregate_sum_as: Some("value_sum".into()),
            },
            window: StreamingWindowSpec {
                window_type: "tumbling".into(),
                size_secs: 3,
                slide_secs: None,
            },
            pipeline: None,
            expected_output: StreamingExpectedOutput {
                aggregate_by: "device_type".into(),
                value_field: "value".into(),
                expected_group_totals: vec![],
            },
        };

        let compiled = compile_pipeline(&workload, "file").unwrap();
        let rendered = render_pipeline(&compiled.pipeline);

        assert!(rendered.contains("source(clickstream)"));
        assert!(rendered.contains(".map(synthetic_clickstream_v1)"));
        assert!(rendered.contains(".filter(event_type eq page_view)"));
        assert!(rendered.contains(".key_by(device_type)"));
        assert!(rendered.contains(".window(tumbling_3s)"));
        assert!(rendered.contains(".aggregate([\"count\", \"sum\"]"));
        assert!(rendered.contains(".sink(file)"));
    }

    #[test]
    fn explicit_pipeline_renders_expected_chain() {
        let workload = StreamingWorkloadDefinition {
            name: "w".into(),
            family: "f".into(),
            description: "d".into(),
            source: StreamingSourceShape {
                family: "clickstream".into(),
                event_time_field: "event_time".into(),
                key_field: "device_type".into(),
            },
            parse: None,
            filter: None,
            schema: vec![],
            scenario: StreamingScenario {
                name: "agg".into(),
                operation: "tumbling_window_grouped_sum".into(),
                group_by: vec!["device_type".into()],
                aggregate_count_as: Some("event_count".into()),
                aggregate_sum_field: Some("value".into()),
                aggregate_sum_as: Some("value_sum".into()),
            },
            window: StreamingWindowSpec {
                window_type: "tumbling".into(),
                size_secs: 3,
                slide_secs: None,
            },
            pipeline: Some(vec![
                StreamingPipelineOperator {
                    kind: "source".into(),
                    family: Some("clickstream".into()),
                    mode: None,
                    field: None,
                    op: None,
                    equals: None,
                    values: None,
                    size_secs: None,
                    count_as: None,
                    sum_field: None,
                    sum_as: None,
                    aggregate_functions: None,
                    sink_mode: None,
                },
                StreamingPipelineOperator {
                    kind: "map".into(),
                    family: None,
                    mode: Some("synthetic_clickstream_v1".into()),
                    field: None,
                    op: None,
                    equals: None,
                    values: None,
                    size_secs: None,
                    count_as: None,
                    sum_field: None,
                    sum_as: None,
                    aggregate_functions: None,
                    sink_mode: None,
                },
                StreamingPipelineOperator {
                    kind: "filter".into(),
                    family: None,
                    mode: None,
                    field: Some("event_type".into()),
                    op: Some("in".into()),
                    equals: None,
                    values: Some(vec!["page_view".into(), "purchase".into()]),
                    size_secs: None,
                    count_as: None,
                    sum_field: None,
                    sum_as: None,
                    aggregate_functions: None,
                    sink_mode: None,
                },
                StreamingPipelineOperator {
                    kind: "key_by".into(),
                    family: None,
                    mode: None,
                    field: Some("device_type".into()),
                    op: None,
                    equals: None,
                    values: None,
                    size_secs: None,
                    count_as: None,
                    sum_field: None,
                    sum_as: None,
                    aggregate_functions: None,
                    sink_mode: None,
                },
                StreamingPipelineOperator {
                    kind: "window".into(),
                    family: None,
                    mode: None,
                    field: None,
                    op: None,
                    equals: None,
                    values: None,
                    size_secs: Some(3),
                    count_as: None,
                    sum_field: None,
                    sum_as: None,
                    aggregate_functions: None,
                    sink_mode: None,
                },
                StreamingPipelineOperator {
                    kind: "aggregate".into(),
                    family: None,
                    mode: None,
                    field: None,
                    op: None,
                    equals: None,
                    values: None,
                    size_secs: None,
                    count_as: Some("event_count".into()),
                    sum_field: Some("value".into()),
                    sum_as: Some("value_sum".into()),
                    aggregate_functions: Some(vec!["count".into(), "avg".into()]),
                    sink_mode: None,
                },
                StreamingPipelineOperator {
                    kind: "sink".into(),
                    family: None,
                    mode: None,
                    field: None,
                    op: None,
                    equals: None,
                    values: None,
                    size_secs: None,
                    count_as: None,
                    sum_field: None,
                    sum_as: None,
                    aggregate_functions: None,
                    sink_mode: Some("file".into()),
                },
            ]),
            expected_output: StreamingExpectedOutput {
                aggregate_by: "device_type".into(),
                value_field: "value".into(),
                expected_group_totals: vec![],
            },
        };

        let compiled = compile_pipeline(&workload, "file").unwrap();
        let rendered = render_pipeline(&compiled.pipeline);
        assert!(rendered.contains(".filter(event_type in [page_view,purchase])"));
        assert!(rendered.contains(".aggregate([\"count\", \"avg\"]"));
    }

    #[test]
    fn authoring_builder_writes_pipeline_into_workload() {
        let workload = WorkloadAuthoringBuilder::new("generated")
            .source("clickstream")
            .map("synthetic_clickstream_v1")
            .filter_with_op("value", "gte", "10")
            .key_by("device_type")
            .window_tumbling_secs(5)
            .aggregate(
                vec!["count".to_string(), "avg".to_string()],
                Some("event_count".to_string()),
                Some("value".to_string()),
                Some("value_sum".to_string()),
            )
            .sink_file()
            .build();

        assert!(workload.pipeline.is_some());
        assert_eq!(workload.pipeline.as_ref().unwrap().len(), 7);
        assert_eq!(workload.window.size_secs, 5);
        assert_eq!(workload.source.key_field, "device_type");
        assert_eq!(workload.filter.as_ref().unwrap().op.as_deref(), Some("gte"));
    }

    #[test]
    fn mini_flink_graph_renders_fluent_chain() {
        let rendered = MiniFlinkGraph::source("clickstream")
            .named("fluent_demo")
            .map("synthetic_clickstream_v1")
            .filter_eq("event_type", "page_view")
            .key_by("device_type")
            .window_tumbling_secs(3)
            .aggregate_count_sum("event_count", "value", "value_sum")
            .sink_file()
            .render()
            .unwrap();

        assert!(rendered.contains("source(clickstream)"));
        assert!(rendered.contains(".map(synthetic_clickstream_v1)"));
        assert!(rendered.contains(".filter(event_type eq page_view)"));
        assert!(rendered.contains(".key_by(device_type)"));
        assert!(rendered.contains(".window(tumbling_3s)"));
        assert!(rendered.contains(".aggregate([\"count\", \"sum\"]"));
        assert!(rendered.contains(".sink(file)"));
    }

    #[test]
    fn mini_flink_graph_writes_pipeline_yaml() {
        let yaml = MiniFlinkGraph::source("clickstream")
            .named("yaml_demo")
            .map("synthetic_clickstream_v1")
            .filter_in("event_type", vec!["page_view".into(), "purchase".into()])
            .key_by("device_type")
            .window_tumbling_secs(3)
            .aggregate(
                vec!["count".into(), "avg".into()],
                Some("event_count".into()),
                Some("value".into()),
                Some("value_sum".into()),
            )
            .sink_stdout()
            .expected_group_totals_from_tuples(vec![("mobile", 2, 5)])
            .to_yaml_string()
            .unwrap();

        assert!(yaml.contains("pipeline:"));
        assert!(yaml.contains("kind: source"));
        assert!(yaml.contains("kind: filter"));
        assert!(yaml.contains("op: in"));
        assert!(yaml.contains("- page_view"));
        assert!(yaml.contains("- avg"));
        assert!(yaml.contains("sink_mode: stdout"));
        assert!(yaml.contains("event_count: 2"));
    }

    #[test]
    fn compile_pipeline_rejects_branching_graph() {
        let workload = WorkloadAuthoringBuilder::new("generated")
            .source("clickstream")
            .map("synthetic_clickstream_v1")
            .key_by("device_type")
            .window_tumbling_secs(5)
            .aggregate(
                vec!["count".to_string()],
                Some("event_count".to_string()),
                None,
                None,
            )
            .sink_file()
            .build();

        let mut pipeline = PipelineBuilder::source("clickstream")
            .map("synthetic_clickstream_v1")
            .key_by("device_type")
            .window_tumbling_secs(5)
            .aggregate(
                vec!["count".to_string()],
                Some("event_count".to_string()),
                None,
                None,
            )
            .sink_file()
            .build();
        pipeline.edges.push(Edge {
            from: "map_1".to_string(),
            to: "aggregate_4".to_string(),
        });

        let err = super::validate_pipeline(&pipeline, &workload, "file").unwrap_err();
        assert!(err.contains("single outgoing edge") || err.contains("single incoming edge"));
    }

    #[test]
    fn compile_pipeline_rejects_invalid_operator_order() {
        let workload = StreamingWorkloadDefinition {
            name: "w".into(),
            family: "f".into(),
            description: "d".into(),
            source: StreamingSourceShape {
                family: "clickstream".into(),
                event_time_field: "event_time".into(),
                key_field: "device_type".into(),
            },
            parse: None,
            filter: None,
            schema: vec![],
            scenario: StreamingScenario {
                name: "agg".into(),
                operation: "tumbling_window_grouped_sum".into(),
                group_by: vec!["device_type".into()],
                aggregate_count_as: Some("event_count".into()),
                aggregate_sum_field: Some("value".into()),
                aggregate_sum_as: Some("value_sum".into()),
            },
            window: StreamingWindowSpec {
                window_type: "tumbling".into(),
                size_secs: 3,
                slide_secs: None,
            },
            pipeline: None,
            expected_output: StreamingExpectedOutput {
                aggregate_by: "device_type".into(),
                value_field: "value".into(),
                expected_group_totals: vec![],
            },
        };

        let pipeline = Pipeline {
            nodes: vec![
                super::OperatorNode {
                    id: "source_0".into(),
                    kind: OperatorKind::Source {
                        family: "clickstream".into(),
                    },
                },
                super::OperatorNode {
                    id: "aggregate_1".into(),
                    kind: OperatorKind::Aggregate {
                        functions: vec!["count".into()],
                        count_as: Some("event_count".into()),
                        sum_field: None,
                        sum_as: None,
                    },
                },
                super::OperatorNode {
                    id: "sink_2".into(),
                    kind: OperatorKind::Sink {
                        mode: "file".into(),
                    },
                },
            ],
            edges: vec![
                Edge {
                    from: "source_0".into(),
                    to: "aggregate_1".into(),
                },
                Edge {
                    from: "aggregate_1".into(),
                    to: "sink_2".into(),
                },
            ],
        };

        let err = super::validate_pipeline(&pipeline, &workload, "file").unwrap_err();
        assert!(err.contains("preceding key_by"));
    }
}
