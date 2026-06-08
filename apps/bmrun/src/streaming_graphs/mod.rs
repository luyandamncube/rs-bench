mod device_avg_demo;
mod my_new_graph;
mod terminal_demo;

use anyhow::{anyhow, Result};
use bm_engine_mini_flink::MiniFlinkGraph;

// Graph authoring contract:
// 1. Create a module in this directory.
// 2. Export one `SPEC` constant and one `build()` function.
// 3. Add the `SPEC` to `GRAPH_SPECS`.
#[derive(Debug, Clone, Copy)]
pub struct StreamingGraphSpec {
    pub name: &'static str,
    pub description: &'static str,
    pub default_output: &'static str,
    pub default_config: &'static str,
    pub build: fn() -> MiniFlinkGraph,
}

impl StreamingGraphSpec {
    pub fn build_graph(&self) -> MiniFlinkGraph {
        (self.build)()
    }

    pub fn render(&self) -> Result<String> {
        self.build_graph()
            .render()
            .map_err(|e| anyhow!("failed to render graph {}: {e}", self.name))
    }

    pub fn write_yaml(&self, output: &str) -> Result<()> {
        self.build_graph()
            .write_yaml(output)
            .map_err(|e| anyhow!("failed to write graph {}: {e}", self.name))
    }
}

const GRAPH_SPECS: &[StreamingGraphSpec] = &[
    my_new_graph::SPEC,
    terminal_demo::SPEC,
    device_avg_demo::SPEC,
];

pub fn all_graphs() -> &'static [StreamingGraphSpec] {
    GRAPH_SPECS
}

pub fn known_graphs() -> Vec<&'static str> {
    GRAPH_SPECS.iter().map(|graph| graph.name).collect()
}

pub fn resolve_graph_spec(name: &str) -> Result<&'static StreamingGraphSpec> {
    GRAPH_SPECS
        .iter()
        .find(|graph| graph.name == name)
        .ok_or_else(|| {
            anyhow!(
                "unsupported streaming graph: {}. known graphs: {}",
                name,
                known_graphs().join(", ")
            )
        })
}

pub fn render_graph(name: &str) -> Result<String> {
    resolve_graph_spec(name)?.render()
}

pub fn write_graph(name: &str, output: &str) -> Result<()> {
    resolve_graph_spec(name)?.write_yaml(output)
}

#[cfg(test)]
pub(crate) fn assert_graph_spec_smoke(spec: &StreamingGraphSpec) {
    let rendered = spec.render().unwrap();
    assert!(!rendered.trim().is_empty());
    assert!(rendered.contains("source("));
    assert!(rendered.contains(".sink("));

    let output = std::env::temp_dir().join(format!("{}_graph_smoke.yaml", spec.name));
    let _ = std::fs::remove_file(&output);
    spec.write_yaml(output.to_str().unwrap()).unwrap();
    let yaml = std::fs::read_to_string(output).unwrap();
    assert!(yaml.contains("pipeline:"));
    assert!(yaml.contains(&format!(
        "name: {}",
        spec.build_graph().build_workload().name
    )));
}

#[cfg(test)]
mod tests {
    use super::{all_graphs, assert_graph_spec_smoke};
    use std::collections::HashSet;

    #[test]
    fn all_graph_specs_have_unique_names_and_defaults() {
        let mut names = HashSet::new();
        let mut outputs = HashSet::new();
        for graph in all_graphs() {
            assert!(
                names.insert(graph.name),
                "duplicate graph name {}",
                graph.name
            );
            assert!(
                outputs.insert(graph.default_output),
                "duplicate default output {}",
                graph.default_output
            );
            assert!(!graph.description.is_empty());
            assert!(!graph.default_config.is_empty());
            assert_graph_spec_smoke(graph);
        }
    }
}
