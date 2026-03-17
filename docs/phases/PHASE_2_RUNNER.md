# Phase 2 — Runner

## Goal

Build the first full benchmark execution loop.

This phase establishes the benchmark harness flow that:
1. loads benchmark config
2. loads the dataset manifest
3. loads the workload definition
4. resolves SQL files from disk
5. executes benchmark queries through an engine adapter
6. persists raw benchmark observations to disk

For this phase, execution uses a **mock adapter** rather than a real engine.

The purpose is to prove the orchestration, workload loading, and raw-result persistence before adding real engine-specific complexity.

---

## Why this phase exists

The benchmark runner is the backbone of the system.

Before integrating real engines such as DataFusion, DuckDB, or ClickHouse, the harness needs to prove that it can:

- interpret benchmark configs
- resolve datasets and workloads from disk
- execute ordered query suites
- repeat benchmark queries
- mark hot vs cold runs
- persist raw observations for later analysis
- emit enough metadata to make runs replayable and inspectable

This phase proves the harness design independently of engine-specific implementation risk.

---

## Scope

### Included
- `bmrun` CLI command for benchmark execution
- run config loading from TOML
- dataset manifest loading from JSON
- workload definition loading from YAML
- SQL file resolution from workload directory
- mock adapter lifecycle execution
- query repetition logic
- hot vs cold labeling
- raw observation persistence to JSONL and CSV
- run manifest persistence

### Excluded
- real DataFusion adapter
- real DuckDB adapter
- real ClickHouse adapter
- query correctness validation
- summary/ranking logic
- report generation
- query plan capture
- concurrency benchmarking

---

## Deliverables

Running:

```bash
cargo run -p bmrun -- run --config configs/runs/comparison_smoke.toml
```
should produce a run directory like:

```
results/runs/<run_id>/
├── run_manifest.json
├── raw_observations.jsonl
└── raw_observations.csv
```

---

## Run config shape

Initial run config:

```
run_name = "comparison_smoke"
engine = "mock"
workload_path = "workloads/clickstream/workload.yaml"
dataset_manifest_path = "datasets/generated/clickstream_small/csv/manifest.json"
repetitions = 3
capture_plans = false
```

---

## Workload packaging

Initial workload file:

```
name: clickstream_core
family: clickstream
description: Small clickstream smoke workload
queries:
  - id: q01
    name: session_filter
    category: scan_filter
    file: q01_session_filter.sql
```

Initial SQL file:

```
SELECT1;
```

---

## Execution flow

The runner flow for this phase is:

1. Load run config
2. Load dataset manifest
3. Load workload YAML
4. Resolve workload SQL files from disk
5. Create run ID and results directory
6. Call engine `bootstrap()`
7. Call engine `prepare_dataset()`
8. Execute each query in workload order
9. Repeat each query according to config
10. Mark first repetition as `cold`, later repetitions as `hot`
11. Persist raw observations
12. Call engine `cleanup()`
13. Persist run manifest

---

## Engine used in this phase

### Mock adapter

This phase uses a mock adapter implementing the common engine trait.

The mock adapter proves:

- the lifecycle contract works
- the runner can execute through the engine abstraction
- benchmark observations can be captured without relying on real query engines

The mock adapter returns synthetic elapsed times and success results.

This is intentional and keeps the runner phase focused on harness design.

---

## Output artifacts

### `run_manifest.json`

Captures:

- run ID
- run name
- engine
- dataset name/family
- workload name/family
- repetition count
- plan-capture flag
- generation timestamp

### `raw_observations.jsonl`

Stores one JSON record per query repetition.

### `raw_observations.csv`

Stores the same raw observations in CSV form for easier inspection.

---

## Raw observation shape

Each observation includes:

- `run_id`
- `run_name`
- `engine_name`
- `engine_version`
- `workload_name`
- `workload_family`
- `dataset_name`
- `dataset_family`
- `dataset_format`
- `query_id`
- `query_name`
- `query_category`
- `repetition`
- `warm_or_cold`
- `started_at`
- `elapsed_ms`
- `success`
- `error_message`

This preserves raw benchmark attempts rather than only storing derived averages.

---

## Main files

### Application

- `apps/bmrun/src/main.rs`

### Runner crate

- `crates/bm-runner/src/lib.rs`

### Shared schemas

- `crates/bm-schema/src/run.rs`
- `crates/bm-schema/src/workload.rs`
- `crates/bm-schema/src/dataset.rs`
- `crates/bm-schema/src/raw_result.rs`

### Engine contract

- `crates/bm-engine/src/adapter.rs`
- `crates/bm-engine/src/request.rs`
- `crates/bm-engine/src/response.rs`
- `crates/bm-engine/src/error.rs`

### Configs / workloads

- `configs/runs/comparison_smoke.toml`
- `workloads/clickstream/workload.yaml`
- `workloads/clickstream/q01_session_filter.sql`

### Output

- `results/runs/<run_id>/*`

---

## Acceptance criteria

Phase 2 is complete when all of the following are true:

- `bmrun run --config ...` executes successfully
- a unique run directory is created in `results/runs/`
- `run_manifest.json` is written
- `raw_observations.jsonl` is written
- `raw_observations.csv` is written
- observation count matches `queries × repetitions`
- first repetition is labeled `cold`
- later repetitions are labeled `hot`
- runner works through the engine adapter contract

---

## Verified results

This phase was verified with a smoke run using:

- engine: `mock`
- workload: `clickstream_core`
- repetitions: `3`

### Output files confirmed

- `run_manifest.json`
- `raw_observations.jsonl`
- `raw_observations.csv`

### Observation count confirmed

For:

- 1 workload query
- 3 repetitions

the output correctly contained:

- 3 raw observations

### Hot/cold labeling confirmed

- repetition 1 = `cold`
- repetition 2 = `hot`
- repetition 3 = `hot`

### Example elapsed times

The mock adapter produced:

- 51 ms
- 52 ms
- 53 ms

which confirmed correct repetition handling and persistence.

---

## Known limitations

This phase intentionally does not yet implement:

- real engine adapters
- semantic result validation
- summary statistics
- ranking output
- chart output
- query plan capture
- concurrency support
- failure retry policies

These belong in later phases.

---

## Risks / caveats

- The mock adapter does not prove real engine correctness or performance.
- This phase validates orchestration, not benchmark fairness.
- SQL dialect compatibility is not tested here because the current SQL is a smoke placeholder.