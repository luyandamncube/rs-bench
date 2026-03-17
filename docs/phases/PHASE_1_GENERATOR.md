# Phase 1 — Generator

## Goal

Build the first real dataset generation path for the benchmark harness.

This phase establishes a reproducible synthetic data pipeline that can generate benchmark-ready datasets to disk, together with enough metadata for later benchmark execution and replay.

The output of this phase is not “just a CSV file.”  
It is a **reusable benchmark dataset artifact** with:
- dataset data
- manifest metadata
- schema snapshot
- generation config snapshot
- seeded reproducibility

---

## Why this phase exists

The benchmark runner should not invent datasets on the fly.

To keep the benchmark harness inspectable, reproducible, and comparable across engines, dataset generation must happen as a separate step.

This phase proves that:
- datasets can be generated independently of engine execution
- generated data can be reused across multiple engines
- the same config + seed yields the same output
- benchmark inputs can be saved and replayed later

---

## Scope

This phase implements the first dataset family and the first output format.

### Included
- `bmgen` CLI command for dataset generation
- clickstream dataset family
- seeded synthetic row generation
- CSV output
- manifest generation
- generation config snapshot
- schema snapshot
- basic generator test coverage

### Excluded
- Parquet output
- multiple dataset families
- null-heavy/skew-heavy realism beyond basic scaffolding
- partitioned dataset writing
- benchmark execution
- reporting/ranking

---

## Deliverables

Running:

```bash
cargo run -p bmgen -- generate --config configs/datasets/clickstream_small.toml
```

should produce:

```
datasets/generated/clickstream_small/
├── clickstream_small.csv
├── manifest.json
├── generation_config.json
└── schema.json
```

---

## Dataset family implemented

### Clickstream

The first synthetic dataset family is a clickstream/events style dataset.

Initial row shape:

- `event_time`
- `user_id`
- `session_id`
- `page_id`
- `device_type`
- `country_code`
- `referrer_domain`
- `event_type`
- `revenue`
- `latency_ms`

This schema is intentionally small but useful enough for:

- scan/filter benchmarks
- aggregation benchmarks
- group-by benchmarks
- top-N benchmarks
- simple revenue-style analytical queries

---

## Config shape

Initial config example:

```
name = "clickstream_small"
family = "clickstream"
seed = 42
rows = 100000
format = "csv"
output_dir = "datasets/generated/clickstream_small"

[cardinality]
users = 10000
sessions = 50000
pages = 500
countries = 12
referrers = 8

[distributions]
null_ratio = 0.0
skew = "medium"
```

---

## Reproducibility requirements

This phase requires deterministic generation.

For the same:

- config
- seed
- generator version

the produced dataset file should be identical.

Validation used in this phase:

- repeated generation with the same seed yields the same file hash
- row count matches config
- output artifacts exist

---

## Main files

### Applications

- `apps/bmgen/src/main.rs`

### Generator crate

- `crates/bm-generator/src/lib.rs`
- `crates/bm-generator/src/config.rs`
- `crates/bm-generator/src/manifest.rs`
- `crates/bm-generator/src/writer_csv.rs`
- `crates/bm-generator/src/families/mod.rs`
- `crates/bm-generator/src/families/clickstream.rs`

### Configs

- `configs/datasets/clickstream_small.toml`

### Output

- `datasets/generated/clickstream_small/*`

### Tests

- `crates/bm-generator/tests/clickstream_generate.rs`

---

## CLI contract

### Command

```
cargo run-p bmgen-- generate--config <config-path>
```

### Expected behavior

1. Load dataset config from TOML
2. Validate config
3. Generate clickstream rows deterministically
4. Write CSV file
5. Write `manifest.json`
6. Write `generation_config.json`
7. Write `schema.json`
8. Print generation summary to terminal

---

## Acceptance criteria

Phase 1 is complete when all of the following are true:

- `bmgen generate --config ...` executes successfully
- CSV file is written to `datasets/generated/...`
- `manifest.json` is written
- `generation_config.json` is written
- `schema.json` is written
- generated row count matches configured row count
- repeated generation with the same seed produces the same file hash
- generator tests pass

---

## Verified results

This phase was verified with:

### Determinism

Two consecutive runs produced the same SHA256 hash for:

```
datasets/generated/clickstream_small/clickstream_small.csv
```

### Row count

`wc -l` returned:

```
100001
```

which correctly represents:

- 100000 data rows
- 1 header row

### Artifact existence

Confirmed output files:

- `clickstream_small.csv`
- `manifest.json`
- `generation_config.json`
- `schema.json`

---

## Known limitations

This phase intentionally does not yet implement:

- Parquet output
- logs dataset family
- IoT/timeseries dataset family
- explicit null injection
- explicit skew modeling
- nested/semi-structured columns
- partitioned writes

These will be added in later phases.

---

## Risks / caveats

- CSV is useful for early validation, but it is not the long-term benchmark baseline format.
- This phase proves the generator path, not storage-format fairness.
- Current synthetic realism is intentionally light; later phases should deepen distributions and workload realism.