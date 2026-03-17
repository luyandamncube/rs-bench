#!/usr/bin/env bash
set -euo pipefail

run_and_capture() {
  local config="$1"
  local output
  output=$(cargo run -q -p bmrun -- run --config "$config")
  echo "$output" >&2
  echo "$output" | awk -F'Results: ' '/Results: /{print $2}' | tail -n1
}

echo "Generating dataset materializations..."
cargo run -q -p bmgen -- generate --config configs/datasets/clickstream_small.toml

echo "Ensuring ClickHouse is up..."
docker compose up -d clickhouse >/dev/null

echo
echo "Running DataFusion CSV..."
df_csv_run_dir=$(run_and_capture "configs/runs/datafusion_smoke.toml")

echo "Running DataFusion Parquet..."
df_parquet_run_dir=$(run_and_capture "configs/runs/datafusion_parquet_smoke.toml")

echo "Running DuckDB CSV..."
duck_csv_run_dir=$(run_and_capture "configs/runs/duckdb_smoke.toml")

echo "Running DuckDB Parquet..."
duck_parquet_run_dir=$(run_and_capture "configs/runs/duckdb_parquet_smoke.toml")

echo "Running ClickHouse CSV..."
ch_csv_run_dir=$(run_and_capture "configs/runs/clickhouse_smoke.toml")

echo "Running ClickHouse Parquet..."
ch_parquet_run_dir=$(run_and_capture "configs/runs/clickhouse_parquet_smoke.toml")

echo
echo "Comparing latest runs..."
cargo run -p bmreport -- compare --inputs \
  "$df_csv_run_dir/raw_observations.jsonl" \
  "$df_parquet_run_dir/raw_observations.jsonl" \
  "$duck_csv_run_dir/raw_observations.jsonl" \
  "$duck_parquet_run_dir/raw_observations.jsonl" \
  "$ch_csv_run_dir/raw_observations.jsonl" \
  "$ch_parquet_run_dir/raw_observations.jsonl" 