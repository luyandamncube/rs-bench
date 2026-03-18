#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="docker-compose.bench.yml"

wait_for_clickhouse() {
  echo "Waiting for ClickHouse to become healthy..."
  for _ in $(seq 1 60); do
    status=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}unknown{{end}}' bm-clickhouse 2>/dev/null || true)
    if [[ "$status" == "healthy" ]]; then
      echo "ClickHouse is healthy."
      return 0
    fi
    sleep 2
  done

  echo "ClickHouse did not become healthy in time." >&2
  docker compose -f "$COMPOSE_FILE" logs clickhouse || true
  exit 1
}

run_and_capture_container() {
  local service="$1"
  local tmpfile
  local run_dir

  tmpfile=$(mktemp)

  # Send full container output to stderr for visibility,
  # but only capture the Results line into run_dir.
  docker compose -f "$COMPOSE_FILE" run --rm "$service" \
    2>&1 | tee /dev/stderr >"$tmpfile"

  run_dir=$(awk -F'Results: ' '/^Results: /{print $2}' "$tmpfile" | tail -n1 | tr -d '\r')

  rm -f "$tmpfile"

  if [[ -z "${run_dir:-}" ]]; then
    echo "Failed to capture run directory for service: $service" >&2
    exit 1
  fi

  printf '%s\n' "$run_dir"
}

echo "Generating dataset materializations..."
cargo run -q -p bmgen -- generate --config configs/datasets/clickstream_small.toml

echo
echo "Starting ClickHouse with constrained resources..."
docker compose -f "$COMPOSE_FILE" up -d clickhouse
wait_for_clickhouse

echo
echo "Running DataFusion (containerized)..."
df_run_dir=$(run_and_capture_container "bench-datafusion")
echo "DataFusion run dir: $df_run_dir"

echo
echo "Running DuckDB (containerized)..."
duck_run_dir=$(run_and_capture_container "bench-duckdb")
echo "DuckDB run dir: $duck_run_dir"

echo
echo "Running ClickHouse benchmark client (containerized)..."
ch_run_dir=$(run_and_capture_container "bench-clickhouse")
echo "ClickHouse run dir: $ch_run_dir"

echo
echo "Comparing latest runs..."
cargo run -p bmreport -- compare --inputs \
  "$df_run_dir/raw_observations.jsonl" \
  "$duck_run_dir/raw_observations.jsonl" \
  "$ch_run_dir/raw_observations.jsonl"