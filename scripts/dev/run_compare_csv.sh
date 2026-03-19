#!/usr/bin/env bash
set -euo pipefail

export BENCH_FORMAT=csv
COMPOSE_FILE="docker-compose.bench.yml"

BASE_DATASET_CONFIG="configs/datasets/clickstream_small.toml"
TMP_DATASET_CONFIG=""

cleanup() {
  if [[ -n "${TMP_DATASET_CONFIG:-}" && -f "${TMP_DATASET_CONFIG:-}" ]]; then
    rm -f "$TMP_DATASET_CONFIG"
  fi
}

trap cleanup EXIT

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

DATASET_CONFIG="$BASE_DATASET_CONFIG"

if [[ $# -ge 1 ]]; then
  ROWS="$1"
  TMP_DATASET_CONFIG="configs/datasets/.clickstream_small_${ROWS}_tmp.toml"

  echo "Preparing dataset config for ${ROWS} rows..."
  awk -v rows="$ROWS" '
    BEGIN { replaced=0 }
    /^rows[[:space:]]*=/ {
      print "rows = " rows
      replaced=1
      next
    }
    { print }
    END {
      if (replaced == 0) exit 1
    }
  ' "$BASE_DATASET_CONFIG" > "$TMP_DATASET_CONFIG"

  DATASET_CONFIG="$TMP_DATASET_CONFIG"
else
  echo "Using dataset config as-is: ${BASE_DATASET_CONFIG}"
fi

echo "Building local debug binaries once..."
cargo build -p bmgen -p bmrun -p bmreport

echo
echo "Generating dataset materializations..."
./target/debug/bmgen generate --config "$DATASET_CONFIG"

echo
echo "Starting ClickHouse with constrained resources..."
docker compose -f "$COMPOSE_FILE" up -d clickhouse
wait_for_clickhouse

echo
echo "Running DataFusion (containerized, CSV)..."
df_run_dir=$(run_and_capture_container "bench-datafusion")
echo "DataFusion run dir: $df_run_dir"

echo
echo "Running DuckDB (containerized, CSV)..."
duck_run_dir=$(run_and_capture_container "bench-duckdb")
echo "DuckDB run dir: $duck_run_dir"

echo
echo "Running ClickHouse benchmark client (containerized, CSV)..."
ch_run_dir=$(run_and_capture_container "bench-clickhouse")
echo "ClickHouse run dir: $ch_run_dir"

echo
echo "Running Polars (containerized, CSV)..."
polars_run_dir=$(run_and_capture_container "bench-polars")
echo "Polars run dir: $polars_run_dir"

echo
echo "Comparing latest CSV runs..."
./target/debug/bmreport compare --inputs \
  "$df_run_dir/raw_observations.jsonl" \
  "$duck_run_dir/raw_observations.jsonl" \
  "$ch_run_dir/raw_observations.jsonl" \
  "$polars_run_dir/raw_observations.jsonl"

# echo
# echo "Cleaning up dev containers..."
# docker compose -f "$COMPOSE_FILE" down