#!/usr/bin/env bash
set -euo pipefail

export BENCH_FORMAT=parquet
COMPOSE_FILE="docker-compose.bench.release.yml"
COMPOSE_PROJECT="rs-bench-release"

wait_for_clickhouse() {
  echo "Waiting for ClickHouse to become healthy..."
  for _ in $(seq 1 60); do
    local cid
    cid=$(docker compose -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" ps -q clickhouse-release 2>/dev/null || true)

    if [[ -n "${cid:-}" ]]; then
      status=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}unknown{{end}}' "$cid" 2>/dev/null || true)
      if [[ "$status" == "healthy" ]]; then
        echo "ClickHouse is healthy."
        return 0
      fi
    fi

    sleep 2
  done

  echo "ClickHouse did not become healthy in time." >&2
  docker compose -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" logs clickhouse-release || true
  exit 1
}

run_and_capture_container() {
  local service="$1"
  local tmpfile
  local run_dir

  tmpfile=$(mktemp)

  docker compose -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" run --rm "$service" \
    2>&1 | tee /dev/stderr >"$tmpfile"

  run_dir=$(awk -F'Results: ' '/^Results: /{print $2}' "$tmpfile" | tail -n1 | tr -d '\r')

  rm -f "$tmpfile"

  if [[ -z "${run_dir:-}" ]]; then
    echo "Failed to capture run directory for service: $service" >&2
    exit 1
  fi

  printf '%s\n' "$run_dir"
}

echo "Starting ClickHouse..."
docker compose -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" up -d --remove-orphans clickhouse-release
wait_for_clickhouse

echo
echo "Running DataFusion (release, Parquet)..."
df_run_dir=$(run_and_capture_container "datafusion-release")
echo "DataFusion run dir: $df_run_dir"

echo
echo "Running DuckDB (release, Parquet)..."
duck_run_dir=$(run_and_capture_container "duckdb-release")
echo "DuckDB run dir: $duck_run_dir"

echo
echo "Running ClickHouse (release, Parquet)..."
ch_run_dir=$(run_and_capture_container "clickhouse-client-release")
echo "ClickHouse run dir: $ch_run_dir"

echo
echo "Running Polars (release, Parquet)..."
polars_run_dir=$(run_and_capture_container "polars-release")
echo "Polars run dir: $polars_run_dir"

echo
echo "Comparing latest Parquet runs..."
docker compose -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" run --rm \
  --entrypoint /usr/local/bin/bmreport \
  datafusion-release compare --inputs \
  "$df_run_dir/raw_observations.jsonl" \
  "$duck_run_dir/raw_observations.jsonl" \
  "$ch_run_dir/raw_observations.jsonl" \
  "$polars_run_dir/raw_observations.jsonl"

echo
echo "Cleaning up release containers..."
docker compose -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" down