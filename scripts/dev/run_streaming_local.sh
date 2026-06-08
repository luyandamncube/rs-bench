#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH="${1:-configs/streaming/local_stream.toml}"
BM_RUN="./target/debug/bmrun"
BM_REPORT="./target/debug/bmreport"

run_and_capture() {
  local config="$1"
  local tmpfile
  local run_dir

  tmpfile=$(mktemp)

  "$BM_RUN" run-streaming --config "$config" \
    2>&1 | tee /dev/stderr >"$tmpfile"

  run_dir=$(awk -F'Results: ' '/^Results: /{print $2}' "$tmpfile" | tail -n1 | tr -d '\r')

  rm -f "$tmpfile"

  if [[ -z "${run_dir:-}" ]]; then
    echo "Failed to capture streaming run directory." >&2
    exit 1
  fi

  printf '%s\n' "$run_dir"
}

echo "Building local debug binaries once..."
cargo build -p bmrun -p bmreport

echo
echo "Running streaming benchmark with config: $CONFIG_PATH"
run_dir=$(run_and_capture "$CONFIG_PATH")
echo "Streaming run dir: $run_dir"

echo
echo "Summarizing streaming run..."
"$BM_REPORT" summarize-streaming \
  --input "$run_dir/streaming_raw_observations.jsonl"

echo
echo "Streaming benchmark complete."
echo "Run directory: $run_dir"
echo "Summary path: $run_dir/streaming_summary.json"
