# Phase 0 — Foundation

## Goal

Create a clean Rust workspace and benchmark skeleton that compiles, tests, and supports future engine adapters.

## Scope

- workspace scaffold
- CLI app entrypoints
- shared schemas
- engine adapter trait
- fake adapter test
- sample configs
- local ClickHouse docker service

## Exit criteria

- `cargo check` passes
- `cargo test` passes
- `docker compose up -d clickhouse` works
- sample configs exist
- engine adapter contract exists
- fake adapter lifecycle test passes

## Non-goals

- real dataset generation
- real benchmark execution
- real engine query execution
- ranking logic