FROM rust:1.89-bookworm AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    clang \
    cmake \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY apps ./apps
COPY crates ./crates
COPY engines ./engines

RUN cargo build --release -p bmrun -p bmgen -p bmreport

FROM debian:bookworm-slim

WORKDIR /workspace

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/bmrun /usr/local/bin/bmrun
COPY --from=builder /app/target/release/bmgen /usr/local/bin/bmgen
COPY --from=builder /app/target/release/bmreport /usr/local/bin/bmreport