FROM rust:1.89-bookworm

WORKDIR /workspace

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    clang \
    cmake \
    build-essential \
    wget \
    && rm -rf /var/lib/apt/lists/*

CMD ["bash"]