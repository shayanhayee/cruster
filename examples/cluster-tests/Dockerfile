# syntax=docker/dockerfile:1.4

# Build stage - skipped if PREBUILT_BINARY is provided
FROM rust:1.93-bookworm AS builder

# Install protobuf compiler (required by etcd-client)
RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY examples ./examples

# Build the cluster-tests binary with cache mounts for Cargo
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release --package cluster-tests --all-features && \
    cp /app/target/release/cluster-tests /app/cluster-tests-bin

# Runtime stage
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binary - either from builder or prebuilt
COPY --from=builder /app/cluster-tests-bin /app/cluster-tests

# Copy test scripts
COPY examples/cluster-tests/tests /app/tests

# Make test scripts executable
RUN chmod +x /app/tests/*.sh /app/tests/lib/*.sh

ENV RUST_LOG=info

EXPOSE 8080 9000

ENTRYPOINT ["/app/cluster-tests"]

# CI stage - uses prebuilt binary from context
FROM debian:bookworm-slim AS ci

RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy prebuilt binary from build context
COPY cluster-tests /app/cluster-tests

# Copy test scripts
COPY tests /app/tests

# Make test scripts executable
RUN chmod +x /app/tests/*.sh /app/tests/lib/*.sh

ENV RUST_LOG=info

EXPOSE 8080 9000

ENTRYPOINT ["/app/cluster-tests"]
