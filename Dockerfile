# ============================================================================
# STAGE 1: Builder (compile Rust application)
# ============================================================================
FROM rust:latest AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        pkg-config \
        libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# Build the CLI binary in release mode
RUN cargo build --release --bin kafka-backup && \
    ls -lh target/release/kafka-backup


# ============================================================================
# STAGE 2: Runtime (minimal image for production)
# ============================================================================
FROM debian:bookworm-slim AS runtime

# Create non-root user for security
RUN useradd -m -u 1000 kafka-backup

# Install runtime dependencies only (TLS, ca-certificates)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Copy binary from builder
COPY --from=builder /app/target/release/kafka-backup /usr/local/bin/kafka-backup

# Ensure binary is executable
RUN chmod +x /usr/local/bin/kafka-backup

# Set non-root user
USER kafka-backup

# Default working directory
WORKDIR /workspace

# Entry point
ENTRYPOINT ["kafka-backup"]
CMD ["--help"]
