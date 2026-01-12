# ============================================================================
# STAGE 1: Builder (compile Rust application)
# ============================================================================
# IMPORTANT: Use rust:bookworm to match the runtime stage's Debian version.
# Using rust:latest can cause glibc version mismatches since it may use a newer
# Debian version (e.g., Trixie with glibc 2.41) while the runtime uses Bookworm
# (glibc 2.36). Binaries compiled against newer glibc won't run on older versions.
# See: https://github.com/osodevops/kafka-backup/issues/5
FROM rust:bookworm AS builder

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
# NOTE: This must use the same Debian version as the builder stage (bookworm).
# If you upgrade the runtime image, also update the builder stage to match.
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
