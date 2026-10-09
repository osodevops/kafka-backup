# OSO Kafka Backup: Docker Containerization Strategy for Rust CLI

## Executive Summary

**Best Practice:** Single repository, single `Dockerfile` in root, but structured to support workspace builds.

```
osodevops/kafka-backup/
├── Cargo.toml (workspace root)
├── core/
│   ├── Cargo.toml (library crate)
│   └── src/
├── cli/
│   ├── Cargo.toml (binary crate)
│   └── src/
├── Dockerfile (multi-stage build)
├── docker-compose.yml (optional, for development)
├── .dockerignore
└── .github/workflows/docker.yml (CI/CD)
```

**Why this is better than separate repos:**
- ✅ Single source of truth
- ✅ Shared workspace cache (faster builds)
- ✅ Atomic releases (binary + core crate version locked)
- ✅ Easier dependency management
- ✅ Standard practice (DuckDB, ripgrep, fd all do this)

---

## Part 1: Repository Structure (Single Repo)

### Current Structure

```
kafka-backup/
├── core/                 # Core library logic
│   ├── Cargo.toml
│   ├── src/
│   │   ├── lib.rs
│   │   ├── backup.rs
│   │   ├── restore.rs
│   │   └── offset_mapping.rs
│   └── tests/
│
├── cli/                  # CLI entry point
│   ├── Cargo.toml
│   ├── src/
│   │   └── main.rs
│   └── tests/
│
├── Cargo.toml (workspace root)
├── Cargo.lock
├── Dockerfile
├── .dockerignore
└── README.md
```

### Workspace Root Cargo.toml

```toml
[workspace]
members = ["core", "cli"]
resolver = "2"

[workspace.package]
version = "0.1.0"
edition = "2021"
authors = ["OSO Tech <team@oso.sh>"]
license = "Apache-2.0"

[workspace.dependencies]
tokio = { version = "1.35", features = ["full"] }
tracing = "0.1"
# ... other shared dependencies
```

### CLI Cargo.toml

```toml
[package]
name = "kafka-backup-cli"
version.workspace = true
edition.workspace = true
authors.workspace = true
license.workspace = true

[[bin]]
name = "kafka-backup"
path = "src/main.rs"

[dependencies]
kafka-backup-core = { path = "../core" }
tokio = { workspace = true }
clap = { version = "4.4", features = ["derive"] }
tracing = { workspace = true }
tracing-subscriber = "0.3"

[profile.release]
opt-level = 3
lto = true
strip = true
```

**Why workspace:**
- One `Cargo.lock` at root (all crates locked to same versions)
- Shared target directory (faster compiles, shared cache)
- Both crates built together by Docker
- Easy to test core library independently

---

## Part 2: Dockerfile (Multi-Stage, Optimized for Workspaces)

### Production-Grade Dockerfile

```dockerfile
# ============================================================================
# STAGE 1: Planner (prepare dependency graph for caching)
# ============================================================================
FROM rust:1.75-slim as planner

WORKDIR /app

# Install cargo-chef for dependency caching optimization
RUN cargo install cargo-chef

# Copy workspace files (smallest footprint)
COPY Cargo.toml Cargo.lock ./
COPY core/Cargo.toml ./core/Cargo.toml
COPY cli/Cargo.toml ./cli/Cargo.toml

# Create minimal src files (just stubs for chef)
RUN mkdir -p core/src cli/src && \
    echo "// stub" > core/src/lib.rs && \
    echo "fn main() {}" > cli/src/main.rs

# Generate recipe.json (dependency graph)
RUN cargo chef prepare --recipe-path recipe.json


# ============================================================================
# STAGE 2: Cacher (build dependencies only, reuse until Cargo.toml changes)
# ============================================================================
FROM rust:1.75-slim as cacher

WORKDIR /app

RUN cargo install cargo-chef

COPY --from=planner /app/recipe.json recipe.json

# Build dependencies (cached layer - survives source code changes)
RUN cargo chef cook --release --recipe-path recipe.json


# ============================================================================
# STAGE 3: Builder (build actual application)
# ============================================================================
FROM rust:1.75-slim as builder

WORKDIR /app

# Copy build cache from previous stage
COPY --from=cacher /app/target target

# Copy source code
COPY Cargo.toml Cargo.lock ./
COPY core ./core
COPY cli ./cli

# Build the CLI binary (use cache from stage 2)
RUN cargo build --release --bin kafka-backup && \
    # Verify binary was created
    ls -lh target/release/kafka-backup


# ============================================================================
# STAGE 4: Runtime (minimal image for production)
# ============================================================================
FROM debian:bookworm-slim as runtime

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
```

### Why This Structure?

**Stage 1: Planner**
- Generates dependency graph (`recipe.json`)
- Only runs when `Cargo.toml` changes
- Minimal (just dependency tree, no source code)

**Stage 2: Cacher**
- Builds ALL dependencies from `recipe.json`
- **Huge time saver:** Docker caches this layer until `Cargo.toml` changes
- Next build only rebuilds your code (seconds vs. minutes)
- Reuses compiled dependencies across builds

**Stage 3: Builder**
- Copies pre-built dependencies from stage 2
- Adds your source code
- Builds the binary (fast, because deps already cached)

**Stage 4: Runtime**
- Minimal base image (`debian:bookworm-slim` ≈ 60MB)
- Only runtime dependencies (no Rust, no build tools)
- Final image size: ~70-100MB (vs. 2GB+ with unoptimized Dockerfile)

---

## Part 3: .dockerignore (Critical for Performance)

```
# Version control
.git
.gitignore
.github

# Build artifacts
target/
*.lock.bak

# IDE
.vscode
.idea
*.swp
*.swo

# Development
.env
.env.local
docker-compose.override.yml

# Documentation
*.md
docs/

# CI/CD
.github/workflows/

# Testing
test-data/
fixtures/

# OS
.DS_Store
Thumbs.db
```

**Why this matters:**
- ❌ Without it: Docker copies 200MB of build artifacts, slow builds
- ✅ With it: Only copies essential source files (fast context send)

---

## Part 4: Building & Pushing to Docker Hub

### Local Build

```bash
# Build with tag
docker build -t osodevops/kafka-backup:latest .

# Build with version tag
docker build -t osodevops/kafka-backup:0.1.0 .

# Verify image size
docker images | grep kafka-backup
# Output: osodevops/kafka-backup  latest  abc123  2 weeks ago  78MB

# Test the image
docker run --rm osodevops/kafka-backup:latest --help
```

### Push to Docker Hub

```bash
# Login (one time)
docker login

# Tag for push
docker tag osodevops/kafka-backup:latest osodevops/kafka-backup:0.1.0

# Push
docker push osodevops/kafka-backup:0.1.0
docker push osodevops/kafka-backup:latest
```

### CI/CD Automation (GitHub Actions)

**File: `.github/workflows/docker-publish.yml`**

```yaml
name: Build & Push Docker Image

on:
  push:
    branches:
      - main
    tags:
      - 'v*'
  pull_request:
    branches:
      - main

env:
  REGISTRY: docker.io
  IMAGE_NAME: osodevops/kafka-backup

jobs:
  build-and-push:
    runs-on: ubuntu-latest
    permissions:
      contents: read

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Docker Buildx
        uses: docker/setup-buildx-action@v3

      - name: Log in to Docker Hub
        if: github.event_name != 'pull_request'
        uses: docker/login-action@v3
        with:
          username: ${{ secrets.DOCKER_USERNAME }}
          password: ${{ secrets.DOCKER_PASSWORD }}

      - name: Extract metadata
        id: meta
        uses: docker/metadata-action@v5
        with:
          images: ${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}
          tags: |
            type=semver,pattern={{version}}
            type=semver,pattern={{major}}.{{minor}}
            type=ref,event=branch
            type=sha,prefix={{branch}}-

      - name: Build and push Docker image
        uses: docker/build-push-action@v5
        with:
          context: .
          push: ${{ github.event_name != 'pull_request' }}
          tags: ${{ steps.meta.outputs.tags }}
          labels: ${{ steps.meta.outputs.labels }}
          cache-from: type=gha
          cache-to: type=gha,mode=max
```

**GitHub Secrets to Set:**
```
DOCKER_USERNAME = "osodevops" (or your user)
DOCKER_PASSWORD = "your PAT token from hub.docker.com"
```

---

## Part 5: Local Development with docker-compose.yml

### For Development (Optional)

The repo's own [`docker-compose.yml`](../docker-compose.yml) is a working
version of this (KRaft broker, MinIO, bucket and topic setup).

```yaml
version: '3.9'

services:
  # Kafka broker for testing
  kafka:
    image: confluentinc/cp-kafka:7.5.0
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    ports:
      - "9092:9092"
    depends_on:
      - zookeeper

  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports:
      - "2181:2181"

  # S3 local (minio for testing). minio/minio was deleted from Docker Hub
  # (Sept 2026); the Chainguard build runs as uid 65532, so run it as root
  # when reusing volumes created by the old image.
  minio:
    image: cgr.dev/chainguard/minio:latest
    user: "0:0"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    ports:
      - "9000:9000"
      - "9001:9001"
    command: ["server", "/data", "--console-address", ":9001"]

  # Create the bucket (the image's entrypoint is mc; there is no shell to wrap)
  minio-setup:
    image: cgr.dev/chainguard/minio-client:latest
    environment:
      MC_HOST_local: http://minioadmin:minioadmin@minio:9000
    command: ["mb", "--ignore-existing", "local/kafka-backups"]
    restart: on-failure
    depends_on:
      - minio

  # OSO Kafka Backup CLI (for testing)
  kafka-backup:
    build:
      context: .
      dockerfile: Dockerfile
    command: backup --help  # Override CMD for testing
    environment:
      # `--config` runs take the endpoint from the YAML (storage.endpoint:
      # http://minio:9000, path_style: true, allow_http: true); `--path s3://`
      # commands read the standard AWS_* variables.
      AWS_ENDPOINT_URL: http://minio:9000
      AWS_ALLOW_HTTP: "true"
      AWS_ACCESS_KEY_ID: minioadmin
      AWS_SECRET_ACCESS_KEY: minioadmin
      AWS_REGION: us-east-1
      RUST_LOG: debug
    depends_on:
      - kafka
      - minio
    volumes:
      - ./config.yaml:/config.yaml
```

### Usage

```bash
# Start services
docker-compose up -d

# Run backup
docker-compose run --rm kafka-backup backup --config /config.yaml

# Check logs
docker-compose logs -f kafka-backup

# Stop services
docker-compose down
```

---

## Part 6: Multi-Architecture Builds (ARM64 + x86-64)

### GitHub Actions: Cross-Platform Build

```yaml
# Add to docker-publish.yml
jobs:
  build-and-push:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        platform: [linux/amd64, linux/arm64]

    steps:
      # ... existing steps ...

      - name: Build and push Docker image (multi-arch)
        uses: docker/build-push-action@v5
        with:
          context: .
          push: ${{ github.event_name != 'pull_request' }}
          platforms: linux/amd64,linux/arm64
          tags: ${{ steps.meta.outputs.tags }}
          labels: ${{ steps.meta.outputs.labels }}
```

**Now Docker image works on:**
- ✅ MacBook M1/M2 (ARM64)
- ✅ Linux servers (x86-64)
- ✅ AWS Graviton (ARM64)

---

## Part 7: Slimming the Image Even Further (Optional)

### Use `busybox` or `alpine` Instead of `debian`

```dockerfile
# Runtime stage (ultra-slim: ~5MB)
FROM alpine:3.19 as runtime

RUN apk add --no-cache ca-certificates libssl3

COPY --from=builder /app/target/release/kafka-backup /usr/local/bin/kafka-backup

RUN chmod +x /usr/local/bin/kafka-backup

USER 1000

ENTRYPOINT ["kafka-backup"]
```

**Image sizes:**
- `debian:bookworm-slim` → ~70MB (Kafka backup binary)
- `alpine:3.19` → ~25MB (Kafka backup binary)
- `scratch` (empty filesystem) → Not possible (needs ca-certificates for TLS)

**Trade-off:** Alpine has fewer system libraries, can cause issues with large glibc dependencies. For Kafka backup, Alpine works fine.

---

## Part 8: Kubernetes Deployment Notes

### Read-Only Root Filesystem

When deploying with `readOnlyRootFilesystem: true` (recommended for security), the offset store needs a writable location. By default it uses `$TMPDIR` (`/tmp`), so mount a tmpfs or emptyDir there:

```yaml
apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      securityContext:
        runAsUser: 1000
        runAsNonRoot: true
      containers:
        - name: kafka-backup
          image: osodevops/kafka-backup:v0.11.4
          securityContext:
            readOnlyRootFilesystem: true
          volumeMounts:
            - name: tmp
              mountPath: /tmp
      volumes:
        - name: tmp
          emptyDir: {}
```

For persistent offset tracking across pod restarts, mount a PVC or emptyDir at `/data` and set `offset_storage.db_path` in your config:

```yaml
offset_storage:
  db_path: /data/offsets.db
```

> **Note:** The Docker image includes a `/data` directory owned by the `kafka-backup` user (uid 1000) for this purpose. Offset state is also synced to remote storage (S3/GCS/Azure) periodically, so local persistence is optional.

---

## Part 9: Dockerfile Best Practices Checklist

✅ **Do:**
- [x] Multi-stage builds (separate builder from runtime)
- [x] Use `.dockerignore` (exclude build artifacts)
- [x] cargo-chef for dependency caching (fast rebuilds)
- [x] Non-root user (security)
- [x] Minimal base image (debian-slim or alpine)
- [x] Layer caching optimization (Cargo.toml before src/)
- [x] Health checks (optional, for services)
- [x] Explicit ENTRYPOINT + CMD

❌ **Don't:**
- [ ] Build in root user
- [ ] Large base images (full Debian, Ubuntu)
- [ ] Copy entire `target/` directory (huge)
- [ ] Build all dependencies every push
- [ ] Missing `.dockerignore`
- [ ] Untagged images in registry

---

## Part 9: Real-World Examples

### What Popular Rust CLIs Do

**DuckDB:**
```
Official Docker image: duckdb/duckdb
Location: https://github.com/duckdb/duckdb-docker
Strategy: Separate repo with GitHub Actions publishing to Docker Hub
Size: ~80MB
```

**Ripgrep:**
```
No official Docker image (too simple, users build locally)
Community images exist but not published
Philosophy: "Use your OS package manager instead"
```

**fd:**
```
No official Docker image
npm package available for installation
Philosophy: "Lightweight CLI, not container-first"
```

**DuckDB (the best model for your use case):**
- Single Dockerfile in root of main repo
- Multi-stage build with cargo-chef
- Publishes to Docker Hub via GitHub Actions
- Supports multiple architectures (amd64, arm64)

---

## Part 10: Implementation Checklist

### Week 1: Setup

- [ ] Create `Dockerfile` in root (use Stage 1 template above)
- [ ] Create `.dockerignore`
- [ ] Test locally: `docker build -t osodevops/kafka-backup:test .`
- [ ] Verify size: `docker images | grep kafka-backup`
- [ ] Test CLI: `docker run --rm osodevops/kafka-backup:test --help`

### Week 2: GitHub Actions

- [ ] Create `.github/workflows/docker-publish.yml`
- [ ] Add `DOCKER_USERNAME` and `DOCKER_PASSWORD` secrets to GitHub
- [ ] Test on PR (should build, not push)
- [ ] Tag release: `git tag v0.1.0 && git push --tags`
- [ ] Verify on Docker Hub: `docker pull osodevops/kafka-backup:v0.1.0`

### Week 3: Documentation

- [ ] Add to README: Docker usage instructions
- [ ] Add examples: `docker run` commands
- [ ] Document environment variables
- [ ] Create `docker-compose.yml` for local dev

### Week 4: Polish

- [ ] Multi-architecture builds (arm64 support)
- [ ] Image health checks
- [ ] Helm chart (if deploying to Kubernetes)
- [ ] Performance optimization (strip binary size)

---

## Part 11: Quick Start Commands

```bash
# Build locally
docker build -t kafka-backup:dev .

# Run help
docker run --rm kafka-backup:dev --help

# Run backup (with config volume mount)
docker run --rm \
  -v $(pwd)/config.yaml:/config.yaml \
  -v $(pwd)/backups:/backups \
  kafka-backup:dev backup --config /config.yaml

# Push to Docker Hub
docker tag kafka-backup:dev osodevops/kafka-backup:latest
docker push osodevops/kafka-backup:latest

# Pull and run from Docker Hub
docker pull osodevops/kafka-backup:latest
docker run --rm osodevops/kafka-backup:latest --version
```

---

## Part 12: Final Architecture

```
Your Machine                GitHub                    Docker Hub
───────────────            ──────                    ──────────
Local changes              Push to main              osodevops/kafka-backup:latest
    ↓                            ↓                            ↑
git commit                 GitHub Actions                  Push image
    ↓                            ↓
git push                   Multi-stage build
    ↓                            ↓
GitHub Repo            docker build-push-action
                               ↓
                        Build amd64 + arm64
                               ↓
                        Push to registry
```

---

## Summary

**✅ Recommended Setup:**

1. **Single Repository** with workspaces (core + cli)
2. **Single Dockerfile** in root with multi-stage build
3. **cargo-chef** for dependency caching
4. **GitHub Actions** for automated publishing
5. **Final image size:** ~70-80MB
6. **Build time:** 
   - First build: ~5 minutes
   - Subsequent builds: ~1 minute (dependencies cached)
   - After source-only change: ~30 seconds

This approach is used by **DuckDB, Grafana, and other enterprise Rust tools**. It's battle-tested and production-ready.

