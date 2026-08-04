FROM rust:bookworm AS chef
RUN cargo install cargo-chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Builds and caches just the dependencies as their own layer, so this step is
# skipped by the Docker/buildx cache whenever only application source changes.
RUN cargo chef cook --release --recipe-path recipe.json --bin library_updater

COPY . .
RUN cargo build --release --bin library_updater


FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y openssl ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

RUN update-ca-certificates

RUN groupadd --system --gid 1000 library_updater \
    && useradd --system --uid 1000 --gid library_updater --home-dir /app --shell /usr/sbin/nologin library_updater

WORKDIR /app

# Dump/working directory used by the updater (see Config::data_dir, DATA_DIR
# env var, defaults to "data" relative to WORKDIR). Created and owned by the
# non-root runtime user. For persistence across container recreations, mount
# a volume at /app/data instead of relying on this baked-in directory.
RUN mkdir -p /app/data && chown -R library_updater:library_updater /app

COPY --from=builder /app/target/release/library_updater /usr/local/bin/library_updater

USER library_updater

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

ENTRYPOINT ["/usr/local/bin/library_updater"]
