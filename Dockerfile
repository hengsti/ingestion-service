FROM rust:1-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY schema ./schema
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates wget && rm -rf /var/lib/apt/lists/* \
    && adduser --disabled-password --gecos '' appuser
WORKDIR /app
COPY --from=build /app/target/release/smarthome-ingest /app/smarthome-ingest
ENV RUST_LOG=info
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -q --spider http://localhost:8085/healthz || exit 1
USER appuser
CMD ["/app/smarthome-ingest"]
