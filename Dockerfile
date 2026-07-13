# Build context is the fiducia.cloud root (path deps to sibling crates).
FROM rust:1.85-bookworm AS build
WORKDIR /workspace
COPY fiducia-interfaces/ fiducia-interfaces/
COPY fiducia-clients/ fiducia-clients/
COPY fiducia-lambda-service.rs/ fiducia-lambda-service.rs/
RUN cargo build --release --locked --manifest-path fiducia-lambda-service.rs/Cargo.toml

# The service intentionally needs psql and /bin/sh, so it cannot use the
# single-binary distroless profile. The explicit profile label is consumed by
# the monorepo audit and still requires numeric non-root execution.
FROM debian:bookworm-slim
LABEL org.fiducia.runtime-profile="tool-runner-nonroot"
RUN apt-get update && apt-get install -y --no-install-recommends postgresql-client ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build --chown=65532:65532 /workspace/fiducia-lambda-service.rs/target/release/fiducia-lambda-service /app/fiducia-lambda-service
ENV HOME=/tmp
USER 65532:65532
EXPOSE 8083
ENTRYPOINT ["/app/fiducia-lambda-service"]
