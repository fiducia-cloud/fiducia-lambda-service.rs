# Build context is the fiducia.cloud root (path deps to sibling crates).
FROM rust:1.85-bookworm AS build
WORKDIR /workspace
COPY fiducia-interfaces/ fiducia-interfaces/
COPY fiducia-clients/ fiducia-clients/
COPY fiducia-lambda-service.rs/ fiducia-lambda-service.rs/
RUN cargo build --release --locked --manifest-path fiducia-lambda-service.rs/Cargo.toml

# psql (definition loader) + a container runner live in the runtime image in
# production; the base here keeps libc + certs. Swap for your runtime image.
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends postgresql-client ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /workspace/fiducia-lambda-service.rs/target/release/fiducia-lambda-service /app
EXPOSE 8083
ENTRYPOINT ["/app"]
