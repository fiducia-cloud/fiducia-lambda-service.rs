# The build fetches immutable sibling path dependencies rather than trusting
# whatever happens to be present in a local parent directory.
FROM rust:1.97.0-bookworm AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates
WORKDIR /workspace
ARG CLIENTS_REF=051b332843fb005006be0d564e98ba46b825785c
ARG INTERFACES_REF=bbd8b52ce729ec34b0a9bff4dda6d0a448181797
RUN git init fiducia-clients \
    && git -C fiducia-clients remote add origin https://github.com/fiducia-cloud/fiducia-clients.git \
    && git -C fiducia-clients fetch --depth 1 origin "$CLIENTS_REF" \
    && git -C fiducia-clients checkout --detach FETCH_HEAD \
    && test "$(git -C fiducia-clients rev-parse HEAD)" = "$CLIENTS_REF"
RUN git init fiducia-interfaces \
    && git -C fiducia-interfaces remote add origin https://github.com/fiducia-cloud/fiducia-interfaces.git \
    && git -C fiducia-interfaces fetch --depth 1 origin "$INTERFACES_REF" \
    && git -C fiducia-interfaces checkout --detach FETCH_HEAD \
    && test "$(git -C fiducia-interfaces rev-parse HEAD)" = "$INTERFACES_REF"
COPY fiducia-lambda-service.rs/ fiducia-lambda-service.rs/
RUN cargo build --release --locked --manifest-path fiducia-lambda-service.rs/Cargo.toml

# The service intentionally needs psql and /bin/sh, so it cannot use the
# single-binary distroless profile. The explicit profile label is consumed by
# the monorepo audit and still requires numeric non-root execution.
FROM debian:bookworm-slim
LABEL org.fiducia.runtime-profile="tool-runner-nonroot"
RUN apt-get update && apt-get install -y --no-install-recommends postgresql-client ca-certificates \
    && apt-get clean
COPY --from=build --chown=65532:65532 /workspace/fiducia-lambda-service.rs/target/release/fiducia-lambda-service /app/fiducia-lambda-service
ENV HOME=/tmp
USER 65532:65532
EXPOSE 8083
ENTRYPOINT ["/app/fiducia-lambda-service"]
