# The build fetches immutable sibling path dependencies rather than trusting
# whatever happens to be present in a local parent directory.
FROM rust:1.97.0-bookworm@sha256:7d0723df719e7f213b69dc7c8c595985c3f4b060cfbee4f7bc0e347a86fe3b6a AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates
WORKDIR /workspace
ARG CLIENTS_REF=bcf2f868697a96d82151c0e4bf0efae258b234e9
ARG INTERFACES_REF=487e470c45ab5851e8f6f3b1dc048fe067fbf408
ARG MESSAGING_REF=416df78b2ca6132990150572933f3908728b2aab
ARG TELEMETRY_REF=b5663ee10367b5dfeac74d44922615226c75b7b2
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
RUN git init fiducia-messaging.rs \
    && git -C fiducia-messaging.rs remote add origin https://github.com/fiducia-cloud/fiducia-messaging.rs.git \
    && git -C fiducia-messaging.rs fetch --depth 1 origin "$MESSAGING_REF" \
    && git -C fiducia-messaging.rs checkout --detach FETCH_HEAD \
    && test "$(git -C fiducia-messaging.rs rev-parse HEAD)" = "$MESSAGING_REF"
RUN git init fiducia-telemetry.rs \
    && git -C fiducia-telemetry.rs remote add origin https://github.com/fiducia-cloud/fiducia-telemetry.rs.git \
    && git -C fiducia-telemetry.rs fetch --depth 1 origin "$TELEMETRY_REF" \
    && git -C fiducia-telemetry.rs checkout --detach FETCH_HEAD \
    && test "$(git -C fiducia-telemetry.rs rev-parse HEAD)" = "$TELEMETRY_REF"
COPY fiducia-lambda-service.rs/ fiducia-lambda-service.rs/
RUN cargo build --release --locked --manifest-path fiducia-lambda-service.rs/Cargo.toml

# The service intentionally needs psql and /bin/sh, so it cannot use the
# single-binary distroless profile. The explicit profile label is consumed by
# the monorepo audit and still requires numeric non-root execution.
FROM debian:bookworm-slim@sha256:60eac759739651111db372c07be67863818726f754804b8707c90979bda511df
LABEL org.fiducia.runtime-profile="tool-runner-nonroot"
RUN apt-get update && apt-get install -y --no-install-recommends postgresql-client ca-certificates \
    && apt-get clean
COPY --from=build --chown=65532:65532 /workspace/fiducia-lambda-service.rs/target/release/fiducia-lambda-service /app/fiducia-lambda-service
ENV HOME=/tmp
USER 65532:65532
EXPOSE 8083
ENTRYPOINT ["/app/fiducia-lambda-service"]
