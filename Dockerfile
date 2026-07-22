# The build fetches immutable sibling path dependencies rather than trusting
# whatever happens to be present in a local parent directory.
FROM rust:1.97.0-bookworm@sha256:8fa55b2f3ddf97471ab6a767bfa3f37e6bad0986ba823e75fea57e2a2a5c3073 AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates
WORKDIR /workspace
ARG CLIENTS_REF=5695b16a1577aadbfe414123927e45927f88a7f0
ARG INTERFACES_REF=6e20a3f4df2e52b99a0ad6add83d4528262b5dbc
ARG MESSAGING_REF=cec4ea4f54162758858c6c284324c34a42f3f3d7
ARG TELEMETRY_REF=20ed56d9e725c9189deb7386a2dee91ea8b25fdb
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

FROM docker.io/library/node:26-bookworm-slim@sha256:2d49d876e96237d76de412761cf05dbfe5aee325cc4406a4d41d5824c5bb8beb AS node-runtime

# Playwright supplies the pinned Chromium build and its OS libraries. Replace
# its bundled Node with Node 25 so browser children can use the stable network
# permission gate in addition to child-process and read-only filesystem grants.
FROM mcr.microsoft.com/playwright:v1.61.1-noble@sha256:5b8f294aff9041b7191c34a4bab3ac270157a28774d4b0660e9743297b697e48
LABEL org.fiducia.runtime-profile="tool-runner-nonroot"
COPY --from=node-runtime /usr/local/ /usr/local/
RUN apt-get update && apt-get install -y --no-install-recommends postgresql-client ca-certificates \
    && apt-get clean
WORKDIR /app
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright \
    PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1 \
    PUPPETEER_SKIP_DOWNLOAD=true \
    LAMBDA_ALLOW_HOST_RUNTIMES=nodejs,playwright,puppeteer
COPY fiducia-lambda-service.rs/package.json fiducia-lambda-service.rs/package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts
COPY --chown=65532:65532 fiducia-lambda-service.rs/child-runtimes/ ./child-runtimes/
COPY --from=build --chown=65532:65532 /workspace/fiducia-lambda-service.rs/target/release/fiducia-lambda-service /app/fiducia-lambda-service
ENV HOME=/tmp
USER 65532:65532
EXPOSE 8083
ENTRYPOINT ["/app/fiducia-lambda-service"]
