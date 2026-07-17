# The build fetches immutable sibling path dependencies rather than trusting
# whatever happens to be present in a local parent directory.
FROM rust:1.97.0-bookworm@sha256:7d0723df719e7f213b69dc7c8c595985c3f4b060cfbee4f7bc0e347a86fe3b6a AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates
WORKDIR /workspace
ARG CLIENTS_REF=1446b254b4bfd57b2df75c3c451a663313f19eb9
ARG INTERFACES_REF=3072e824e4e10f4a392a5851ea155ab5693ff206
ARG MESSAGING_REF=cec4ea4f54162758858c6c284324c34a42f3f3d7
ARG TELEMETRY_REF=724844e62ba35f409917d72343e7804c199878a9
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

FROM docker.io/library/node:25-bookworm-slim@sha256:81db02c4b671288a03915da9534dbd54f96d0e7c24d80ccc54f5b36b2e684370 AS node-runtime

# Playwright supplies the pinned Chromium build and its OS libraries. Replace
# its bundled Node with Node 25 so browser children can use the stable network
# permission gate in addition to child-process and read-only filesystem grants.
FROM mcr.microsoft.com/playwright:v1.56.0-noble@sha256:35246d87a7c88ea9b771c65d33171b2611b02a8253b4b12ce6f94376c55f99f2
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
