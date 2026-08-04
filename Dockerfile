# The build fetches immutable sibling path dependencies rather than trusting
# whatever happens to be present in a local parent directory.
FROM rust:1.97.1-bookworm@sha256:77fac8b98f9f46062bb680b6d25d5bcaabfc400143952ebc572e924bcbedc3fa AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates
WORKDIR /workspace
ARG CLIENTS_REF=d17e67e1c531ff8e1bc427f21a8bcd0ff57cd214
ARG INTERFACES_REF=bd718cd72d72aa330534f3688f8fb1ce90c19d10
ARG MESSAGING_REF=d3e88b3692bfdb4d387a9aa70fb7af5ecf200c49
ARG TELEMETRY_REF=1128b3f9ab4fb003e918f61ab7743fd5c6c62fc9
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

FROM docker.io/library/node:26-bookworm-slim@sha256:9e6f9357d371591e32ab6f2d8a26d63bdd0d17c29eee3f4f3e7e454d9634bf73 AS node-runtime

# Playwright supplies the pinned Chromium build and its OS libraries. Replace
# its bundled Node with Node 25 so browser children can use the stable network
# permission gate in addition to child-process and read-only filesystem grants.
FROM mcr.microsoft.com/playwright:v1.62.1-noble@sha256:dcc5531e97840b9b5e794f2814476b21571c5124a3fca2267d73041f56e7580e
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
