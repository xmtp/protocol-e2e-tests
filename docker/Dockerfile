FROM rust:1-bookworm AS builder
WORKDIR /src

RUN apt-get update \
 && apt-get install -y --no-install-recommends pkg-config libssl-dev ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY . .

ARG FORCE_REBUILD=0
RUN echo "FORCE_REBUILD=$FORCE_REBUILD"

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build --release --package xdbg

FROM debian:trixie-slim

WORKDIR /work

RUN apt-get update \
 && apt-get install -y --no-install-recommends tini bash ca-certificates procps libssl3 \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/target/release/xdbg /usr/local/bin/xdbg
COPY xmtp_debug/newrunner.sh /usr/local/bin/newrunner.sh
RUN chmod +x /usr/local/bin/xdbg /usr/local/bin/newrunner.sh

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/newrunner.sh"]