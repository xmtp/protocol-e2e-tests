FROM debian:bullseye-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends tini bash ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY target/release/xdbg /usr/local/bin/xdbg
COPY xmtp_debug/newrunner.sh /usr/local/bin/newrunner.sh
RUN chmod +x /usr/local/bin/xdbg /usr/local/bin/newrunner.sh

ENV RUST_LOG=info

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/newrunner.sh"]
