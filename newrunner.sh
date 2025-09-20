#!/bin/bash

set -euo pipefail

: "${XDBG_LOOP_PAUSE:=300}" # default interval between restarts

function log {
    echo "[$(date '+%F %T')] $*"
}

# Determine backend from WORKSPACE
WORKSPACE="${WORKSPACE:-}"
case "${WORKSPACE}" in
    testnet) BACKEND="production" ;;
    testnet-dev) BACKEND="dev" ;;
    testnet-staging) BACKEND="staging" ;;
    ""|*) BACKEND="local" ;;
esac
log "WORKSPACE='${WORKSPACE:-<unset>}' -> backend='${BACKEND}'"

xdbg -d -b "${BACKEND}" generate --entity identity --amount 10 --concurrency 1
xdbg -d -b "${BACKEND}" generate --entity group --amount 1 --concurrency 1 --invite 1

while true; do 
  log "Identities..."
  xdbg -d -b "${BACKEND}" generate --entity identity --amount 1 --concurrency 1
  log "Groups..."
  xdbg -d -b "${BACKEND}" generate --entity group --amount 1 --concurrency 1 --invite 1
  log "Messages..."
  xdbg -d -b "${BACKEND}" generate --entity message --amount 1 --concurrency 1
  log "Sleeping $XDBG_LOOP_PAUSE seconds..."
  sleep $XDBG_LOOP_PAUSE
done