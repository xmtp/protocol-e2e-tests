#!/bin/bash

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


while true; do 
  log "Reset environment.."
  XDBG_LOOP_PAUSE=0 xdbg -d -b "${BACKEND}" --clear
  XDBG_LOOP_PAUSE=0 xdbg -d -b "${BACKEND}" generate --entity identity --amount 5 --concurrency 1
  XDBG_LOOP_PAUSE=0 xdbg -d -b "${BACKEND}" generate --entity group --amount 1 --concurrency 1 --invite 1
  log "Reset complete, starting tests"
  
  for x in {1..10}; {
    log "Identities..."
    XDBG_LOOP_PAUSE=0 xdbg -d -b "${BACKEND}" generate --entity identity --amount 1 --concurrency 1
    log "Sleeping 20s..."
    sleep 20
    log "Groups..."
    XDBG_LOOP_PAUSE=0 xdbg -d -b "${BACKEND}" generate --entity group --amount 1 --concurrency 1 --invite 1
    log "Sleeping 20s..."
    sleep 20
    log "Messages..."
    XDBG_LOOP_PAUSE=0 xdbg -d -b "${BACKEND}" generate --entity message --amount 1 --concurrency 1
    log "Sleeping $XDBG_LOOP_PAUSE seconds..."
    sleep $XDBG_LOOP_PAUSE
  }
done