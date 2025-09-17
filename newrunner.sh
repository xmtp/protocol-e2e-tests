#!/bin/bash

set -euo pipefail

: "${XDBG_LOOP_PAUSE:=300}" # set the default interval between requests to 5min

MESSAGE_DB=xdbg-message-db
GROUP_DB=xdbg-group-db

MSG_LOG=xdbg-scheduled-messages.out
GRP_LOG=xdbg-scheduled-groups.out

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

function clear_db() {
    local db_root=$1
    mkdir -p "$db_root"
    log "Clearing DB at $db_root"
    XDBG_DB_ROOT="$db_root" xdbg -d -b "${BACKEND}" --clear || log "Clear failed"
    sleep 10
}

function generate_identities() {
    local db_root=$1
    mkdir -p "$db_root"
    log "Generating identities at $db_root"
    XDBG_DB_ROOT="$db_root" xdbg -d -b "${BACKEND}" generate --entity identity --amount 10 --concurrency 1 \
        || { log "Identity generation failed, clearing and retrying"; clear_db "$db_root"; XDBG_DB_ROOT="$db_root" xdbg -d -b "${BACKEND}" generate --entity identity --amount 10 --concurrency 1; }
}

function generate_groups_with_retry() {
    local db_root=$1
    local attempts=0
    local max_attempts=5

    while [ $attempts -lt $max_attempts ]; do
        log "Sleeping 60 seconds before attempting group generation..."
        sleep 60
        log "Attempting group generation (attempt $((attempts + 1)))"
        if XDBG_DB_ROOT="$db_root" xdbg -d -b "${BACKEND}" generate --entity group --invite 1 --amount 1 --concurrency 1; then
            return 0
        else
            log "Group generation failed. Clearing DB and retrying..."
            clear_db "$db_root"
            generate_identities "$db_root"
            sleep 5
            attempts=$((attempts + 1))
        fi
    done

    log "Group generation failed after $max_attempts attempts. Exiting."
    exit 1
}

function setup_data() {
    local db_root=$1
    local for_entity=$2

    log "Setting up data for $for_entity (db=$db_root)"
    generate_identities "$db_root"

    if [ "$for_entity" = "message" ]; then
        log "Sleeping 60 seconds before group generation for messages"
        sleep 60
        generate_groups_with_retry "$db_root"
    fi
}

function run_long_test() {
    local db_root=$1
    local entity=$2
    local log_file=$3

    mkdir -p "$db_root"
    log "Starting long-running $entity test with DB at $db_root"
    while true; do
        if ! XDBG_DB_ROOT="$db_root" xdbg -d -b "${BACKEND}" generate --entity "$entity" --amount 9999999 --concurrency 1 &> "$log_file"; then
            log "$entity test failed. Resetting DB and retrying..."
            clear_db "$db_root"
            setup_data "$db_root" "$entity"
        else
            log "$entity test completed (restarting loop)"
        fi
    done
}

# Initial setup
setup_data "$GROUP_DB" group
setup_data "$MESSAGE_DB" message

echo "Starting tests...."
run_long_test "$GROUP_DB" group "$GRP_LOG" &
run_long_test "$MESSAGE_DB" message "$MSG_LOG" &
wait
