#!/bin/bash

while true; do 
    attempt=0
    max_attempts=5
    until cargo xdbg -d -b dev generate --entity group --amount 1 --concurrency 1; do
        attempt=$((attempt + 1))
        echo "Group creation failed (attempt $attempt/$max_attempts). Retrying..."
        if [ $attempt -ge $max_attempts ]; then
            echo "Group creation failed after $max_attempts attempts. Skipping to next iteration."
            break
        fi
    done

    cargo xdbg -d -b dev generate --entity message --amount 1 --concurrency 1
    sleep 60
done
