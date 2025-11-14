#!/bin/bash


: "${PUSHGATEWAY_URL:=http://localhost:9091}"
: "${HEALTHCHECK_TIMEOUT:=10}"

function log {
    echo "[$(date '+%F %T')] [healthcheck] $*"
}

if [ -z "${WEB_HEALTHCHECK_ENDPOINTS}" ]; then
    log "WEB_HEALTHCHECK_ENDPOINTS not set, skipping health checks"
    exit 0
fi

IFS=',' read -ra ENDPOINTS <<< "${WEB_HEALTHCHECK_ENDPOINTS}"

if [ ${#ENDPOINTS[@]} -eq 0 ]; then
    log "No endpoints configured"
    exit 0
fi

log "Checking ${#ENDPOINTS[@]} endpoint(s)..."

for endpoint in "${ENDPOINTS[@]}"; do
    endpoint=$(echo "$endpoint" | xargs)
    
    endpoint_id=$(echo "$endpoint" | sed 's|https\?://||' | sed 's|[^a-zA-Z0-9]|_|g')
    
    log "Checking endpoint: $endpoint (id: $endpoint_id)"
    
    http_code=$(curl -o /dev/null -s -w "%{http_code}" -m "$HEALTHCHECK_TIMEOUT" "$endpoint")
    curl_exit_code=$?
    
    if [ $curl_exit_code -eq 0 ] && [ "$http_code" = "200" ]; then
        health_status=1
        log "[OK] $endpoint - OK (200)"
    else
        health_status=0
        if [ $curl_exit_code -ne 0 ]; then
            log "[FAIL] $endpoint - FAILED (curl error: $curl_exit_code)"
        else
            log "[FAIL] $endpoint - FAILED (HTTP $http_code)"
        fi
    fi
    
    metric_name="web_endpoint_health"
    timestamp=$(date +%s)000
    
    # Construct payload with explicit newlines
    metrics_payload="# HELP ${metric_name} Health status of web endpoints (1 = healthy, 0 = unhealthy)
# TYPE ${metric_name} gauge
${metric_name}{endpoint=\"${endpoint}\",endpoint_id=\"${endpoint_id}\",http_code=\"${http_code}\"} ${health_status} ${timestamp}
"
    
    push_url="${PUSHGATEWAY_URL}/metrics/job/web_healthcheck/instance/${endpoint_id}"
    
    log "DEBUG: About to push to Pushgateway"
    log "DEBUG: Push URL: $push_url"
    log "DEBUG: Payload to push:"
    log "$metrics_payload"
    
    # Test if we can reach Pushgateway first
    log "DEBUG: Testing Pushgateway connectivity..."
    if curl -s -f "${PUSHGATEWAY_URL}/metrics" > /dev/null 2>&1; then
        log "DEBUG: Pushgateway is reachable at ${PUSHGATEWAY_URL}"
    else
        log "DEBUG: WARNING - Cannot reach Pushgateway at ${PUSHGATEWAY_URL}"
    fi
    
    # Push with full response capture
    push_response=$(curl -s -w "\nHTTP_CODE:%{http_code}" -X POST -H "Content-Type: text/plain" --data-binary "$metrics_payload" "$push_url" 2>&1)
    push_exit_code=$?
    push_http_code=$(echo "$push_response" | grep "HTTP_CODE:" | cut -d: -f2)
    
    log "DEBUG: Push exit code: $push_exit_code"
    log "DEBUG: Push HTTP code: $push_http_code"
    if [ -n "$push_response" ]; then
        log "DEBUG: Push response: $push_response"
    fi
    
    if [ $push_exit_code -eq 0 ]; then
        log "Metrics pushed for $endpoint_id (HTTP $push_http_code)"
        
        # Wait a moment for Pushgateway to process
        sleep 1
        
        # VERIFY: Read back the metric we just pushed
        log "VERIFY: Reading back metrics from Pushgateway..."
        log "VERIFY: Fetching all metrics from ${PUSHGATEWAY_URL}/metrics"
        
        all_metrics=$(curl -s "${PUSHGATEWAY_URL}/metrics")
        verification=$(echo "$all_metrics" | grep "web_endpoint_health.*${endpoint_id}")
        
        if [ -n "$verification" ]; then
            log "VERIFY SUCCESS: Metric found in Pushgateway:"
            log "$verification"
        else
            log "VERIFY FAILED: Metric NOT found in Pushgateway!"
            log "VERIFY: Checking all web_endpoint_health metrics:"
            web_health_metrics=$(echo "$all_metrics" | grep "web_endpoint_health")
            if [ -n "$web_health_metrics" ]; then
                echo "$web_health_metrics" | while read -r line; do
                    log "  $line"
                done
            else
                log "  No web_endpoint_health metrics found at all!"
            fi
            log "VERIFY: First 50 lines of all Pushgateway metrics:"
            echo "$all_metrics" | head -50 | while read -r line; do
                log "  $line"
            done
        fi
    else
        log "WARNING: Failed to push metrics for $endpoint_id (exit code: $push_exit_code)"
    fi
done

log "Health check cycle complete"

