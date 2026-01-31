#!/bin/bash

BASE_URL="${CLUSTER_TESTS_URL:-http://localhost:8080}"

# HTTP helpers
post() { curl -sf -X POST "$BASE_URL$1" -H "Content-Type: application/json" -d "$2"; }
get() { curl -sf "$BASE_URL$1"; }
delete() { curl -sf -X DELETE "$BASE_URL$1"; }

# Assertions
assert_eq() {
    if [ "$1" != "$2" ]; then
        echo "FAIL: expected '$2', got '$1'"
        exit 1
    fi
}

assert_contains() {
    if [[ "$1" != *"$2"* ]]; then
        echo "FAIL: expected '$1' to contain '$2'"
        exit 1
    fi
}

# Test lifecycle
test_start() { echo "=== $1 ==="; }
test_pass() { echo "PASS: $1"; }
test_fail() { echo "FAIL: $1"; exit 1; }
