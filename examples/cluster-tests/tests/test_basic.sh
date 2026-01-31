#!/bin/bash
set -e
source "$(dirname "$0")/lib/helpers.sh"

test_start "Basic Counter Operations"

# Increment
RESULT=$(post "/counter/test1/increment" "5")
assert_eq "$RESULT" "5"
test_pass "increment returns new value"

# Get
RESULT=$(get "/counter/test1")
assert_eq "$RESULT" "5"
test_pass "get returns current value"

# Multiple increments
post "/counter/test1/increment" "3" > /dev/null
RESULT=$(get "/counter/test1")
assert_eq "$RESULT" "8"
test_pass "multiple increments accumulate"

# Decrement
RESULT=$(post "/counter/test1/decrement" "2")
assert_eq "$RESULT" "6"
test_pass "decrement works"

# Reset
post "/counter/test1/reset" "{}" > /dev/null
RESULT=$(get "/counter/test1")
assert_eq "$RESULT" "0"
test_pass "reset works"

# Different entity IDs are independent
post "/counter/test2/increment" "100" > /dev/null
RESULT=$(get "/counter/test1")
assert_eq "$RESULT" "0"
RESULT=$(get "/counter/test2")
assert_eq "$RESULT" "100"
test_pass "entity IDs are independent"

echo "All basic tests passed"
