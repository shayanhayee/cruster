#!/bin/bash
set -e
source "$(dirname "$0")/lib/helpers.sh"

test_start "Singleton Entities"

# Reset state for clean testing
post "/singleton/reset-sequence" "{}" > /dev/null
post "/singleton/clear-history" "{}" > /dev/null
test_pass "reset singleton state"

# Get sequence numbers (should be unique and increasing)
SEQ1=$(post "/singleton/next-sequence" "{}")
SEQ2=$(post "/singleton/next-sequence" "{}")
SEQ3=$(post "/singleton/next-sequence" "{}")

# Verify uniqueness
if [ "$SEQ1" -eq "$SEQ2" ] || [ "$SEQ2" -eq "$SEQ3" ] || [ "$SEQ1" -eq "$SEQ3" ]; then
    test_fail "sequence numbers not unique: $SEQ1, $SEQ2, $SEQ3"
fi
test_pass "sequence numbers unique"

# Verify ordering (each should be greater than the previous)
if [ "$SEQ2" -le "$SEQ1" ] || [ "$SEQ3" -le "$SEQ2" ]; then
    test_fail "sequence numbers not increasing: $SEQ1, $SEQ2, $SEQ3"
fi
test_pass "sequence numbers increasing"

# Verify they started from 1 (after reset)
if [ "$SEQ1" -ne "1" ]; then
    test_fail "first sequence after reset should be 1, got: $SEQ1"
fi
test_pass "sequence starts at 1 after reset"

# Get many sequence numbers quickly to test atomicity
for i in $(seq 1 10); do
    post "/singleton/next-sequence" "{}" > /dev/null
done
SEQ_AFTER=$(post "/singleton/next-sequence" "{}")
EXPECTED=$((3 + 10 + 1))  # 3 from before + 10 in loop + 1 final
if [ "$SEQ_AFTER" -ne "$EXPECTED" ]; then
    test_fail "sequence counter mismatch: expected $EXPECTED, got $SEQ_AFTER"
fi
test_pass "rapid sequence generation works correctly"

# Check current runner
RUNNER=$(get "/singleton/current-runner")
if [ -z "$RUNNER" ] || [ "$RUNNER" = "null" ] || [ "$RUNNER" = "\"\"" ]; then
    test_fail "no runner reported: got '$RUNNER'"
fi
test_pass "singleton has runner"

# Record a leadership change
post "/singleton/record-leadership" "{\"runner_id\": \"test-runner-1\"}" > /dev/null
test_pass "recorded leadership change"

# Check leadership history
HISTORY=$(get "/singleton/leader-history")
assert_contains "$HISTORY" "test-runner-1"
test_pass "leadership history contains recorded change"

# Record another leadership change
post "/singleton/record-leadership" "{\"runner_id\": \"test-runner-2\"}" > /dev/null
HISTORY=$(get "/singleton/leader-history")
assert_contains "$HISTORY" "test-runner-1"
assert_contains "$HISTORY" "test-runner-2"
test_pass "leadership history accumulates"

# Clear history
post "/singleton/clear-history" "{}" > /dev/null
HISTORY=$(get "/singleton/leader-history")
assert_eq "$HISTORY" "[]"
test_pass "clear history works"

# Test reset sequence
post "/singleton/reset-sequence" "{}" > /dev/null
SEQ=$(post "/singleton/next-sequence" "{}")
if [ "$SEQ" -ne "1" ]; then
    test_fail "sequence should be 1 after reset, got: $SEQ"
fi
test_pass "reset sequence works"

echo "All singleton tests passed"
