#!/bin/bash
set -e
source "$(dirname "$0")/lib/helpers.sh"

test_start "Timers"

ID="timer-test-$(date +%s)"
TIMER_ID="t-$(date +%s)"

# Clear any previous fires
post "/timer/$ID/clear-fires" "{}" > /dev/null
test_pass "cleared previous fires"

# Schedule a timer with 2 second delay
post "/timer/$ID/schedule" "{\"timer_id\": \"$TIMER_ID\", \"delay_ms\": 2000}" > /dev/null
test_pass "timer scheduled"

# Check the timer is pending
PENDING=$(get "/timer/$ID/pending")
assert_contains "$PENDING" "$TIMER_ID"
test_pass "timer is pending"

# Check not fired yet
FIRES=$(get "/timer/$ID/fires")
if [[ "$FIRES" == *"$TIMER_ID"* ]]; then
    test_fail "timer fired too early"
fi
test_pass "timer not fired immediately"

# Wait for timer to fire
echo "Waiting for timer to fire..."
sleep 3

# Check timer has fired
FIRES=$(get "/timer/$ID/fires")
assert_contains "$FIRES" "$TIMER_ID"
test_pass "timer fired after delay"

# Check timer is no longer pending
PENDING=$(get "/timer/$ID/pending")
if [[ "$PENDING" == *"$TIMER_ID"* ]]; then
    test_fail "fired timer still appears as pending"
fi
test_pass "fired timer removed from pending"

# Test cancellation
TIMER_ID2="t2-$(date +%s)"
post "/timer/$ID/schedule" "{\"timer_id\": \"$TIMER_ID2\", \"delay_ms\": 5000}" > /dev/null
test_pass "second timer scheduled"

# Verify it's pending
PENDING=$(get "/timer/$ID/pending")
assert_contains "$PENDING" "$TIMER_ID2"
test_pass "second timer is pending"

# Cancel the timer
RESULT=$(post "/timer/$ID/cancel" "{\"timer_id\": \"$TIMER_ID2\"}")
assert_eq "$RESULT" "true"
test_pass "timer cancel returned true"

# Verify it's no longer pending
PENDING=$(get "/timer/$ID/pending")
if [[ "$PENDING" == *"$TIMER_ID2"* ]]; then
    test_fail "cancelled timer still pending"
fi
test_pass "cancelled timer removed from pending"

# Wait a moment and verify it never fired
sleep 1
FIRES=$(get "/timer/$ID/fires")
if [[ "$FIRES" == *"$TIMER_ID2"* ]]; then
    test_fail "cancelled timer still fired"
fi
test_pass "cancelled timer did not fire"

# Try to cancel a non-existent timer
RESULT=$(post "/timer/$ID/cancel" "{\"timer_id\": \"nonexistent\"}")
assert_eq "$RESULT" "false"
test_pass "cancel of non-existent timer returns false"

# Test multiple timers
ID2="timer-test-2-$(date +%s)"
post "/timer/$ID2/clear-fires" "{}" > /dev/null
TIMER_A="multi-a-$(date +%s)"
TIMER_B="multi-b-$(date +%s)"

post "/timer/$ID2/schedule" "{\"timer_id\": \"$TIMER_A\", \"delay_ms\": 1000}" > /dev/null
post "/timer/$ID2/schedule" "{\"timer_id\": \"$TIMER_B\", \"delay_ms\": 1500}" > /dev/null
test_pass "multiple timers scheduled"

# Wait for both to fire
sleep 2

FIRES=$(get "/timer/$ID2/fires")
assert_contains "$FIRES" "$TIMER_A"
assert_contains "$FIRES" "$TIMER_B"
test_pass "multiple timers fired"

# Clear fires and verify
post "/timer/$ID2/clear-fires" "{}" > /dev/null
FIRES=$(get "/timer/$ID2/fires")
assert_eq "$FIRES" "[]"
test_pass "clear fires works"

# Different entity IDs have independent timers
ID3="timer-test-3-$(date +%s)"
post "/timer/$ID3/schedule" "{\"timer_id\": \"independent\", \"delay_ms\": 10000}" > /dev/null
PENDING_ID2=$(get "/timer/$ID2/pending")
if [[ "$PENDING_ID2" == *"independent"* ]]; then
    test_fail "timers not independent between entities"
fi
test_pass "entity IDs have independent timers"

# Cleanup - cancel the long timer
post "/timer/$ID3/cancel" "{\"timer_id\": \"independent\"}" > /dev/null

echo "All timer tests passed"
