#!/bin/bash
set -e

SCRIPT_DIR="$(dirname "$0")"

echo "========================================="
echo "Cluster E2E Tests"
echo "========================================="
echo "Target: ${CLUSTER_TESTS_URL:-http://localhost:8080}"
echo ""

# Health check
echo "Checking cluster health..."
if ! curl -sf "${CLUSTER_TESTS_URL:-http://localhost:8080}/health" > /dev/null; then
    echo "FAIL: cluster not healthy"
    exit 1
fi
echo "Cluster healthy"
echo ""

# Run all test suites
"$SCRIPT_DIR/test_basic.sh"
echo ""

"$SCRIPT_DIR/test_persistence.sh"
echo ""

"$SCRIPT_DIR/test_workflows.sh"
echo ""

"$SCRIPT_DIR/test_activities.sh"
echo ""

"$SCRIPT_DIR/test_traits.sh"
echo ""

"$SCRIPT_DIR/test_timers.sh"
echo ""

"$SCRIPT_DIR/test_cross_entity.sh"
echo ""

"$SCRIPT_DIR/test_singletons.sh"
echo ""

echo "========================================="
echo "All E2E tests passed!"
echo "========================================="
