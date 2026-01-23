#!/bin/bash

# Quick validation script
# Runs reconciliation and exits with error code if failed

set -e

echo "=========================================="
echo "MIGRATION VALIDATION"
echo "=========================================="
echo ""

# Activate venv
source venv/bin/activate

# Run reconciliation
python migration_scripts/reconcile.py \
    --table raw.csv_customers \
    --bq-table raw_data.csv_customers

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    echo "=========================================="
    echo "✅ VALIDATION PASSED"
    echo "=========================================="
    exit 0
else
    echo ""
    echo "=========================================="
    echo "❌ VALIDATION FAILED"
    echo "=========================================="
    exit 1
fi
