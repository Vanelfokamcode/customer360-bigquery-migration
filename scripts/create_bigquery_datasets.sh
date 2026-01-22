#!/bin/bash

# BigQuery Dataset Creation Script
# Creates all datasets for customer360 migration

set -e  # Exit on error

# Configuration
PROJECT_ID=$(gcloud config get-value project)
LOCATION="EU"  # Multi-region EU (Belgium + Netherlands)

echo "================================================"
echo "Creating BigQuery Datasets"
echo "Project: $PROJECT_ID"
echo "Location: $LOCATION"
echo "================================================"
echo ""

# Function to create dataset
create_dataset() {
    local dataset_name=$1
    local description=$2
    
    echo "Creating dataset: $dataset_name"
    
    bq mk \
        --dataset \
        --location=$LOCATION \
        --description="$description" \
        ${PROJECT_ID}:${dataset_name}
    
    if [ $? -eq 0 ]; then
        echo "✅ Dataset $dataset_name created successfully"
    else
        echo "⚠️  Dataset $dataset_name may already exist (skipping)"
    fi
    echo ""
}

# Create datasets
create_dataset "raw_data" \
    "Raw landing zone - immutable data as received from sources"

create_dataset "staging" \
    "Staging layer - cleaned and typed data"

create_dataset "analytics" \
    "Analytics layer - business-ready data"

create_dataset "governance" \
    "Governance layer - observability and data quality"

echo "================================================"
echo "✅ All datasets created!"
echo "================================================"
echo ""

# List all datasets
echo "Listing datasets in project $PROJECT_ID:"
bq ls --project_id=$PROJECT_ID

echo ""
echo "Next steps:"
echo "1. Verify datasets in BigQuery UI"
echo "2. Ready for Day 5: Data quality assessment"
