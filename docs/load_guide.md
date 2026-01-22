# BigQuery Load Guide

## Overview

Load CSV files (extracted from PostgreSQL) into BigQuery tables.

**Location:** `migration_scripts/load_to_bigquery.py`

---

## Usage

### Load Single File
```bash
python migration_scripts/load_to_bigquery.py \
    --file data/exports/raw/raw_csv_customers.csv \
    --table raw_data.csv_customers
```

### Load All Files from Schema
```bash
python migration_scripts/load_to_bigquery.py --schema raw
```

### Load All Files
```bash
python migration_scripts/load_to_bigquery.py --all
```

---

## Schema Mapping

| PostgreSQL Schema | BigQuery Dataset |
|-------------------|------------------|
| `raw`             | `raw_data`       |
| `staging`         | `staging`        |
| `warehouse`       | `analytics`      |

---

## Load Configuration

- **Source format:** CSV
- **Skip header:** Yes (first row)
- **Schema detection:** Auto (BigQuery infers types)
- **Write mode:** WRITE_TRUNCATE (overwrite existing data)
- **Error handling:** Fail on any bad record

---

## Expected Results

### raw.csv_customers
```
Source: data/exports/raw/raw_csv_customers.csv
Target: raw_data.csv_customers
Rows: 5,000
Size: 0.85 MB
Schema: 11 fields (auto-detected)
```

---

## Validation

### Via BigQuery UI

1. Go to: https://console.cloud.google.com/bigquery
2. Navigate to: `raw_data` → `csv_customers`
3. Click **"PREVIEW"** tab
4. Verify 5,000 rows

### Via CLI
```bash
# Count rows
bq query --nouse_legacy_sql 'SELECT COUNT(*) FROM `PROJECT_ID.raw_data.csv_customers`'

# Sample data
bq query --nouse_legacy_sql 'SELECT * FROM `PROJECT_ID.raw_data.csv_customers` LIMIT 10'
```

### Via Python
```python
from google.cloud import bigquery

client = bigquery.Client()
query = "SELECT COUNT(*) as count FROM `PROJECT_ID.raw_data.csv_customers`"
result = client.query(query).to_dataframe()
print(result)
```

---

## Troubleshooting

### Error: "Dataset not found"
```bash
# Create dataset
bq mk --dataset --location=EU PROJECT_ID:raw_data

# Or run setup script
./scripts/create_bigquery_datasets.sh
```

### Error: "File not found"
```bash
# Check CSV exists
ls -lh data/exports/raw/raw_csv_customers.csv

# If not, extract first (Day 7)
python migration_scripts/extract_from_postgres.py --schema raw
```

### Error: "Permission denied"
```bash
# Re-authenticate
gcloud auth application-default login

# Verify project
gcloud config get-value project
```

### Error: "Invalid CSV"
```bash
# Check CSV format
head -10 data/exports/raw/raw_csv_customers.csv

# Verify no corrupted rows
wc -l data/exports/raw/raw_csv_customers.csv
```

---

## Next Steps

After loading:
1. ✅ Verify row counts match PostgreSQL
2. ✅ Check schema auto-detection was correct
3. ⏭️  Day 9: Create proper table schemas (with clustering)
4. ⏭️  Day 10: Build reconciliation framework

