# dbt Setup Guide

## Installation
```bash
# Install dbt-bigquery
pip install dbt-bigquery

# Verify
dbt --version
```

---

## Project Structure
```
customer360_dbt/
├── models/
│   ├── staging/          # Cleaned data (views)
│   │   ├── sources.yml   # Source definitions
│   │   └── stg_*.sql     # Staging models
│   ├── intermediate/     # Deduplication logic (tables)
│   └── marts/            # Analytics models (tables)
├── macros/               # Reusable SQL functions
├── tests/                # Custom data tests
├── dbt_project.yml       # Project configuration
└── README.md

~/.dbt/
└── profiles.yml          # Connection configuration
```

---

## Configuration

### profiles.yml (~/.dbt/profiles.yml)
```yaml
customer360_dbt:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: YOUR_PROJECT_ID  # ← Change this!
      dataset: analytics
      threads: 4
      location: EU
```

**Authentication:** Uses `gcloud auth application-default login`

---

## Common Commands
```bash
# Test connection
dbt debug

# Run all models
dbt run

# Run specific model
dbt run --select stg_csv_customers

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve

# Clean artifacts
dbt clean
```

---

## Model Configuration

### Materialization Types
```sql
-- VIEW (default for staging)
{{ config(materialized='view') }}

-- TABLE (for intermediate/marts)
{{ config(materialized='table') }}

-- INCREMENTAL (for large tables)
{{ config(materialized='incremental') }}
```

### Clustering
```sql
{{ config(
    materialized='table',
    cluster_by=['customer_key']
) }}
```

---

## Sources

Define raw tables in `sources.yml`:
```yaml
sources:
  - name: raw_data
    database: YOUR_PROJECT_ID
    schema: raw_data
    tables:
      - name: csv_customers
```

Reference in models:
```sql
SELECT * FROM {{ source('raw_data', 'csv_customers') }}
```

---

## Troubleshooting

### "Project not found"
- Check `project:` in profiles.yml matches your GCP project ID
- Run: `gcloud config get-value project`

### "Permission denied"
- Re-authenticate: `gcloud auth application-default login`

### "dbt: command not found"
- Activate venv: `source venv/bin/activate`
- Or use full path: `./venv/bin/dbt`

### "Compilation Error"
- Check SQL syntax
- Verify source definitions
- Run: `dbt compile` to see compiled SQL

---

## Next Steps

1. ✅ dbt installed and tested
2. ⏭️ Create staging models (stg_csv_customers)
3. ⏭️ Create intermediate models (deduplication)
4. ⏭️ Create mart models (RFM, health scoring)
5. ⏭️ Add tests and documentation

