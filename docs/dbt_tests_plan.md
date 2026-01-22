# dbt Tests Plan for BigQuery Migration

## Purpose

Define **data quality tests** to run automatically with dbt after migration.

**Goal:** Catch data quality issues early → Don't deploy bad data to production!

---

## Test Categories

### 1. Schema Tests (Built-in dbt)

#### staging.stg_csv_customers
```yaml
# models/staging/schema.yml
version: 2

models:
  - name: stg_csv_customers
    description: "Cleaned customer data from raw CSV"
    columns:
      - name: customer_id
        description: "Unique customer identifier"
        tests:
          - not_null
          - unique
      
      - name: email_clean
        description: "Normalized email (lowercase, trimmed)"
        tests:
          - not_null  # At least one of email_clean or email_raw should exist
      
      - name: created_at_parsed
        description: "Parsed date from mixed formats"
        tests:
          - not_null  # Should parse successfully for all records
      
      - name: is_valid_email
        description: "Boolean flag for email format validity"
        tests:
          - not_null
          - accepted_values:
              values: [true, false]
```

---

#### warehouse.dim_customers
```yaml
# models/marts/schema.yml
version: 2

models:
  - name: dim_customers
    description: "Deduplicated golden customer records"
    tests:
      - dbt_utils.equal_rowcount:
          compare_model: ref('int_customer_deduped')
    columns:
      - name: customer_key
        description: "Surrogate key (MD5 hash)"
        tests:
          - unique
          - not_null
      
      - name: email
        description: "Normalized email"
        tests:
          - unique  # No duplicates allowed!
          - not_null
      
      - name: created_at
        description: "Parsed creation date"
        tests:
          - not_null
```

---

#### warehouse.customer_rfm
```yaml
  - name: customer_rfm
    description: "RFM segmentation"
    columns:
      - name: customer_key
        tests:
          - unique
          - not_null
          - relationships:
              to: ref('dim_customers')
              field: customer_key
      
      - name: rfm_segment
        tests:
          - not_null
          - accepted_values:
              values: ['VIP', 'Champion', 'Loyal', 'At Risk', 'Lost', 'Others']
      
      - name: recency_score
        tests:
          - not_null
          - accepted_values:
              values: [1, 2, 3, 4, 5]
      
      - name: frequency_score
        tests:
          - not_null
          - accepted_values:
              values: [1, 2, 3, 4, 5]
      
      - name: monetary_score
        tests:
          - not_null
          - accepted_values:
              values: [1, 2, 3, 4, 5]
```

---

### 2. Custom Data Tests

#### Test: Email Format Validity
```sql
-- tests/generic/test_email_format.sql
{% test email_format(model, column_name) %}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
  AND NOT REGEXP_CONTAINS({{ column_name }}, r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$')

{% endtest %}
```

**Usage:**
```yaml
columns:
  - name: email_clean
    tests:
      - email_format
```

---

#### Test: Row Count Match (PostgreSQL vs BigQuery)
```sql
-- tests/assert_row_count_match.sql
-- Expected: 5,437 rows in raw layer
SELECT
    COUNT(*) as actual_count,
    5437 as expected_count
FROM {{ ref('raw_csv_customers') }}
WHERE COUNT(*) != 5437
```

---

#### Test: Deduplication Worked
```sql
-- tests/assert_deduplication_count.sql
-- Expected: 4,501 unique customers
SELECT
    COUNT(*) as actual_unique,
    4501 as expected_unique
FROM {{ ref('dim_customers') }}
WHERE COUNT(*) NOT BETWEEN 4480 AND 4520  -- ±20 tolerance
```

---

#### Test: No Future Dates
```sql
-- tests/assert_no_future_dates.sql
SELECT *
FROM {{ ref('stg_csv_customers') }}
WHERE created_at_parsed > CURRENT_DATE()
```

---

#### Test: VIP Count in Range
```sql
-- tests/assert_vip_count.sql
WITH vip_count AS (
    SELECT COUNT(*) as vips
    FROM {{ ref('customer_rfm') }}
    WHERE rfm_segment = 'VIP'
)
SELECT *
FROM vip_count
WHERE vips NOT BETWEEN 430 AND 470  -- 450 ±20
```

---

### 3. dbt_expectations Tests (Advanced)

Install: `pip install dbt-expectations`
```yaml
# models/staging/schema.yml
models:
  - name: stg_csv_customers
    tests:
      - dbt_expectations.expect_table_row_count_to_equal:
          value: 5437
      
      - dbt_expectations.expect_column_values_to_be_between:
          column_name: created_at_parsed
          min_value: '2020-01-01'
          max_value: 'CURRENT_DATE()'
      
      - dbt_expectations.expect_column_values_to_match_regex:
          column_name: phone_clean
          regex: '^\+?\d{10,15}$'
```

---

## Test Execution Strategy

### Pre-Migration (PostgreSQL)
```bash
# Run all tests on PostgreSQL dbt project
cd ~/customer360-prod/dbt_project
dbt test
```

**Expected:** All tests pass ✅ (baseline)

---

### Post-Migration (BigQuery)
```bash
# Run same tests on BigQuery dbt project
cd ~/customer360-bigquery-migration/dbt_project
dbt test
```

**Expected:** All tests pass ✅ (migration successful!)

---

### Continuous Monitoring (Production)
```bash
# Run tests daily via cron or Airflow
dbt test --select tag:critical

# Fail pipeline if critical tests fail
dbt test --select tag:critical || exit 1
```

---

## Test Tagging Strategy
```yaml
# Tag tests by severity
models:
  - name: dim_customers
    meta:
      tags: ['critical', 'daily']
    tests:
      - unique:
          column_name: customer_key
          tags: ['critical']
      
      - not_null:
          column_name: email
          tags: ['critical']
```

**Run critical tests only:**
```bash
dbt test --select tag:critical
```

---

## Expected Test Results

### Passing Tests (✅ Expected)

- `unique` on `customer_key` → 4,501 unique values
- `not_null` on `email` → 5,425 non-null (12 nulls acceptable)
- `accepted_values` on `rfm_segment` → 6 valid values
- `row_count` → 5,437 in raw, 4,501 in warehouse

### Failing Tests (⚠️  Expected - Known Issues)

- `email_format` on 63 records → Known invalid emails
  - **Action:** Flag as warnings, not errors
  - **Fix:** Business team to correct at source

---

## Summary

**Total tests planned:** ~30
- Built-in schema tests: ~15
- Custom data tests: ~10
- dbt_expectations: ~5

**Coverage:**
- ✅ Uniqueness (no duplicates)
- ✅ Completeness (no unexpected nulls)
- ✅ Validity (format checks)
- ✅ Consistency (referential integrity)
- ✅ Accuracy (row counts match baseline)

**Next step:** Implement tests during dbt migration (Days 12-17)

