# PostgreSQL Legacy Schema Documentation

## Database Overview

**Database:** customer360  
**PostgreSQL Version:** 14/15  
**Environment:** Docker container  
**Source:** dbt project (~/customer360-prod)  
**Total Schemas:** 3 (raw, staging, warehouse)  
**Total Tables:** 10  

**Note:** Documentation extracted from dbt models, not live database.

---

## Schema Architecture
```
PostgreSQL: customer360
├── raw (Landing Zone - CSV imports)
│   ├── csv_customers
│   ├── csv_orders
│   └── csv_products
│
├── staging (Cleaned & Validated)
│   ├── stg_csv_customers
│   ├── stg_csv_orders
│   └── stg_csv_products
│
└── warehouse (Analytics Layer)
    ├── dim_customers (deduplicated)
    ├── customer_rfm (segmentation)
    ├── customer_health (scoring)
    └── cohort_retention (cohort analysis)
```

---

## RAW Layer

### Source: CSV Files
**Location:** ~/customer360-prod/data/ (or equivalent)

### raw.csv_customers
**Columns (from dbt source):**
- customer_id: INTEGER
- email: VARCHAR(255)
- first_name: VARCHAR(100)
- last_name: VARCHAR(100)
- created_at: TEXT (mixed formats)
- country: VARCHAR(50)
- postal_code: VARCHAR(20)

**Estimated rows:** ~5,437

### raw.csv_orders
**Columns:**
- order_id: INTEGER
- customer_id: INTEGER (FK)
- product_id: INTEGER (FK)
- order_date: TEXT
- quantity: INTEGER
- unit_price: NUMERIC(10,2)
- total_amount: NUMERIC(10,2)

**Estimated rows:** ~15,000

### raw.csv_products
**Columns:**
- product_id: INTEGER
- product_name: VARCHAR(200)
- category: VARCHAR(100)
- price: NUMERIC(10,2)

**Estimated rows:** ~500

---

## STAGING Layer (from dbt models)

### staging.stg_csv_customers
**Source file:** models/staging/stg_csv_customers.sql

**Key Transformations:**
```sql
-- Email normalization
LOWER(TRIM(email)) as email_normalized

-- Date parsing (multi-format)
CASE
    WHEN created_at ~ '^\d{4}-\d{2}-\d{2}' THEN created_at::DATE
    WHEN created_at ~ '^\d{2}/\d{2}/\d{4}' THEN TO_DATE(created_at, 'DD/MM/YYYY')
    ELSE NULL
END as created_at_parsed

-- Email validation
CASE 
    WHEN email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$' 
    THEN TRUE 
    ELSE FALSE 
END as is_valid_email

-- Surrogate key
MD5(customer_id::TEXT) as customer_key
```

**Output columns:**
- customer_id: INTEGER
- email_normalized: VARCHAR(255)
- first_name: VARCHAR(100)
- last_name: VARCHAR(100)
- created_at_parsed: DATE
- country: VARCHAR(50)
- is_valid_email: BOOLEAN
- customer_key: VARCHAR(32)

---

## WAREHOUSE Layer (from dbt models)

### warehouse.dim_customers
**Source:** models/marts/dim_customers.sql or intermediate/int_customer_deduped.sql

**Deduplication Logic:**
```sql
ROW_NUMBER() OVER (
    PARTITION BY email_normalized
    ORDER BY created_at_parsed ASC, customer_id ASC
) as row_num

-- Keep only row_num = 1
WHERE row_num = 1
```

**Result:** 5,437 → 4,501 unique customers

---

### warehouse.customer_rfm
**Source:** models/marts/customer_rfm.sql

**RFM Scoring:**
```sql
-- Recency (days since last order)
CURRENT_DATE - MAX(order_date) as recency_days

-- Frequency (number of orders)
COUNT(DISTINCT order_id) as frequency

-- Monetary (total spend)
SUM(total_amount) as monetary

-- Scoring
CASE
    WHEN recency_days <= 30 THEN 5
    WHEN recency_days <= 60 THEN 4
    WHEN recency_days <= 90 THEN 3
    WHEN recency_days <= 180 THEN 2
    ELSE 1
END as recency_score

NTILE(5) OVER (ORDER BY frequency) as frequency_score
NTILE(5) OVER (ORDER BY monetary) as monetary_score
```

**Segments:**
- VIP: R≥4, F≥4, M≥4 (~450 customers)
- Champion: R≥4, F≥3 (~800 customers)
- Loyal: R≥3, F≥3
- At Risk: R≤2, F≥3
- Lost: R≤2, F≤2

---

### warehouse.customer_health
**Source:** models/marts/customer_health.sql

**Health Score Formula:**
```sql
Health Score = 
    (recency_score * 25) +
    (frequency_score * 25) +
    (monetary_score * 30) +
    (CASE WHEN is_valid_email THEN 20 ELSE 0 END)

Range: 0-100
```

**Classification:**
- Excellent: ≥80
- Good: ≥60
- Fair: ≥40
- At Risk: <40

---

## PostgreSQL-Specific Syntax (to translate for BigQuery)

### 1. Type Casting (::)
```sql
-- PostgreSQL
created_at::DATE
customer_id::TEXT
amount::NUMERIC

-- BigQuery equivalent
CAST(created_at AS DATE)
CAST(customer_id AS STRING)
CAST(amount AS NUMERIC)
```

### 2. Regex Operator (~)
```sql
-- PostgreSQL
WHERE email ~ '^[A-Za-z0-9._%+-]+@'

-- BigQuery
WHERE REGEXP_CONTAINS(email, r'^[A-Za-z0-9._%+-]+@')
```

### 3. Date Functions
```sql
-- PostgreSQL
CURRENT_DATE - order_date  -- Returns integer (days)

-- BigQuery
DATE_DIFF(CURRENT_DATE(), order_date, DAY)
```

---

## Data Type Mapping

| PostgreSQL | BigQuery | Notes |
|-----------|----------|-------|
| INTEGER | INT64 | All integers → INT64 |
| SERIAL | INT64 | No auto-increment in BigQuery |
| VARCHAR(n) | STRING | No length limit |
| TEXT | STRING | Same as VARCHAR |
| TIMESTAMP | TIMESTAMP | Compatible |
| DATE | DATE | Compatible |
| NUMERIC(p,s) | NUMERIC | Compatible |
| BOOLEAN | BOOL | Compatible |

---

## Key Business Metrics (to preserve in migration)

✅ **Total customers (raw):** ~5,437  
✅ **Unique customers (after dedup):** ~4,501  
✅ **VIP customers:** ~450  
✅ **Total orders:** ~15,000  
✅ **Products:** ~500  

**If these numbers don't match after migration → Migration failed!**
