# PostgreSQL Legacy Schema Documentation

## Database Overview

**Database:** customer360  
**PostgreSQL Version:** 14/15  
**Environment:** Docker container  
**Source Project:** ~/customer360-prod  
**Architecture:** 3-layer data warehouse (Medallion Architecture)  
**Total Schemas:** 3 + metadata  
**Total Tables:** 10 core tables + 1 observability table  

---

## 📊 Architecture: 3-Layer Medallion Pattern
```
┌─────────────────────────────────────────────────────────────┐
│ LAYER 1: RAW (Bronze)                                       │
│ - Immutable landing zone                                    │
│ - Data "as received" from sources                           │
│ - NEVER delete/update (audit trail)                         │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ LAYER 2: STAGING (Silver)                                   │
│ - Light cleanup: typing, normalization                      │
│ - NO business logic                                         │
│ - Idempotent transformations                                │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ LAYER 3: WAREHOUSE (Gold)                                   │
│ - Business-ready data                                       │
│ - Golden records, metrics, segments                         │
│ - What analysts/dashboards consume                          │
└─────────────────────────────────────────────────────────────┘
```

**Why 3 layers?**
- **Separation of concerns:** Raw data ≠ Clean data ≠ Business logic
- **Auditability:** Can always trace back to original source
- **Idempotency:** Can re-run pipelines without data corruption
- **Debugging:** Know exactly where transformation failed

---

## 🗄️ Schema Details

### SCHEMA: raw

**Purpose:** Immutable landing zone - data as received from sources  
**Rule:** NEVER modify or delete from raw (source of truth for audit)  
**Tracking:** Every table has `loaded_at` timestamp for data lineage  

**Tables:**
- `raw.csv_customers` (primary table)

**Why `loaded_at` in every raw table?**
- Track when data arrived (data lineage)
- Detect ingestion lag (e.g., yesterday's data arriving today)
- Debug pipeline issues ("When did this batch load?")

---

### SCHEMA: staging

**Purpose:** Cleaned and typed data - no business logic yet  
**Transformations:**
- Type casting (VARCHAR → proper types)
- Normalization (email lowercase, trim whitespace)
- Validation (email format, date parsing)
- **NO** aggregations, joins, or business rules

**Tables:**
- `staging.stg_csv_customers` (dbt view)

**Idempotency:** Can re-run staging models without side effects

---

### SCHEMA: warehouse

**Purpose:** Business-ready data - golden records and metrics  
**Content:**
- Customer dimension (deduplicated golden records)
- RFM segmentation
- Health scores
- Cohort retention analysis

**Tables:**
- `warehouse.dim_customers` (golden customer records)
- `warehouse.customer_rfm` (RFM segments)
- `warehouse.customer_health` (health scoring)
- `warehouse.cohort_retention` (retention analysis)
- `warehouse.pipeline_metadata` (observability)

---

## 📋 RAW Layer Tables

### raw.csv_customers

**Source:** CSV file (messy_customers.csv)  
**Rows:** ~5,437  
**Purpose:** Customer master data - intentionally messy for data quality demo  

**DDL:**
```sql
CREATE TABLE raw.csv_customers (
    -- ID unique (from CSV)
    customer_id VARCHAR(100) PRIMARY KEY,
    
    -- Personal information
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(50),
    
    -- Address
    address TEXT,
    city VARCHAR(100),
    country VARCHAR(10),
    
    -- Timestamps
    created_at VARCHAR(50),  -- VARCHAR because mixed formats!
    
    -- Ingestion metadata
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(255)
);

-- Indexes for fast lookup
CREATE INDEX idx_csv_customers_email ON raw.csv_customers(email);
CREATE INDEX idx_csv_customers_loaded_at ON raw.csv_customers(loaded_at);
```

**Data Quality Issues (by design):**
- **Email:** Mixed case, extra spaces, some invalid formats
- **Names:** Contains special chars (™, ©)
- **Phone:** Mixed formats (+33, 33, with/without spaces)
- **Dates:** 3 different formats:
  - ISO: `2023-01-15`
  - European: `15/01/2023`
  - US: `01-15-2023`

**Why VARCHAR for created_at?**
- Raw data has **mixed date formats** → Can't use DATE type
- Will be parsed in staging layer

---

## 🧹 STAGING Layer Tables

### staging.stg_csv_customers

**Type:** dbt view (materialized='view')  
**Source:** `{{ source('raw', 'csv_customers') }}`  
**Purpose:** Clean and validate customer data  

**Key Transformations:**

#### 1. Email Cleaning
```sql
CASE
    WHEN email IS NULL THEN NULL
    WHEN TRIM(email) NOT LIKE '%@%' THEN NULL  -- Invalid format
    ELSE LOWER(TRIM(email))                     -- Normalize
END AS email_clean
```

**Result:**
- `"  JOHN@GMAIL.COM  "` → `"john@gmail.com"`
- `"invalid-email"` → `NULL`

#### 2. Name Cleaning
```sql
INITCAP(
    TRIM(
        REGEXP_REPLACE(first_name, '[™©]', '', 'g')  -- Remove special chars
    )
) AS first_name_clean
```

**Result:**
- `"john™"` → `"John"`
- `"marie-louise"` → `"Marie-Louise"`

#### 3. Phone Cleaning
```sql
REGEXP_REPLACE(phone, '[^0-9+]', '', 'g') AS phone_clean
```

**Result:**
- `"+33 6 12 34 56 78"` → `"+33612345678"`
- `"06-12-34-56-78"` → `"0612345678"`

#### 4. Date Parsing (Multi-format)
```sql
{{ parse_mixed_dates('created_at') }} AS created_at_parsed
```

**Macro `parse_mixed_dates`:**
```sql
{% macro parse_mixed_dates(column_name) %}
    CASE
        -- ISO format: 2023-01-15
        WHEN {{ column_name }} ~ '^\d{4}-\d{2}-\d{2}$'
            THEN {{ column_name }}::DATE
        
        -- European: 15/01/2023
        WHEN {{ column_name }} ~ '^\d{2}/\d{2}/\d{4}$'
            THEN TO_DATE({{ column_name }}, 'DD/MM/YYYY')
        
        -- US: 01-15-2023
        WHEN {{ column_name }} ~ '^\d{2}-\d{2}-\d{4}$'
            THEN TO_DATE({{ column_name }}, 'MM-DD-YYYY')
        
        ELSE NULL
    END
{% endmacro %}
```

**Result:**
- `"2023-01-15"` → `DATE '2023-01-15'`
- `"15/01/2023"` → `DATE '2023-01-15'`
- `"01-15-2023"` → `DATE '2023-01-15'`
- `"invalid"` → `NULL`

#### 5. Dual Column Pattern
For **auditability**, keep both raw and cleaned versions:
```sql
email_clean,        -- For business logic
email_raw,          -- For debugging
first_name_clean,
first_name_raw,
phone_clean,
phone_raw
```

**Why?**
- **email_clean:** Used in joins, deduplication
- **email_raw:** See original value if something looks wrong

---

**Full Column List (staging.stg_csv_customers):**
```
customer_id
email_clean
email_raw
first_name_clean
first_name_raw
last_name_clean
last_name_raw
phone_clean
phone_raw
address
city
country
created_at_raw
created_at_parsed
loaded_at
source_file
```

---

## 🏆 WAREHOUSE Layer Tables

### warehouse.dim_customers

**Purpose:** Deduplicated golden customer records  
**Source:** `intermediate.int_customer_deduped`  

**Deduplication Logic:**
```sql
-- Step 1: Create identity match key
MD5(email_clean) AS identity_match_key

-- Step 2: Rank duplicates
ROW_NUMBER() OVER (
    PARTITION BY identity_match_key
    ORDER BY created_at_parsed ASC, customer_id ASC
) AS row_num

-- Step 3: Keep only first occurrence
WHERE row_num = 1
```

**Result:**
- **Before:** 5,437 rows (raw)
- **After:** 4,501 unique customers (deduplicated)
- **Removed:** 436 duplicates (~8%)

**Business Rule:** 
- Same email = same customer
- Keep **oldest** account (first `created_at_parsed`)

---

### warehouse.customer_rfm

**Purpose:** RFM (Recency, Frequency, Monetary) segmentation  
**Source:** `warehouse.dim_customers` + order data  

**RFM Calculation:**

#### Recency (R)
```sql
-- Days since last order
CURRENT_DATE - MAX(order_date) AS recency_days

-- Score (1-5, higher = more recent)
CASE
    WHEN recency_days <= 30  THEN 5  -- Active
    WHEN recency_days <= 60  THEN 4
    WHEN recency_days <= 90  THEN 3
    WHEN recency_days <= 180 THEN 2
    ELSE 1                            -- Lost
END AS recency_score
```

#### Frequency (F)
```sql
-- Total number of orders
COUNT(DISTINCT order_id) AS frequency

-- Score (quintiles, 1-5)
NTILE(5) OVER (ORDER BY frequency) AS frequency_score
```

#### Monetary (M)
```sql
-- Total lifetime spend
SUM(order_amount) AS monetary

-- Score (quintiles, 1-5)
NTILE(5) OVER (ORDER BY monetary) AS monetary_score
```

**RFM Segments:**
```sql
CASE
    WHEN R≥4 AND F≥4 AND M≥4 THEN 'VIP'        -- ~450 customers (10%)
    WHEN R≥4 AND F≥3        THEN 'Champion'    -- ~800 customers
    WHEN R≥3 AND F≥3        THEN 'Loyal'       -- ~1,200 customers
    WHEN R≤2 AND F≥3        THEN 'At Risk'     -- ~200 customers (⚠️)
    WHEN R≤2 AND F≤2        THEN 'Lost'        -- ~1,851 customers
    ELSE 'Others'
END AS rfm_segment
```

**Business Insights:**
- **VIP (450):** Recent, frequent, high-value → Retain at all costs!
- **At Risk (200):** Were loyal but haven't ordered recently → Re-engagement campaign
- **Lost (1,851):** Inactive, low value → Win-back campaign or deprioritize

---

### warehouse.customer_health

**Purpose:** Overall customer health scoring (0-100)  

**Formula:**
```sql
Health Score = 
    (recency_score × 25) +      -- 25% weight
    (frequency_score × 25) +     -- 25% weight
    (monetary_score × 30) +      -- 30% weight (most important!)
    (email_valid × 20)           -- 20% weight (can we contact them?)

Range: 0-100
```

**Classification:**
```sql
CASE
    WHEN health_score >= 80 THEN 'Excellent'  -- ~450 customers
    WHEN health_score >= 60 THEN 'Good'       -- ~1,200 customers
    WHEN health_score >= 40 THEN 'Fair'       -- ~1,500 customers
    ELSE 'At Risk'                            -- ~1,351 customers
END AS health_status
```

**Why these weights?**
- **Monetary (30%):** Revenue matters most for business
- **Recency (25%):** Recent activity = engaged
- **Frequency (25%):** Loyalty indicator
- **Email Valid (20%):** Can't market to invalid emails

---

### warehouse.cohort_retention

**Purpose:** Monthly cohort retention analysis  
**Rows:** ~156 (13 months × 12 cohorts)  

**Metrics:**
- Customers acquired per month
- Retention rate by cohort month
- Revenue per cohort
- Cohort age

**Example:**
```
cohort_month | cohort_month_1 | retention_m1 | retention_m3 | retention_m6
-------------|----------------|--------------|--------------|-------------
2023-01      | 425            | 78%          | 45%          | 28%
2023-02      | 389            | 82%          | 51%          | 31%
```

---

### warehouse.pipeline_metadata

**Purpose:** Data observability - track all pipeline executions  

**DDL:**
```sql
CREATE TABLE warehouse.pipeline_metadata (
    run_id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    schema_name VARCHAR(50) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    rows_inserted INT,
    rows_updated INT,
    run_status VARCHAR(20),  -- 'success', 'failed', 'running'
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT
);

CREATE INDEX idx_pipeline_runs 
ON warehouse.pipeline_metadata(pipeline_name, started_at DESC);
```

**Use Cases:**
- "When did dbt last run successfully?"
- "Which pipelines failed today?"
- "How many rows were inserted in the last run?"
- Alerting if pipeline hasn't run in 24h

---

## 🔧 PostgreSQL-Specific Features (To Translate for BigQuery)

### 1. Type Casting (`::`)

**PostgreSQL:**
```sql
created_at::DATE
customer_id::TEXT
amount::NUMERIC
```

**BigQuery:**
```sql
CAST(created_at AS DATE)
CAST(customer_id AS STRING)
CAST(amount AS NUMERIC)
```

---

### 2. Regex Match Operator (`~`)

**PostgreSQL:**
```sql
WHERE email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$'
WHERE created_at ~ '^\d{4}-\d{2}-\d{2}$'
```

**BigQuery:**
```sql
WHERE REGEXP_CONTAINS(email, r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$')
WHERE REGEXP_CONTAINS(created_at, r'^\d{4}-\d{2}-\d{2}$')
```

**Note:** BigQuery uses `r'...'` for raw strings (like Python)

---

### 3. REGEXP_REPLACE

**PostgreSQL:**
```sql
REGEXP_REPLACE(phone, '[^0-9+]', '', 'g')  -- 'g' = global flag
```

**BigQuery:**
```sql
REGEXP_REPLACE(phone, r'[^0-9+]', '')  -- No 'g' flag (always global)
```

---

### 4. String Functions

**INITCAP (capitalize first letter):**

**PostgreSQL:**
```sql
INITCAP('john doe')  -- Returns: 'John Doe'
```

**BigQuery:** (no native INITCAP, must implement)
```sql
-- Option 1: Simple first letter
CONCAT(UPPER(SUBSTR(name, 1, 1)), LOWER(SUBSTR(name, 2)))

-- Option 2: Each word (complex, use UDF)
```

---

### 5. Date Arithmetic

**PostgreSQL:**
```sql
CURRENT_DATE - order_date  -- Returns integer (days)
```

**BigQuery:**
```sql
DATE_DIFF(CURRENT_DATE(), order_date, DAY)
```

---

### 6. SERIAL Type

**PostgreSQL:**
```sql
run_id SERIAL PRIMARY KEY  -- Auto-increment
```

**BigQuery:**
```sql
run_id INT64  -- No auto-increment; use GENERATE_UUID() or handle in app
```

---

## 📊 Data Type Mapping

| PostgreSQL Type | Example | BigQuery Type | Notes |
|----------------|---------|---------------|-------|
| `VARCHAR(n)` | `VARCHAR(100)` | `STRING` | No length limit in BigQuery |
| `TEXT` | `TEXT` | `STRING` | Same as VARCHAR |
| `INTEGER` | `INTEGER` | `INT64` | All integers → INT64 |
| `SERIAL` | `SERIAL PRIMARY KEY` | `INT64` | Lose auto-increment |
| `TIMESTAMP` | `TIMESTAMP` | `TIMESTAMP` | Compatible ✅ |
| `DATE` | `DATE` | `DATE` | Compatible ✅ |
| `NUMERIC(p,s)` | `NUMERIC(10,2)` | `NUMERIC` | Compatible ✅ |

---

## 🎯 Key Business Metrics (Must Preserve After Migration)

| Metric | Value | Layer | Verification Query |
|--------|-------|-------|-------------------|
| Total raw customers | 5,437 | raw | `SELECT COUNT(*) FROM raw.csv_customers` |
| Unique customers (deduplicated) | 4,501 | warehouse | `SELECT COUNT(*) FROM warehouse.dim_customers` |
| Duplicates removed | 436 | - | `5,437 - 4,501 = 936` |
| VIP customers | ~450 | warehouse | `SELECT COUNT(*) FROM warehouse.customer_rfm WHERE rfm_segment='VIP'` |
| At Risk customers | ~200 | warehouse | `WHERE rfm_segment='At Risk'` |
| Excellent health customers | ~450 | warehouse | `WHERE health_status='Excellent'` |

**❗ If these numbers don't match after migration → Migration failed!**

---

## 🔄 dbt Project Structure
```
dbt_project/
├── models/
│   ├── staging/
│   │   ├── stg_csv_customers.sql     (view)
│   │   └── sources.yml
│   ├── intermediate/
│   │   ├── int_customer_identity.sql (table)
│   │   └── int_customer_deduped.sql  (table)
│   └── marts/
│       ├── dim_customers.sql         (table)
│       ├── customer_rfm.sql          (table)
│       ├── customer_health.sql       (table)
│       └── cohort_retention.sql      (table)
├── macros/
│   └── parse_mixed_dates.sql
└── dbt_project.yml
```

**dbt Lineage:**
```
raw.csv_customers
    ↓
staging.stg_csv_customers (view)
    ↓
intermediate.int_customer_identity (table)
    ↓
intermediate.int_customer_deduped (table)
    ↓
warehouse.dim_customers (table)
    ↓
├─→ warehouse.customer_rfm (table)
├─→ warehouse.customer_health (table)
└─→ warehouse.cohort_retention (table)
```

---

## 🚀 Migration Strategy

### Phase 1: Raw Layer (Days 6-11)
1. Extract CSV from PostgreSQL raw tables
2. Load to BigQuery `raw_data` dataset
3. Reconcile row counts (must match exactly)

### Phase 2: Staging Layer (Days 12-14)
1. Translate `stg_csv_customers.sql` to BigQuery syntax
2. Rewrite `parse_mixed_dates` macro for BigQuery
3. Test email cleaning, phone parsing

### Phase 3: Warehouse Layer (Days 15-17)
1. Migrate identity resolution & deduplication
2. Translate RFM segmentation logic
3. Translate health scoring
4. Migrate cohort retention

### Phase 4: Validation (Day 18-20)
1. Compare metrics: 4,501 customers, 450 VIPs
2. Performance testing (clustering, partitioning)
3. Export for Power BI
4. Final documentation

---

## 📚 References

- **Source project:** `~/customer360-prod`
- **SQL DDL:** `sql/init_schemas.sql`, `sql/create_raw_tables.sql`
- **dbt models:** `dbt_project/models/`
- **Macros:** `dbt_project/macros/parse_mixed_dates.sql`

---

**Documentation Date:** $(date +%Y-%m-%d)  
**PostgreSQL Version:** 14/15  
**Status:** Ready for BigQuery migration

