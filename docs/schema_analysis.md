# BigQuery Schema Analysis

## Auto-detected Schema (Day 8)

Table: `raw_data.csv_customers`  
Rows: 5,000  
Detection: Automatic from CSV

### Current Schema

| Column | Auto-detected Type | Issues | Correct Type |
|--------|-------------------|--------|--------------|
| `customer_id` | STRING | ✅ OK | STRING |
| `first_name` | STRING | ✅ OK | STRING |
| `last_name` | STRING | ✅ OK | STRING |
| `email` | STRING | ✅ OK | STRING |
| `phone` | STRING | ✅ OK | STRING |
| `address` | STRING | ✅ OK | STRING |
| `city` | STRING | ✅ OK | STRING |
| `country` | STRING | ✅ OK | STRING |
| `created_at` | STRING | ❌ Should be DATE | DATE |
| `loaded_at` | TIMESTAMP | ✅ OK | TIMESTAMP |
| `source_file` | STRING | ✅ OK | STRING |

---

## Issues to Fix

### 1. created_at is STRING
**Problem:** Date stored as text → Can't use DATE functions

**Current:**
```sql
SELECT created_at FROM raw_data.csv_customers LIMIT 1;
-- Result: "2023-01-15" (STRING)
```

**After fix:**
```sql
SELECT created_at FROM raw_data.csv_customers LIMIT 1;
-- Result: 2023-01-15 (DATE)

-- Can now use date functions:
SELECT 
    created_at,
    EXTRACT(YEAR FROM created_at) as year,
    DATE_DIFF(CURRENT_DATE(), created_at, DAY) as days_ago
FROM raw_data.csv_customers;
```

---

### 2. No Clustering
**Problem:** Queries scanning full table → Slow + Expensive

**Current query cost:**
```sql
SELECT * FROM raw_data.csv_customers WHERE customer_id = 'CUST_123';
-- Scans: 0.85 MB (100% of table)
```

**After clustering by customer_id:**
```sql
-- Scans: ~0.01 MB (1% of table) → 100x cheaper!
```

---

### 3. No NOT NULL Constraints
**Problem:** No data quality enforcement

**Current:** All columns allow NULL

**After fix:**
```sql
customer_id STRING NOT NULL  -- Enforce non-null
```

---

## Recommended Schema
```sql
CREATE TABLE `raw_data.csv_customers` (
    -- Identifiers
    customer_id STRING NOT NULL,
    
    -- Personal info
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    
    -- Address
    address STRING,
    city STRING,
    country STRING,
    
    -- Timestamps
    created_at DATE,           -- ← FIXED: was STRING
    loaded_at TIMESTAMP,
    
    -- Metadata
    source_file STRING
)
CLUSTER BY customer_id         -- ← NEW: Performance optimization
OPTIONS(
    description="Raw customer data from PostgreSQL - immutable landing zone"
);
```

---

## Benefits of Manual Schema

1. **Correct types** → Can use date/numeric functions
2. **Clustering** → 10-100x faster queries
3. **NOT NULL** → Data quality enforcement
4. **Documentation** → Clear table purpose
5. **Future-proof** → Ready for millions of rows

---

## Migration Plan

1. ✅ Analyze auto-detected schema (this document)
2. ⏭️ Create new table with correct schema
3. ⏭️ Load data into new table
4. ⏭️ Validate row counts match (5,000)
5. ⏭️ Drop old auto-detected table
6. ⏭️ Repeat for other tables

