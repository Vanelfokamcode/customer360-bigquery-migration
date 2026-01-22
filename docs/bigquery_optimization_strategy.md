# BigQuery Optimization Strategy

## Overview

BigQuery offers two main optimization techniques:
1. **Clustering:** Sort data physically by column(s)
2. **Partitioning:** Divide table into segments (usually by date)

**Goal:** Reduce query time + Reduce cost (scan less data)

---

## Clustering Strategy

### What is Clustering?

Clustering **physically sorts** data by specified columns. Queries filtering on clustered columns scan **less data** → faster + cheaper.

**Limits:**
- Up to **4 columns** per table
- Order matters! First column = primary sort

### Clustering Decisions

#### Table: `analytics.dim_customers`
```sql
CLUSTER BY customer_key
```

**Why?**
- **Primary access pattern:** Lookup by customer_key
- **Example queries:**
```sql
  -- Dashboard: Show customer details
  WHERE customer_key = 'abc123'
  
  -- Join with orders
  JOIN orders ON customers.customer_key = orders.customer_key
```
- **Expected improvement:** 50-80% less data scanned

---

#### Table: `analytics.customer_rfm`
```sql
CLUSTER BY rfm_segment, customer_key
```

**Why multi-column?**
- **Primary pattern:** Filter by segment, then lookup customer
- **Example queries:**
```sql
  -- Marketing: Get all VIP customers
  WHERE rfm_segment = 'VIP'
  
  -- Sales: At Risk customers for re-engagement
  WHERE rfm_segment = 'At Risk'
  
  -- Specific customer in segment
  WHERE rfm_segment = 'VIP' AND customer_key = 'abc123'
```
- **Column order:** `rfm_segment` first because:
  - Only 6 segment values ('VIP', 'Champion', etc.)
  - High cardinality filter first = better clustering
- **Expected improvement:** 70-90% less data scanned

---

#### Table: `analytics.customer_health`
```sql
CLUSTER BY health_status, customer_key
```

**Why?**
- **Primary pattern:** Filter by health status
- **Example queries:**
```sql
  -- Alert: Customers at risk
  WHERE health_status = 'At Risk'
  
  -- Report: Excellent customers
  WHERE health_status = 'Excellent'
```
- **Expected improvement:** 60-80% less data scanned

---

## Partitioning Strategy

### What is Partitioning?

Partitioning **divides table into segments** (partitions). Queries with partition filter **only scan relevant partitions** → massive savings.

**Best for:** Time-series data (logs, events, cohorts)

**Types:**
- **Time-unit partitioning:** By day, month, year
- **Integer range:** By ranges (0-100, 101-200, etc.)
- **Ingestion time:** Automatic `_PARTITIONTIME` column

### Partitioning Decisions

#### Table: `analytics.cohort_retention`
```sql
PARTITION BY cohort_month
CLUSTER BY cohort_month
```

**Why partition + cluster?**
- **Data nature:** Time-series (cohorts by month)
- **Access pattern:** Queries usually filter by date range
- **Example queries:**
```sql
  -- Last 6 months retention
  WHERE cohort_month >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
  
  -- Specific year
  WHERE cohort_month BETWEEN '2023-01-01' AND '2023-12-31'
```
- **Benefit:**
  - Partition: Only scan 6 months of data (not all 156 cohorts)
  - Cluster: Within each partition, data sorted by month
- **Expected improvement:** 90-95% less data scanned
- **Partition expiration:** `null` (keep all data, small volume)

---

#### Table: `governance.pipeline_metadata`
```sql
PARTITION BY DATE(started_at)
CLUSTER BY pipeline_name, run_status
OPTIONS(
    partition_expiration_days=90  -- Auto-delete after 3 months
)
```

**Why partition + cluster?**
- **Data nature:** Logs (time-series)
- **Access pattern:** Recent logs + filter by pipeline/status
- **Example queries:**
```sql
  -- Today's failed runs (alerting)
  WHERE DATE(started_at) = CURRENT_DATE()
    AND run_status = 'failed'
  
  -- Last week's dbt runs
  WHERE DATE(started_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    AND pipeline_name LIKE 'dbt%'
```
- **Partition expiration:** Old logs auto-deleted → save storage costs
- **Expected improvement:** 95-99% less data scanned

---

## Performance Benchmarks (Expected)

| Table | Size | Query Type | Without Optimization | With Optimization | Improvement |
|-------|------|-----------|---------------------|------------------|-------------|
| `dim_customers` | 4,501 rows | Lookup | 100% scan | 20% scan | **80% reduction** |
| `customer_rfm` | 4,501 rows | Filter by segment | 100% scan | 10% scan | **90% reduction** |
| `customer_health` | 4,501 rows | Filter by status | 100% scan | 15% scan | **85% reduction** |
| `cohort_retention` | 156 rows | Date range | 100% scan | 5% scan | **95% reduction** |
| `pipeline_metadata` | 10,000+ rows | Recent logs | 100% scan | 1% scan | **99% reduction** |

**Note:** Current data volume is small (~5,000 rows). Benefits multiply as data grows!

---

## Clustering Best Practices

### ✅ Good Candidates for Clustering

1. **High cardinality columns** (many unique values)
   - ✅ `customer_key` (4,501 unique values)
   - ✅ `email` (4,501 unique values)

2. **Columns in WHERE clauses**
   - ✅ `rfm_segment` (used in filters)
   - ✅ `health_status` (used in dashboards)

3. **Columns in JOIN conditions**
   - ✅ `customer_key` (FK in joins)

### ❌ Poor Candidates for Clustering

1. **Low cardinality columns** (few unique values)
   - ❌ `country` (only ~10 countries)
   - ❌ `is_valid_email` (only TRUE/FALSE)

2. **Columns rarely used in queries**
   - ❌ `source_file`
   - ❌ `loaded_at` (except for debugging)

---

## Partitioning Best Practices

### ✅ Good Candidates for Partitioning

1. **Time-series data**
   - ✅ `cohort_month` (monthly cohorts)
   - ✅ `started_at` (pipeline logs)
   - ✅ `order_date` (transactions)

2. **Tables with date range queries**
   - ✅ "Show me last 30 days"
   - ✅ "Year-over-year comparison"

3. **Large tables (>1GB)** with old data rarely accessed
   - ✅ Pipeline logs (recent = hot, old = cold)

### ❌ Poor Candidates for Partitioning

1. **Small tables (<1GB)**
   - ❌ `dim_customers` (only 4,501 rows)
   - Clustering alone is sufficient

2. **Tables without date columns**
   - ❌ `dim_products` (no time dimension)

3. **Tables queried with full scans**
   - ❌ "Count all customers" (needs all partitions anyway)

---

## Cost Impact

### Clustering
- **Setup cost:** $0 (automatic, no extra storage)
- **Query cost:** Reduced (scan less data)
- **Example:** Query scans 100MB instead of 1GB → **10x cheaper**

### Partitioning
- **Setup cost:** $0 (metadata only)
- **Query cost:** Reduced (scan fewer partitions)
- **Storage cost:** **Slightly higher** (partition metadata)
- **Example:** Query scans 1 partition (10MB) instead of 100 partitions (1GB) → **100x cheaper**

### Current Project (Free Tier)
- Data: ~2MB total
- Queries: ~10MB/month
- **Cost: $0** (well within 1TB/month free tier)
- Optimizations = **future-proofing** for when data grows

---

## Implementation Timeline

### Week 2 (Day 9): Create Tables with Clustering
```sql
CREATE TABLE analytics.dim_customers (...)
CLUSTER BY customer_key;
```

### Week 3 (Day 16): Add Partitioning to Cohorts
```sql
CREATE TABLE analytics.cohort_retention (...)
PARTITION BY cohort_month
CLUSTER BY cohort_month;
```

### Week 4 (Day 18): Benchmark Performance
- Run queries WITH and WITHOUT optimizations
- Measure:
  - Bytes scanned (BigQuery UI)
  - Execution time
  - Cost (if any)
- Document improvements

---

## Monitoring Optimizations

### Check if Clustering is Working
```sql
-- BigQuery automatically shows "Bytes scanned"
SELECT customer_key, email
FROM `analytics.dim_customers`
WHERE customer_key = 'abc123';

-- Check clustering info
SELECT 
  table_name,
  clustering_ordinal_position,
  clustering_field
FROM `customer360-migration.analytics.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'dim_customers'
  AND clustering_ordinal_position IS NOT NULL;
```

### Check if Partitioning is Working
```sql
-- See partition info
SELECT 
  table_name,
  partition_id,
  total_rows
FROM `customer360-migration.analytics.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'cohort_retention';

-- Query should show "partitions scanned: 1" (not all)
WHERE cohort_month = '2023-01-01';
```

---

## Summary: Optimization Checklist

- [x] `dim_customers`: CLUSTER BY customer_key
- [x] `customer_rfm`: CLUSTER BY rfm_segment, customer_key
- [x] `customer_health`: CLUSTER BY health_status, customer_key
- [x] `cohort_retention`: PARTITION + CLUSTER BY cohort_month
- [x] `pipeline_metadata`: PARTITION BY DATE(started_at), CLUSTER BY pipeline_name
- [x] `raw_data.*`: No optimization (sequential access)
- [x] `staging.*`: No optimization (views)

**Expected overall improvement:** 70-90% less data scanned → Faster queries + Lower costs

