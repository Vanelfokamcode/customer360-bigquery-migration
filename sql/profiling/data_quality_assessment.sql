-- =========================================
-- DATA QUALITY ASSESSMENT
-- Customer 360 PostgreSQL Database
-- =========================================

\echo '================================================'
\echo 'DATA QUALITY ASSESSMENT - Customer 360'
\echo 'Database: customer360'
\echo 'Date: ' `date`
\echo '================================================'
\echo ''

-- =========================================
-- 1. ROW COUNTS (Volume Assessment)
-- =========================================

\echo '1. ROW COUNTS BY TABLE'
\echo '----------------------'

SELECT 
    'raw.csv_customers' as table_name,
    COUNT(*) as total_rows,
    pg_size_pretty(pg_total_relation_size('raw.csv_customers')) as table_size
FROM raw.csv_customers

UNION ALL

SELECT 
    'staging.stg_csv_customers',
    COUNT(*),
    pg_size_pretty(pg_total_relation_size('staging.stg_csv_customers'))
FROM staging.stg_csv_customers

UNION ALL

SELECT 
    'warehouse.dim_customers',
    COUNT(*),
    pg_size_pretty(pg_total_relation_size('warehouse.dim_customers'))
FROM warehouse.dim_customers

ORDER BY table_name;

\echo ''

-- =========================================
-- 2. COMPLETENESS (NULL Analysis)
-- =========================================

\echo '2. COMPLETENESS - Missing Values in RAW Layer'
\echo '----------------------------------------------'

SELECT
    'raw.csv_customers' as table_name,
    COUNT(*) as total_rows,
    COUNT(*) - COUNT(customer_id) as missing_customer_id,
    COUNT(*) - COUNT(email) as missing_email,
    COUNT(*) - COUNT(first_name) as missing_first_name,
    COUNT(*) - COUNT(last_name) as missing_last_name,
    COUNT(*) - COUNT(phone) as missing_phone,
    COUNT(*) - COUNT(created_at) as missing_created_at,
    COUNT(*) - COUNT(country) as missing_country
FROM raw.csv_customers;

\echo ''
\echo 'Percentage of Missing Values:'
\echo '------------------------------'

SELECT
    'email' as column_name,
    ROUND(100.0 * (COUNT(*) - COUNT(email)) / COUNT(*), 2) as pct_missing
FROM raw.csv_customers

UNION ALL

SELECT 'phone', ROUND(100.0 * (COUNT(*) - COUNT(phone)) / COUNT(*), 2)
FROM raw.csv_customers

UNION ALL

SELECT 'first_name', ROUND(100.0 * (COUNT(*) - COUNT(first_name)) / COUNT(*), 2)
FROM raw.csv_customers

UNION ALL

SELECT 'last_name', ROUND(100.0 * (COUNT(*) - COUNT(last_name)) / COUNT(*), 2)
FROM raw.csv_customers

ORDER BY pct_missing DESC;

\echo ''

-- =========================================
-- 3. VALIDITY (Format Validation)
-- =========================================

\echo '3. VALIDITY - Email Format'
\echo '--------------------------'

WITH email_validation AS (
    SELECT
        email,
        CASE 
            WHEN email IS NULL THEN 'NULL'
            WHEN TRIM(email) = '' THEN 'EMPTY'
            WHEN email !~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$' THEN 'INVALID_FORMAT'
            ELSE 'VALID'
        END as validation_status
    FROM raw.csv_customers
)
SELECT
    validation_status,
    COUNT(*) as count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM raw.csv_customers), 2) as percentage
FROM email_validation
GROUP BY validation_status
ORDER BY count DESC;

\echo ''
\echo 'Sample of Invalid Emails:'
\echo '-------------------------'

SELECT email, customer_id
FROM raw.csv_customers
WHERE email IS NOT NULL 
  AND email !~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$'
LIMIT 10;

\echo ''

-- =========================================
-- 4. VALIDITY - Date Format
-- =========================================

\echo '4. VALIDITY - Date Format Patterns'
\echo '-----------------------------------'

WITH date_patterns AS (
    SELECT
        created_at,
        CASE
            WHEN created_at ~ '^\d{4}-\d{2}-\d{2}' THEN 'ISO (YYYY-MM-DD)'
            WHEN created_at ~ '^\d{2}/\d{2}/\d{4}' THEN 'European (DD/MM/YYYY)'
            WHEN created_at ~ '^\d{2}-\d{2}-\d{4}' THEN 'US (MM-DD-YYYY)'
            WHEN created_at IS NULL THEN 'NULL'
            ELSE 'UNKNOWN'
        END as date_format
    FROM raw.csv_customers
)
SELECT
    date_format,
    COUNT(*) as count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM raw.csv_customers), 2) as percentage
FROM date_patterns
GROUP BY date_format
ORDER BY count DESC;

\echo ''
\echo 'Sample of Unknown Date Formats:'
\echo '--------------------------------'

SELECT created_at, customer_id
FROM raw.csv_customers
WHERE created_at IS NOT NULL
  AND created_at !~ '^\d{4}-\d{2}-\d{2}'
  AND created_at !~ '^\d{2}/\d{2}/\d{4}'
  AND created_at !~ '^\d{2}-\d{2}-\d{4}'
LIMIT 10;

\echo ''

-- =========================================
-- 5. UNIQUENESS (Duplicate Detection)
-- =========================================

\echo '5. UNIQUENESS - Duplicate Analysis'
\echo '----------------------------------'

\echo 'Duplicate Emails in RAW Layer:'

SELECT
    COUNT(*) as total_rows,
    COUNT(DISTINCT email) as unique_emails,
    COUNT(*) - COUNT(DISTINCT email) as duplicate_count,
    ROUND(100.0 * (COUNT(*) - COUNT(DISTINCT email)) / COUNT(*), 2) as pct_duplicates
FROM raw.csv_customers
WHERE email IS NOT NULL;

\echo ''
\echo 'Top 10 Most Duplicated Emails:'
\echo '-------------------------------'

SELECT
    LOWER(TRIM(email)) as email_normalized,
    COUNT(*) as occurrence_count
FROM raw.csv_customers
WHERE email IS NOT NULL
GROUP BY LOWER(TRIM(email))
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC
LIMIT 10;

\echo ''

-- =========================================
-- 6. DEDUPLICATION IMPACT
-- =========================================

\echo '6. DEDUPLICATION IMPACT'
\echo '-----------------------'

SELECT
    'Before Deduplication (raw)' as stage,
    COUNT(*) as customer_count
FROM raw.csv_customers

UNION ALL

SELECT
    'After Deduplication (warehouse)',
    COUNT(*)
FROM warehouse.dim_customers

UNION ALL

SELECT
    'Duplicates Removed',
    (SELECT COUNT(*) FROM raw.csv_customers) - 
    (SELECT COUNT(*) FROM warehouse.dim_customers);

\echo ''

-- =========================================
-- 7. CONSISTENCY (Cross-Column Checks)
-- =========================================

\echo '7. CONSISTENCY - Phone Format'
\echo '------------------------------'

WITH phone_patterns AS (
    SELECT
        phone,
        CASE
            WHEN phone IS NULL THEN 'NULL'
            WHEN phone ~ '^\+\d{10,15}$' THEN 'INTERNATIONAL_VALID'
            WHEN phone ~ '^0\d{9}$' THEN 'LOCAL_VALID'
            WHEN phone ~ '\d' THEN 'CONTAINS_DIGITS'
            ELSE 'INVALID'
        END as phone_status
    FROM raw.csv_customers
)
SELECT
    phone_status,
    COUNT(*) as count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM raw.csv_customers), 2) as percentage
FROM phone_patterns
GROUP BY phone_status
ORDER BY count DESC;

\echo ''

-- =========================================
-- 8. COUNTRY DISTRIBUTION
-- =========================================

\echo '8. COUNTRY DISTRIBUTION'
\echo '-----------------------'

SELECT
    COALESCE(country, 'NULL') as country,
    COUNT(*) as customer_count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM raw.csv_customers), 2) as percentage
FROM raw.csv_customers
GROUP BY country
ORDER BY customer_count DESC
LIMIT 10;

\echo ''

-- =========================================
-- 9. DATA FRESHNESS
-- =========================================

\echo '9. DATA FRESHNESS'
\echo '-----------------'

SELECT
    'Oldest Record' as metric,
    MIN(loaded_at) as timestamp
FROM raw.csv_customers

UNION ALL

SELECT
    'Newest Record',
    MAX(loaded_at)
FROM raw.csv_customers

UNION ALL

SELECT
    'Data Age (days)',
    EXTRACT(DAY FROM (CURRENT_TIMESTAMP - MIN(loaded_at)))::TEXT::TIMESTAMP
FROM raw.csv_customers;

\echo ''

-- =========================================
-- 10. SUMMARY SCORECARD
-- =========================================

\echo '10. DATA QUALITY SCORECARD'
\echo '--------------------------'

WITH quality_metrics AS (
    SELECT
        -- Completeness (email is mandatory)
        ROUND(100.0 * COUNT(email) / COUNT(*), 2) as completeness_score,
        
        -- Validity (email format)
        ROUND(100.0 * COUNT(CASE 
            WHEN email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$' 
            THEN 1 END) / COUNT(*), 2) as validity_score,
        
        -- Uniqueness (deduplicated in warehouse)
        ROUND(100.0 * (SELECT COUNT(*) FROM warehouse.dim_customers) / COUNT(*), 2) as uniqueness_score,
        
        -- Overall count
        COUNT(*) as total_records
    FROM raw.csv_customers
)
SELECT
    'Completeness (email)' as dimension,
    completeness_score as score,
    CASE 
        WHEN completeness_score >= 95 THEN '✅ Excellent'
        WHEN completeness_score >= 80 THEN '⚠️  Good'
        WHEN completeness_score >= 60 THEN '⚠️  Fair'
        ELSE '❌ Poor'
    END as grade
FROM quality_metrics

UNION ALL

SELECT
    'Validity (email format)',
    validity_score,
    CASE 
        WHEN validity_score >= 95 THEN '✅ Excellent'
        WHEN validity_score >= 80 THEN '⚠️  Good'
        WHEN validity_score >= 60 THEN '⚠️  Fair'
        ELSE '❌ Poor'
    END
FROM quality_metrics

UNION ALL

SELECT
    'Uniqueness (deduplication)',
    uniqueness_score,
    CASE 
        WHEN uniqueness_score >= 95 THEN '✅ Excellent'
        WHEN uniqueness_score >= 80 THEN '⚠️  Good'
        WHEN uniqueness_score >= 60 THEN '⚠️  Fair'
        ELSE '❌ Poor'
    END
FROM quality_metrics;

\echo ''
\echo '================================================'
\echo 'DATA QUALITY ASSESSMENT COMPLETE'
\echo '================================================'
