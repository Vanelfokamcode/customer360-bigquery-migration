{{ config(
    materialized='table',
    tags=['marts', 'monitoring']
) }}

/*
MART: Data Quality Monitoring
- Track data quality over time
- Identify trends and issues
- Alert-ready metrics
*/

WITH staging_quality AS (
    SELECT
        'staging' AS layer,
        COUNT(*) AS total_records,
        
        -- Email quality
        COUNTIF(email_clean IS NOT NULL) AS records_with_email,
        COUNTIF(is_valid_email = true) AS valid_emails,
        COUNTIF(is_valid_email = false) AS invalid_emails,
        
        -- Date quality
        COUNTIF(created_at_parsed IS NOT NULL) AS parsed_dates,
        COUNTIF(created_at_parsed IS NULL) AS unparsed_dates,
        
        -- Name quality
        COUNTIF(first_name_clean IS NOT NULL) AS has_first_name,
        COUNTIF(last_name_clean IS NOT NULL) AS has_last_name,
        
        -- Phone quality
        COUNTIF(phone_clean IS NOT NULL) AS has_phone,
        COUNTIF(LENGTH(phone_clean) >= 10) AS valid_phone_length
        
    FROM {{ ref('stg_csv_customers') }}
),

intermediate_quality AS (
    SELECT
        'intermediate' AS layer,
        COUNT(*) AS total_records,
        
        -- Deduplication metrics
        COUNT(DISTINCT identity_match_key) AS unique_identities,
        
        -- Should always be 1 after dedup
        AVG(customer_rank) AS avg_customer_rank,
        MAX(customer_rank) AS max_customer_rank
        
    FROM {{ ref('int_customers_deduplicated') }}
),

marts_quality AS (
    SELECT
        'marts' AS layer,
        COUNT(*) AS total_records,
        
        -- Quality tiers
        COUNTIF(data_quality_tier = 'Complete') AS tier_complete,
        COUNTIF(data_quality_tier = 'Partial') AS tier_partial,
        COUNTIF(data_quality_tier = 'Incomplete') AS tier_incomplete,
        
        -- Lifecycle distribution
        COUNTIF(customer_lifecycle_stage = 'New') AS lifecycle_new,
        COUNTIF(customer_lifecycle_stage = 'Recent') AS lifecycle_recent,
        COUNTIF(customer_lifecycle_stage = 'Established') AS lifecycle_established
        
    FROM {{ ref('dim_customers') }}
),

combined AS (
    SELECT
        layer,
        total_records,
        records_with_email,
        valid_emails,
        invalid_emails,
        parsed_dates,
        unparsed_dates,
        has_first_name,
        has_last_name,
        has_phone,
        valid_phone_length,
        NULL AS unique_identities,
        NULL AS avg_customer_rank,
        NULL AS tier_complete,
        NULL AS tier_partial,
        NULL AS tier_incomplete,
        NULL AS lifecycle_new,
        NULL AS lifecycle_recent,
        NULL AS lifecycle_established
    FROM staging_quality
    
    UNION ALL
    
    SELECT
        layer,
        total_records,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
        unique_identities,
        avg_customer_rank,
        NULL, NULL, NULL, NULL, NULL, NULL
    FROM intermediate_quality
    
    UNION ALL
    
    SELECT
        layer,
        total_records,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
        tier_complete,
        tier_partial,
        tier_incomplete,
        lifecycle_new,
        lifecycle_recent,
        lifecycle_established
    FROM marts_quality
)

SELECT
    layer,
    total_records,
    
    -- Staging metrics
    ROUND(100.0 * valid_emails / NULLIF(records_with_email, 0), 2) AS pct_valid_emails,
    ROUND(100.0 * parsed_dates / NULLIF(total_records, 0), 2) AS pct_parsed_dates,
    ROUND(100.0 * has_phone / NULLIF(total_records, 0), 2) AS pct_has_phone,
    
    -- Intermediate metrics
    unique_identities,
    CASE WHEN avg_customer_rank = 1 THEN '✅ OK' ELSE '⚠️ CHECK' END AS dedup_status,
    
    -- Marts metrics
    tier_complete,
    tier_partial,
    tier_incomplete,
    ROUND(100.0 * tier_complete / NULLIF(total_records, 0), 2) AS pct_complete,
    
    lifecycle_new,
    lifecycle_recent,
    lifecycle_established,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS report_generated_at

FROM combined
ORDER BY 
    CASE layer
        WHEN 'staging' THEN 1
        WHEN 'intermediate' THEN 2
        WHEN 'marts' THEN 3
    END
