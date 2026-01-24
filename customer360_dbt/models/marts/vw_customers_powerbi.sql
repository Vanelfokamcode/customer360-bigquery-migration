{{ config(
    materialized='view',
    tags=['marts', 'bi-export']
) }}

/*
VIEW: Power BI Export - Customers
- Simplified view for BI tools
- Only essential columns
- No complex nested structures
- Optimized for dashboard performance
*/

SELECT
    -- IDs
    customer_key,
    source_customer_id,
    
    -- Customer Info
    full_name,
    email,
    phone,
    
    -- Location
    country,
    city,
    
    -- Dates
    account_created_at,
    account_age_days,
    account_created_year,
    account_created_month,
    
    -- Segments
    data_quality_tier,
    customer_lifecycle_stage,
    
    -- Flags (for filtering)
    is_valid_email AS has_valid_email,
    CASE WHEN phone IS NOT NULL THEN TRUE ELSE FALSE END AS has_phone,
    CASE WHEN data_quality_tier = 'Complete' THEN TRUE ELSE FALSE END AS is_complete_profile,
    
    -- Metadata
    dbt_updated_at AS last_updated

FROM {{ ref('dim_customers') }}
