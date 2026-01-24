{{ config(
    materialized='view',
    tags=['marts', 'bi-export']
) }}

/*
VIEW: Power BI Export - Customers (MINIMAL VERSION)
- Only columns that exist in dim_customers
*/

SELECT
    -- IDs
    customer_key,
    source_customer_id,
    
    -- Customer Info
    full_name,
    first_name,
    last_name,
    email,
    phone,
    
    -- Location
    country,
    city,
    address,
    
    -- Dates
    account_created_at,
    account_age_days,
    EXTRACT(YEAR FROM account_created_at) AS account_created_year,
    EXTRACT(MONTH FROM account_created_at) AS account_created_month,
    
    -- Data Quality
    data_quality_tier,
    
    -- Flags
    is_valid_email AS has_valid_email,
    CASE WHEN phone IS NOT NULL THEN TRUE ELSE FALSE END AS has_phone,
    CASE WHEN data_quality_tier = 'Complete' THEN TRUE ELSE FALSE END AS is_complete_profile,
    
    -- Lifecycle (calculate here since it doesn't exist in dim_customers)
    CASE
        WHEN account_created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) 
        THEN 'New'
        WHEN account_created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
        THEN 'Recent'
        ELSE 'Established'
    END AS customer_lifecycle_stage,
    
    -- Metadata
    dbt_updated_at AS last_updated

FROM {{ ref('dim_customers') }}
