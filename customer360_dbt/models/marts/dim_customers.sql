{{ config(
    materialized='table',
    tags=['marts', 'dimensions'],
    cluster_by=['country', 'data_quality_tier', 'customer_key']
) }}

/*
MART: Customer Dimension Table (OPTIMIZED)
- Clustered by: country, data_quality_tier, customer_key
- Optimizes queries filtering by country or quality tier
- One row = one unique customer
*/

SELECT
    -- Primary Key
    identity_match_key AS customer_key,
    customer_id AS source_customer_id,
    
    -- Demographics
    full_name,
    first_name_clean AS first_name,
    last_name_clean AS last_name,
    
    -- Contact Information
    email_clean AS email,
    is_valid_email,
    phone_clean AS phone,
    
    -- Location (CLUSTERED)
    country,
    city,
    address,
    
    -- Temporal
    account_created_at,
    DATE_DIFF(CURRENT_DATE(), account_created_at, DAY) AS account_age_days,
    EXTRACT(YEAR FROM account_created_at) AS account_created_year,
    EXTRACT(MONTH FROM account_created_at) AS account_created_month,
    
    -- Data Quality Flags (CLUSTERED)
    CASE 
        WHEN is_valid_email = true 
         AND phone_clean IS NOT NULL 
         AND account_created_at IS NOT NULL
        THEN 'Complete'
        WHEN is_valid_email = true
        THEN 'Partial'
        ELSE 'Incomplete'
    END AS data_quality_tier,
    
    -- Segmentation helpers
    CASE
        WHEN account_created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) 
        THEN 'New'
        WHEN account_created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
        THEN 'Recent'
        ELSE 'Established'
    END AS customer_lifecycle_stage,
    
    -- Metadata
    loaded_at,
    CURRENT_TIMESTAMP() AS dbt_updated_at

FROM {{ ref('int_customers_deduplicated') }}
