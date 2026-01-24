{{ config(
    materialized='table',
    tags=['marts', 'metrics']
) }}

/*
MART: Customer Metrics Summary
- Aggregate metrics for dashboards
- Country-level and quality-level breakdowns
- Updated daily by dbt
*/

WITH base_metrics AS (
    SELECT
        -- Overall metrics
        COUNT(*) AS total_customers,
        COUNTIF(is_valid_email) AS customers_with_valid_email,
        COUNTIF(phone IS NOT NULL) AS customers_with_phone,
        COUNTIF(data_quality_tier = 'Complete') AS customers_complete_profile,
        
        -- By country
        country,
        
        -- By quality tier
        data_quality_tier,
        
        -- By lifecycle stage
        customer_lifecycle_stage,
        
        -- Temporal
        account_created_year,
        account_created_month
        
    FROM {{ ref('dim_customers') }}
    GROUP BY 
        country, 
        data_quality_tier, 
        customer_lifecycle_stage,
        account_created_year,
        account_created_month
)

SELECT
    -- Dimensions
    country,
    data_quality_tier,
    customer_lifecycle_stage,
    account_created_year,
    account_created_month,
    
    -- Metrics
    total_customers,
    customers_with_valid_email,
    customers_with_phone,
    customers_complete_profile,
    
    -- Percentages
    ROUND(100.0 * customers_with_valid_email / NULLIF(total_customers, 0), 2) AS pct_valid_email,
    ROUND(100.0 * customers_with_phone / NULLIF(total_customers, 0), 2) AS pct_with_phone,
    ROUND(100.0 * customers_complete_profile / NULLIF(total_customers, 0), 2) AS pct_complete_profile,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS calculated_at

FROM base_metrics
