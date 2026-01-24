{{ config(
    materialized='view',
    tags=['marts', 'bi-export']
) }}

/*
VIEW: Power BI Export - Metrics (MINIMAL VERSION)
- Pre-aggregated metrics
- Calculated directly from dim_customers
*/

SELECT
    -- Dimensions
    country,
    data_quality_tier,
    
    -- Lifecycle calculated here
    CASE
        WHEN account_created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) 
        THEN 'New'
        WHEN account_created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
        THEN 'Recent'
        ELSE 'Established'
    END AS customer_lifecycle_stage,
    
    -- Date dimensions
    EXTRACT(YEAR FROM account_created_at) AS account_created_year,
    EXTRACT(MONTH FROM account_created_at) AS account_created_month,
    
    -- Metrics
    COUNT(*) AS total_customers,
    COUNTIF(is_valid_email = TRUE) AS customers_with_valid_email,
    COUNTIF(phone IS NOT NULL) AS customers_with_phone,
    COUNTIF(data_quality_tier = 'Complete') AS customers_complete_profile,
    
    -- Percentages
    ROUND(100.0 * COUNTIF(is_valid_email = TRUE) / COUNT(*), 2) AS pct_valid_email,
    ROUND(100.0 * COUNTIF(phone IS NOT NULL) / COUNT(*), 2) AS pct_with_phone,
    ROUND(100.0 * COUNTIF(data_quality_tier = 'Complete') / COUNT(*), 2) AS pct_complete_profile,
    
    -- Average metrics
    ROUND(AVG(account_age_days), 0) AS avg_account_age_days,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS calculated_at

FROM {{ ref('dim_customers') }}

GROUP BY 
    country, 
    data_quality_tier, 
    customer_lifecycle_stage,
    account_created_year,
    account_created_month
