{{ config(
    materialized='view',
    tags=['marts', 'bi-export']
) }}

/*
VIEW: Power BI Export - Metrics
- Pre-aggregated metrics
- Fast dashboard loading
*/

SELECT
    country,
    data_quality_tier,
    customer_lifecycle_stage,
    account_created_year,
    account_created_month,
    
    total_customers,
    pct_valid_email,
    pct_with_phone,
    pct_complete_profile,
    
    calculated_at AS last_updated

FROM {{ ref('customer_metrics') }}
