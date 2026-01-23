-- Test model to validate dbt setup
-- Selects first 10 customers from raw data

{{ config(
    materialized='view',
    tags=['test']
) }}

SELECT
    customer_id,
    email,
    first_name,
    last_name,
    country,
    created_at
FROM {{ source('raw_data', 'csv_customers') }}
LIMIT 10
