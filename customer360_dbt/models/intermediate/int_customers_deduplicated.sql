{{ config(
    materialized='table',
    tags=['intermediate', 'customers']
) }}

/*
INTERMEDIATE: Customer Deduplication
- Identity resolution based on normalized email
- Keeps first occurrence (earliest created_at)
- 5,437 raw customers → ~4,501 unique customers
*/

WITH customers_with_match_key AS (
    SELECT
        *,
        -- Create identity matching key from normalized email
        TO_HEX(MD5(LOWER(TRIM(COALESCE(email_clean, customer_id))))) AS identity_match_key
    FROM {{ ref('stg_csv_customers') }}
),

ranked_customers AS (
    SELECT
        *,
        -- Rank by earliest account creation
        ROW_NUMBER() OVER (
            PARTITION BY identity_match_key 
            ORDER BY 
                created_at_parsed ASC NULLS LAST,
                customer_id ASC
        ) AS customer_rank
    FROM customers_with_match_key
),

deduplicated AS (
    SELECT
        -- Keep original customer_id of first occurrence
        customer_id,
        identity_match_key,
        
        -- Email (already cleaned)
        email_clean,
        is_valid_email,
        
        -- Names (already cleaned)
        first_name_clean,
        last_name_clean,
        
        -- Full name
        CONCAT(
            COALESCE(first_name_clean, ''),
            ' ',
            COALESCE(last_name_clean, '')
        ) AS full_name,
        
        -- Contact info
        phone_clean,
        address,
        city,
        country,
        
        -- Dates
        created_at_parsed AS account_created_at,
        
        -- Metadata
        customer_rank,  -- Always = 1 for deduplicated records
        loaded_at
        
    FROM ranked_customers
    WHERE customer_rank = 1  -- Keep only first occurrence
)

SELECT * FROM deduplicated
