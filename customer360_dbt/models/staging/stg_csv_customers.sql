{{
    config(
        materialized='view',
        tags=['staging', 'customers']
    )
}}

/*
Staging model for customer data
- Cleans and normalizes raw customer data
- Validates email formats
- Parses mixed date formats
- No deduplication (done in intermediate layer)
*/

WITH source AS (
    SELECT * FROM {{ source('raw_data', 'csv_customers') }}
),

cleaned AS (
    SELECT
        -- IDs
        customer_id,
        
        -- EMAIL CLEANING
        CASE
            WHEN email IS NULL THEN NULL
            WHEN TRIM(email) = '' THEN NULL
            WHEN NOT REGEXP_CONTAINS(TRIM(email), r'@') THEN NULL
            ELSE LOWER(TRIM(email))
        END AS email_clean,
        email AS email_raw,
        
        -- EMAIL VALIDATION
        CASE 
            WHEN email IS NULL THEN FALSE
            WHEN TRIM(email) = '' THEN FALSE
            WHEN REGEXP_CONTAINS(
                LOWER(TRIM(email)), 
                r'^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$'
            ) THEN TRUE
            ELSE FALSE
        END AS is_valid_email,
        
        -- NAME CLEANING (capitalize first letter manually)
        CASE
            WHEN first_name IS NULL THEN NULL
            WHEN TRIM(first_name) = '' THEN NULL
            ELSE CONCAT(
                UPPER(SUBSTR(REGEXP_REPLACE(TRIM(first_name), r'[™©®]', ''), 1, 1)),
                LOWER(SUBSTR(REGEXP_REPLACE(TRIM(first_name), r'[™©®]', ''), 2))
            )
        END AS first_name_clean,
        first_name AS first_name_raw,
        
        CASE
            WHEN last_name IS NULL THEN NULL
            WHEN TRIM(last_name) = '' THEN NULL
            ELSE CONCAT(
                UPPER(SUBSTR(REGEXP_REPLACE(TRIM(last_name), r'[™©®]', ''), 1, 1)),
                LOWER(SUBSTR(REGEXP_REPLACE(TRIM(last_name), r'[™©®]', ''), 2))
            )
        END AS last_name_clean,
        last_name AS last_name_raw,
        
        -- PHONE CLEANING
        CASE
            WHEN phone IS NULL THEN NULL
            -- Remove all non-digits and non-plus
            ELSE REGEXP_REPLACE(phone, r'[^0-9+]', '')
        END AS phone_clean,
        phone AS phone_raw,
        
        -- ADDRESS (keep as-is for now)
        address,
        city,
        country,
        
        -- DATE PARSING (using macro)
        created_at AS created_at_raw,
        {{ parse_mixed_dates('created_at') }} AS created_at_parsed,
        
        -- METADATA
        loaded_at,
        source_file
        
    FROM source
)

SELECT * FROM cleaned
