-- Test that is_valid_email only contains TRUE or FALSE (no NULL, no other values)

SELECT
  customer_id,
  is_valid_email
FROM {{ ref('stg_csv_customers') }}
WHERE is_valid_email IS NULL
   OR is_valid_email NOT IN (TRUE, FALSE)
