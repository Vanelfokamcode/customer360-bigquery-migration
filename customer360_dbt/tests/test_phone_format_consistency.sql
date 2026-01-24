-- Test phone number format consistency
-- French phones should be 10 digits or +33...

SELECT
  customer_id,
  phone_raw,
  phone_clean,
  LENGTH(phone_clean) AS phone_length
FROM {{ ref('stg_csv_customers') }}
WHERE phone_clean IS NOT NULL
  AND country = 'France'
  -- French phones: either 10 digits (0612345678) or 12 chars (+33612345678)
  AND LENGTH(phone_clean) NOT IN (10, 12)
