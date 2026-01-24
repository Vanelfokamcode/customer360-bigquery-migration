-- Test that account creation dates are reasonable
-- Flags dates in the future or too far in the past

SELECT
  customer_id,
  created_at_raw,
  created_at_parsed
FROM {{ ref('stg_csv_customers') }}
WHERE created_at_parsed IS NOT NULL
  AND (
    -- Future dates (impossible)
    created_at_parsed > CURRENT_DATE()
    -- Or unreasonably old (before 2000)
    OR created_at_parsed < '2000-01-01'
  )
