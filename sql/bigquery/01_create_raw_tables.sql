-- =========================================
-- BigQuery DDL: RAW DATA Tables
-- Dataset: raw_data
-- =========================================

-- =========================================
-- Table: csv_customers
-- Source: PostgreSQL raw.csv_customers → CSV → BigQuery
-- Purpose: Immutable landing zone for customer data
-- =========================================

-- Drop table if exists (for idempotent script)
DROP TABLE IF EXISTS `raw_data.csv_customers`;

CREATE TABLE `raw_data.csv_customers` (
    -- Unique identifier
    customer_id STRING NOT NULL OPTIONS(description="Unique customer ID from source system"),
    
    -- Personal information
    first_name STRING OPTIONS(description="Customer first name (may contain special chars)"),
    last_name STRING OPTIONS(description="Customer last name"),
    email STRING OPTIONS(description="Email address (may be invalid - validation in staging)"),
    phone STRING OPTIONS(description="Phone number (mixed formats)"),
    
    -- Address information
    address STRING OPTIONS(description="Full address"),
    city STRING OPTIONS(description="City name"),
    country STRING OPTIONS(description="Country code (e.g., FR, BE, CH)"),
    
    -- Timestamps
    created_at STRING OPTIONS(description="Account creation date (STRING due to mixed formats - will parse in staging)"),
    loaded_at TIMESTAMP OPTIONS(description="Timestamp when data was loaded into BigQuery"),
    
    -- Metadata
    source_file STRING OPTIONS(description="Source CSV filename for audit trail")
)
CLUSTER BY customer_id
OPTIONS(
    description="Raw customer data from PostgreSQL - immutable landing zone. Data quality issues intentional for demo.",
    labels=[("source", "postgresql"), ("layer", "raw"), ("migrated", "2024-01-22")]
);

-- =========================================
-- NOTES:
-- =========================================
-- 1. created_at is STRING because source has mixed date formats:
--    - ISO: "2023-01-15"
--    - European: "15/01/2023"  
--    - US: "01-15-2023"
--    Will be parsed to proper DATE in staging layer.
--
-- 2. customer_id clustering:
--    - Most queries filter by customer_id
--    - Expected 10-100x performance improvement
--
-- 3. Data quality issues (intentional):
--    - ~63 invalid email formats (1.16%)
--    - ~936 duplicate customers (will be deduplicated in warehouse)
--    - Mixed date formats (handled in staging)
--
-- 4. Row count validation:
--    - Expected: 5,000 rows
--    - Run: SELECT COUNT(*) FROM `raw_data.csv_customers`;
-- =========================================

