{% macro parse_mixed_dates(column_name) %}
    CASE
        -- ISO format: 2023-01-15 (YYYY-MM-DD)
        WHEN REGEXP_CONTAINS({{ column_name }}, r'^\d{4}-\d{2}-\d{2}')
            THEN PARSE_DATE('%Y-%m-%d', {{ column_name }})
        
        -- European format: 15/01/2023 (DD/MM/YYYY)
        WHEN REGEXP_CONTAINS({{ column_name }}, r'^\d{2}/\d{2}/\d{4}')
            THEN PARSE_DATE('%d/%m/%Y', {{ column_name }})
        
        -- US format: 01-15-2023 (MM-DD-YYYY)
        WHEN REGEXP_CONTAINS({{ column_name }}, r'^\d{2}-\d{2}-\d{4}')
            THEN PARSE_DATE('%m-%d-%Y', {{ column_name }})
        
        -- Null for invalid formats
        ELSE NULL
    END
{% endmacro %}
