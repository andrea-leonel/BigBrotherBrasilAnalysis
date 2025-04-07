{% macro parse_date(date_column) %}
-- macro only works with day de month de year formats
    CASE WHEN {{ date_column }} IS NULL THEN NULL
    DATE(
        CAST(RIGHT({{ date_column }}, 4) AS INT), -- Extract year
        CAST({{ convert_month(date_column) }} AS INT), -- Convert month
        CAST(
            CASE 
                WHEN STRPOS({{ date_column }}, ' ') > 0 
                THEN LEFT({{ date_column }}, STRPOS({{ date_column }}, ' ') - 1)
                ELSE NULL
            END AS INT
        )
    ) END
{% endmacro %}