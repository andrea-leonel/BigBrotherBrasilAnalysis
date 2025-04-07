{% macro break_week(week_column, new_column_dinamica) %}
    CASE 
        WHEN STRPOS({{ week_column }}, ' - ') > 0 THEN LEFT({{ week_column }}, STRPOS({{ week_column }}, ' - ') - 1)
        ELSE {{ week_column }}
    END As semana,
    CASE 
        WHEN STRPOS({{ week_column }}, ' - ') > 0 THEN SUBSTR({{ week_column }}, STRPOS({{ week_column }}, ' - ') + 3)
        ELSE NULL
    END AS {{ new_column_dinamica }}
{% endmacro %}