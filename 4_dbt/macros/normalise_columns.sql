{% macro normalise_columns(table_name) %}
    {% set columns_query %}
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{{ table_name }}'
    {% endset %}
    
    {% set results = run_query(columns_query) %}

    {% if execute %}
        {% set transformed_columns = [] %}
        {% for row in results %}
            {% set new_column_name = row['column_name'] | replace('__', '_') %}
            {% set transformed_columns = transformed_columns + [new_column_name] %}
        {% endfor %}
    {% endif %}

    {{ transformed_columns | join(', ') }}
{% endmacro %}