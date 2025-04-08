{% macro deduplicate_strings(column) %}
    array_to_string(array_agg(distinct trim(part)), ' | ')
    from
        (
            select trim(part) as part
            from unnest(split({{ column }}, ' | '))
            where trim(part) is not null and trim(part) != ''
        )
{% endmacro %}
