{% macro convert_month(date_column) %}

    CASE 
    WHEN INSTR({{ date_column }}, 'janeiro') > 0 THEN '01'
    WHEN INSTR({{ date_column }}, 'fevereiro') > 0 THEN '02'
    WHEN INSTR({{ date_column }}, 'março') > 0 THEN '03'
    WHEN INSTR({{ date_column }}, 'abril') > 0 THEN '04'
    WHEN INSTR({{ date_column }}, 'maio') > 0 THEN '05'
    WHEN INSTR({{ date_column }}, 'junho') > 0 THEN '06'
    WHEN INSTR({{ date_column }}, 'julho') > 0 THEN '07'
    WHEN INSTR({{ date_column }}, 'agosto') > 0 THEN '08'
    WHEN INSTR({{ date_column }}, 'setembro') > 0 THEN '09'
    WHEN INSTR({{ date_column }}, 'outubro') > 0 THEN '10'
    WHEN INSTR({{ date_column }}, 'novembro') > 0 THEN '11'
    WHEN INSTR({{ date_column }}, 'dezembro') > 0 THEN '12' END

{% endmacro %}