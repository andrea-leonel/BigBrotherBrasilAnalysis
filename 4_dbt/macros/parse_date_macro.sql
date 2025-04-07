{% macro parse_date_macro(date_column) %}
    PARSE_DATE('%d de %m de %Y',
        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    REPLACE(
                                        REPLACE(
                                            REPLACE(
                                                REPLACE(
                                                    REPLACE({{ date_column }}, 'janeiro', '01'),
                                                    'fevereiro', '02'),
                                                'março', '03'),
                                            'abril', '04'),
                                        'maio', '05'),
                                    'junho', '06'),
                                'julho', '07'),
                            'agosto', '08'),
                        'setembro', '09'),
                    'outubro', '10'),
                'novembro', '11'),
            'dezembro', '12')
    )
{% endmacro %}