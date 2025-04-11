WITH unpivoted_data AS (
    {{ 
        dbt_utils.unpivot(
            ref('stg_ratings_weekly'),
            exclude=['edicao', 'Semana']
        ) 
    }}
)
SELECT *
FROM unpivoted_data
