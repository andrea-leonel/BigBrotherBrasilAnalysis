SELECT *
FROM {{ source('big-brother-brasil-454420', 'Contestants') }}
WHERE STRPOS(data_resultado, 'ento') > 0