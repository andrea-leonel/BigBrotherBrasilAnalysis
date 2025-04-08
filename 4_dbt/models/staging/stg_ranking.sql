with 

source as (

    select * from {{ source('big-brother-brasil-454420', 'ranking') }}

),

ranking as (

    select
        pos_ AS posicao,
        regexp_extract(participante, r'^(.*?) ') AS primeiro_nome,
        meio_de_indicacao,
        __dos_votos,
        eliminado_em,
        edicao,
        indicado_por

    from source

)

select 
{{ dbt_utils.generate_surrogate_key(['edicao','primeiro_nome']) }} AS id_participante,
posicao,
primeiro_nome,
edicao
from ranking
