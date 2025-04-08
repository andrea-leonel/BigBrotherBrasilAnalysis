with

    source as (select * from {{ source("big-brother-brasil-454420", "ranking") }}),
    
    ranking as (

        select
            pos_ as posicao,
            participante as participante_ou_dupla,
            case
                when strpos(participante, '&') > 0
                then regexp_extract(participante, r'^(.*?) &')
                else participante
            end as participante_1,
            case
                when strpos(participante, '&') > 0
                then regexp_extract(participante, '& (.*)')
                else null
            end as participante_2,
            meio_de_indicacao,
            __dos_votos,
            eliminado_em,
            edicao,
            indicado_por

        from source

    ),

    ranking_pivot as (

        select participante_ou_dupla, participante_1 as participante, posicao, edicao
        from ranking

    union all

    select participante_ou_dupla, participante_2 as participante, posicao, edicao
    from ranking
    where participante_2 is not null
    ),

    fixed_ranking as (
        
        select
        participante_ou_dupla,
        case 
            when participante = 'Jaquelline Grohalski' then 'Jaqueline Grohalski'
            else participante
        end as participante,
        posicao,
        edicao
        from ranking_pivot
    )

select
{{ dbt_utils.generate_surrogate_key(['edicao','participante']) }} AS id_participante,
*
from fixed_ranking
