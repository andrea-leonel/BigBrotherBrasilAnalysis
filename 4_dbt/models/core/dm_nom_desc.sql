{{ config(materialized="table") }}

with select_columns as (
    select
        id_paredao,
        edicao,
        semana,
        dinamica,
        tipo_paredao,
     from {{ ref("stg_nominations_2") }}
     where not tipo_paredao is null 
),

    nominations_n as (
        select
        id_paredao,
        case
            when count(case when evento = 'indicado' then 1 end) = 2 then 'duplo'
            when count(case when evento = 'indicado' then 1 end) = 3 then 'triplo'
            when count(case when evento = 'indicado' then 1 end) = 4 then 'quadruplo'
        else '4+'
        end as n_paredao
        from {{ ref("stg_nominations_2") }}
        group by id_paredao
    ),

    join_tables as (
        select a.*,
        b.n_paredao
        from select_columns a
        left join nominations_n b on a.id_paredao = b.id_paredao
    ),

    votos as (
        select
        a.*,
        case
            when b.votos = 'Nao divulgado' then null
            else CAST(REPLACE(b.votos, '.', '') AS INT) 
        end as votos
        from join_tables a left join {{ ref("stg_nominations") }} b on a.id_paredao = b.id_paredao
    )

select distinct * from votos
order by id_paredao asc