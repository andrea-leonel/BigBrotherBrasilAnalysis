select * from {{ ref("stg_nominations") }}
where poder_do_veto is null and veto is not null
--where edicao = 10 and semana = 'Sem 6' and dinamica = 'Dia 41'

select * from {{ ref("stg_nominations") }}
where STRPOS(semana, 'Sem ') > 0

select distinct * from {{ ref("stg_contestants") }}
where STRPOS(nome_completo, 'Grohalski') > 0

select distinct * from {{ ref("stg_ranking") }}
where STRPOS(participante, 'Grohalski') > 0

select Edicao, Semana, Dinamica, Eliminado, Eliminado_5, Eliminado_6 from {{ ref("stg_eviction") }}
where Eliminado_6 is not null

select distinct Dinamica from {{ ref("stg_eviction_union") }}
order by Dinamica ASC
where Eliminado_6 is not null

select * from {{ ref("stg_eviction") }}

    column_select as (
        select
            {{ dbt_utils.generate_surrogate_key(["edicao", "semana", "dinamica"]) }}
            as id_paredao,
            edicao,
            replace(semana, 'Sem ', 'Semana ') as semana,
            dinamica,
            eliminado_cons
        from column_fix
