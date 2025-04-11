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

select distinct Dinamica from {{ ref("stg_nominations") }}
order by Dinamica ASC
where Eliminado_6 is not null

with
    unpivot as (

        {{
            dbt_utils.unpivot(
                relation=ref("stg_eviction"),
                exclude=["edicao", "semana", "dinamica"],
                cast_to="string",
                field_name="Categoria",
                value_name="Valor",
            )
        }}
    ),

select distinct evento from {{ref('dm_nominations')}}
order by event ASC
where tipo_paredao is null
--where semana = 'Semana 10' and edicao = 24