{{ config(materialized="table") }}

with select_columns as (
    select
        id_paredao,
        edicao,
        semana,
        dinamica,
        tipo_paredao    
     from {{ ref("stg_nominations_2") }}
     where not tipo_paredao is null 
)

select distinct * from select_columns
order by edicao, semana, dinamica asc