{{ config(materialized='table') }}

with contestant_data as (
    select * from {{ ref('stg_contestants')}}
),

ranking_data as (
    select * from {{ ref('stg_ranking')}}
)

select c.*, r.posicao from contestant_data c
left join ranking_data r on CONCAT('%', c.alias, '%') LIKE r.participante
and r.edicao = c.edicao

