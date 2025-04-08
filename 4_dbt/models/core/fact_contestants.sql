with contestant_data as (
    select * from {{ ref('stg_contestants')}}
),

ranking_data as (
    select * from {{ ref('stg_ranking')}}
)

select c.*, r.posicao from contestant_data c
left join ranking_data r on r.participante LIKE CONCAT('%', c.alias, '%') 
and r.edicao = c.edicao

