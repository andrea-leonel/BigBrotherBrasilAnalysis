select * from {{ ref("stg_nominations") }}
where lider is null and Indicado_lider is not null

select * from {{ ref("stg_nominations") }}
where id_paredao = '94fcb63b6dba53e9e2c712dd387db615'

select distinct * from {{ ref("stg_contestants") }}
where STRPOS(nome_completo, 'Grohalski') > 0

select distinct * from {{ ref("stg_ranking") }}
where STRPOS(participante, 'Grohalski') > 0
