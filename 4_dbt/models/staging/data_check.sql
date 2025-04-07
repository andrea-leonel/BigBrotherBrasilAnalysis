{{ config(materialized='ephemeral') }}

select * from {{ source('staging', 'Contestants') }}
order by data_resultado ASC

select * from {{ source('staging', 'ranking') }}
where meio_de_indicacao = 'Poder Surpresa'

