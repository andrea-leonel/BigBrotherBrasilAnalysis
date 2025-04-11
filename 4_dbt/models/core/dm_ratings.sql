{{ config(materialized="table") }}

with unpivot_days as (
        {{ dbt_utils.unpivot(
  relation=ref("stg_ratings_weekly"),
  cast_to='string',
  exclude=['edicao','semana','data_de_transmissao','media_semanal'],
  field_name='dia_semana',
  value_name='pontos' )
 }}
    )

    select * from unpivot_days