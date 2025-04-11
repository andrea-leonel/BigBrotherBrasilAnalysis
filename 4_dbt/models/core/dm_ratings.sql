{{ config(materialized="table") }}

with unpivot_days as (
        {{ dbt_utils.unpivot(
  relation=ref("stg_ratings_weekly"),
  cast_to='string',
  exclude=['edicao','semana','data_de_transmissao','media_semanal'],
  field_name='dia_semana',
  value_name='pontos' )
 }}
    ),

    null_values as (
        select 
            edicao,
            semana,
            data_de_transmissao,
            case when strpos(cast(media_semanal as string), 'nan')>0 then null else media_semanal end as media_semanal,
            dia_semana,
            case when strpos(cast(pontos as string), 'nan')>0 then null else pontos end as pontos
            from unpivot_days
    ),

    pontos_conversion as (
        select
        a.edicao,
        semana,
        data_de_transmissao,
        cast(media_semanal as FLOAT64) as media_semanal,
        dia_semana,
        cast(pontos as FLOAT64) as pontos,
        cast(safe_multiply(cast(a.media_semanal as FLOAT64), cast(b.domicilios as FLOAT64)) as int64) as domicilios_semana,
        cast(safe_multiply(cast(a.pontos as FLOAT64), cast(b.domicilios as FLOAT64)) as int64) as domicilios_dia
        from null_values a
        left join {{ref("rating_points")}} b on a.edicao = b.edicao 
    )
    
    select * from pontos_conversion
