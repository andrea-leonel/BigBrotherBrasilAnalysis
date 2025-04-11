{{ config(materialized="table") }}

with unpivot_alias as (
        {{ dbt_utils.unpivot(
  relation=ref("stg_nominations_2"),
  cast_to='string',
  exclude=['id_paredao','edicao','semana','dinamica','tipo_paredao','evento','tipo_evento','participante','porcentagem'],
  field_name='aliases',
  value_name='participante_individual' )
 }}
    ),

    filter_table as (
        select * from unpivot_alias
        where not participante_individual = ''
    ),

    create_id_participante as (

        select *, {{ dbt_utils.generate_surrogate_key(['edicao','participante_individual']) }} AS id_participante
        from filter_table
        order by id_paredao,tipo_evento asc

    ),

    final_table as (
        select
            id_paredao,
            id_participante,
            edicao,
            semana,
            evento,
            tipo_evento,
            participante_individual as participante,
            porcentagem
        from create_id_participante
    )

    select * from final_table


