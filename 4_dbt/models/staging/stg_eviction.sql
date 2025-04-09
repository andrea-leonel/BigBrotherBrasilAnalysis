with
    unpivot as (

        {{ dbt_utils.unpivot(
            relation=ref('stg_eviction_union'),
            exclude=['edicao','semana','dinamica','votos'],
            field_name='Evento',
            value_name='Participante'
) }}
    )

select * from unpivot
