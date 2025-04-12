{{ config(materialized="table") }}

with
    unpivot_table as (
        {{
            dbt_utils.unpivot(
                relation=ref("stg_nominations"),
                exclude=["id_paredao", "edicao", "semana", "dinamica"],
                cast_to="string",
                field_name="evento",
                value_name="participante",
            )
        }}
    ),

    filter_table as (
        select *
        from unpivot_table
        where participante is not null and not evento = 'votos'
        order by edicao, semana, dinamica asc
    ),

    evento_break as (
        select
            id_paredao,
            edicao,
            semana,
            dinamica,
            case
                when strpos(participante, 'para') > 0
                then regexp_extract(participante, r'(para.*)')
                else null
            end as tipo_paredao,
            case
                when strpos(evento, 'indicado') > 0
                then 'indicado'
                when strpos(evento, 'eliminado') > 0
                then 'mais votado'
                when strpos(evento, 'porcent_outros') > 0
                then 'outros votados'
                when strpos(evento, 'expulso') > 0
                then 'expulso'
                when strpos(evento, 'indicado') > 0
                then 'indicado'
                else evento
            end as evento,
            case
                when strpos(evento, 'eliminado') > 0
                then replace(evento, 'eliminado', 'mais votado')
                when strpos(evento, 'porcent_outros') > 0
                then replace(evento, 'porcent_outros', 'outros_votados')
                else evento
            end as tipo_evento,
            case
                when strpos(participante, '%') > 0
                then regexp_extract(participante, r'^(.*?) \d')
                when strpos(participante, 'pontos') > 0
                then regexp_extract(participante, r'^(.*?) \d')
                when strpos(participante, 'eliminar') > 0
                then regexp_extract(participante, r'^(.*?) Nao')
                else participante
            end as participante,
            case
                when strpos(participante, '%') > 0
                then regexp_extract(participante, r'\d.*?%')
                else null
            end as porcentagem
        from filter_table
    ),

    participant_break as ({{ break_alias("evento_break", "participante") }}),

    join_alias as (
        select
            a.*,
            b.alias,
            b.alias_2,
            b.alias_3,
            b.alias_4,
            b.alias_5,
            b.alias_6,
            b.alias_7,
            b.alias_8,
            b.alias_9,
            b.alias_10,
            b.alias_11,
            b.alias_12,
            b.alias_13,
            b.alias_14
        from evento_break a
        left join
            participant_break b
            on concat(a.id_paredao, a.evento, trim(replace(a.participante, ' & ', ' ')))
            = concat(b.id_paredao, b.evento, b.participante)
    )

    select * from join_alias

-- dbt build --select stg_nominations2.sql --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
