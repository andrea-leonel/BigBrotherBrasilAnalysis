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

    participant_break as (
        -- matching solo names, single names, double names
        with
            inital_table as (
                select
                    *,
                    split(participante, ' ')[safe_offset(0)] as name1,
                    regexp_extract(participante, r'^(.*? .*?) ') as double_name1
                from evento_break
            ),

            fix_1 as (
                select a.*, c.alias
                from inital_table a
                left join
                    {{ ref("dm_contestants") }} c
                    on concat(c.alias, c.edicao) = concat(a.participante, a.edicao) 
                    or concat(c.alias, c.edicao) = concat(a.double_name1, a.edicao)
                    or concat(c.alias, c.edicao) = concat(a.name1, a.edicao)
            ),

            check_1 as (
                select
                    id_paredao,
                    edicao,
                    semana,
                    dinamica,
                    tipo_paredao,
                    evento,
                    tipo_evento,
                    participante,
                    case
                        when alias is null
                        then participante
                        else replace(participante, alias, '')
                    end as participante_remaining,
                    alias,
                    porcentagem
                from fix_1
            ),

            prep_2 as (
                select
                    id_paredao,
                    edicao,
                    semana,
                    dinamica,
                    tipo_paredao,
                    evento,
                    tipo_evento,
                    participante,
                    trim(participante_remaining) as participante_remaining,
                    REGEXP_EXTRACT(TRIM(participante_remaining), r'^[^ ]+') AS name1,
                    REGEXP_EXTRACT(TRIM(participante_remaining), r'^[^ ]+ [^ ]+') AS double_name1,
                    alias,
                    porcentagem
                from check_1
            ),

            fix_2 as (
                select a.*, 
                c.alias as alias_new
                from prep_2 a
                left join
                    {{ ref("dm_contestants") }} c
                    on concat(c.alias, c.edicao) = concat(a.participante_remaining, a.edicao)
                    or concat(c.alias, c.edicao) = concat(a.double_name1, a.edicao) 
                    or concat(c.alias, c.edicao) = concat(a.name1, a.edicao)
            ),

            check_2 as (
                select
                    id_paredao,
                    edicao,
                    semana,
                    dinamica,
                    tipo_paredao,
                    evento,
                    tipo_evento,
                    participante,
                    case
                        when alias_new is null
                        then participante_remaining
                        else replace(participante_remaining, alias_new, '')
                    end as participante_remaining,
                    case 
                        when alias is null then alias_new
                        else alias 
                    end as alias,
                    case 
                        when alias is not null then alias_new
                        else null
                    end as alias2,
                    alias_new,
                    porcentagem
                from fix_2
            ),

            prep_3 as (
                select
                    id_paredao,
                    edicao,
                    semana,
                    dinamica,
                    tipo_paredao,
                    evento,
                    tipo_evento,
                    participante,
                    trim(participante_remaining) as participante_remaining,
                    REGEXP_EXTRACT(TRIM(participante_remaining), r'^[^ ]+') AS name1,
                    REGEXP_EXTRACT(TRIM(participante_remaining), r'^[^ ]+ [^ ]+') AS double_name1,
                    alias,
                    porcentagem
                from check_2
            ),

            fix_3 as (
                select a.*, 
                c.alias as alias_new
                from prep_3 a
                left join
                    {{ ref("dm_contestants") }} c
                    on concat(c.alias, c.edicao) = concat(a.participante_remaining, a.edicao)
                    or concat(c.alias, c.edicao) = concat(a.double_name1, a.edicao) 
                    or concat(c.alias, c.edicao) = concat(a.name1, a.edicao)
            ),

            check_3 as (
                select
                    id_paredao,
                    edicao,
                    semana,
                    dinamica,
                    tipo_paredao,
                    evento,
                    tipo_evento,
                    participante,
                    case
                        when alias_new is null
                        then participante_remaining
                        else replace(participante_remaining, alias_new, '')
                    end as participante_remaining,
                    case 
                        when alias is null then alias_new
                        else alias 
                    end as alias,
                    case 
                        when alias is not null then alias_new
                        else null
                    end as alias2,
                    case
                        when alias2 is not null and alias is not null then alias_new
                        else null
                    end as alias3,  
                    alias_new,
                    porcentagem
                from fix_3
            )
            
        -- subtract one string from the other
        -- union all the tables
        select *
        from check_3


    )

select *
from participant_break
