{% macro break_alias(table, column_to_break) %}

with
            prep_1 as (
                select
                    id_paredao,
                    edicao,
                    evento,
                    trim(replace({{column_to_break}},' & ',' ')) as participante,
                    trim(split({{column_to_break}}, ' ')[safe_offset(0)]) as name1,
                    trim(regexp_extract({{column_to_break}}, r'^[^ ]+ [^ ]+')) as double_name1,
                    porcentagem
                from {{table}}
            ),

            fix_1 as (
                select
                    id_paredao,
                    a.edicao,
                    evento,
                    participante,
                    name1,
                    double_name1,
                    array_agg(c.alias order by case
                                when concat(a.participante, a.edicao) = concat(c.alias, c.edicao) then 1
                                when concat(a.double_name1, a.edicao) = concat(c.alias, c.edicao) then 2
                                when concat(a.name1, a.edicao) = concat(c.alias, c.edicao) then 3
                                else null
                            end asc
                    ) as matched_aliases
                from prep_1 a
                left join {{ ref("dm_contestants") }} c
                    on concat(a.participante, a.edicao) = concat(c.alias, c.edicao)
                    or concat(a.double_name1, a.edicao) = concat(c.alias, c.edicao)
                    or concat(a.name1, a.edicao) = concat(c.alias, c.edicao)
                group by id_paredao, a.edicao, evento, participante, name1, double_name1
            ),

            select_alias1 as (
                select
                    id_paredao,
                    edicao,
                    evento,
                    participante,
                    trim(replace(participante,matched_aliases[safe_offset(0)],'')) as participante_remaining,
                    matched_aliases[safe_offset(0)] as alias,
                    from fix_1
            ),

            prep_2 as (
                select
                    id_paredao,
                    edicao,
                    evento,
                    trim(participante) as participante,
                    participante_remaining,
                    trim(split(participante_remaining,' ')[safe_offset(0)]) as name1,
                    trim(regexp_extract(participante_remaining, r'^[^ ]+ [^ ]+')) as double_name1,
                    alias
                    from select_alias1
            ),

            fix_2 as (
                select
                    id_paredao,
                    a.edicao,
                    evento,
                    participante,
                    participante_remaining,
                    name1,
                    double_name1,
                    a.alias,
                    array_agg(case when c.alias is null then '' else c.alias end order by case
                                when concat(a.participante_remaining, a.edicao) = concat(c.alias, c.edicao) then 1
                                when concat(a.double_name1, a.edicao) = concat(c.alias, c.edicao) then 2
                                when concat(a.name1, a.edicao) = concat(c.alias, c.edicao) then 3
                                else null
                            end asc
                    ) as matched_aliases
                from prep_2 a
                left join {{ ref("dm_contestants") }} c
                    on concat(a.participante_remaining, a.edicao) = concat(c.alias, c.edicao)
                    or concat(a.double_name1, a.edicao) = concat(c.alias, c.edicao)
                    or concat(a.name1, a.edicao) = concat(c.alias, c.edicao)
                group by id_paredao, a.edicao, evento, participante, participante_remaining, name1, double_name1,a.alias
            ),

            select_alias2 as (
                select
                    id_paredao,
                    edicao,
                    evento,
                    participante,
                    matched_aliases[safe_offset(0)] as new_alias,
                    trim(replace(participante_remaining,matched_aliases[safe_offset(0)],'')) as participante_remaining,
                    coalesce(alias,matched_aliases[safe_offset(0)]) as alias,
                    coalesce(matched_aliases[safe_offset(0)]) as alias_2
                    from fix_2
            ),

            prep_3 as (
                select
                    id_paredao,
                    edicao,
                    evento,
                    trim(participante) as participante,
                    participante_remaining,
                    trim(split(participante_remaining,' ')[safe_offset(0)]) as name1,
                    trim(regexp_extract(participante_remaining, r'^[^ ]+ [^ ]+')) as double_name1,
                    alias,
                    alias_2
                    from select_alias2
            ),

            fix_3 as (
                select
                    id_paredao,
                    a.edicao,
                    evento,
                    participante,
                    participante_remaining,
                    name1,
                    double_name1,
                    a.alias,
                    a.alias_2,
                    array_agg(case when c.alias is null then '' else c.alias end order by case
                                when concat(a.participante_remaining, a.edicao) = concat(c.alias, c.edicao) then 1
                                when concat(a.double_name1, a.edicao) = concat(c.alias, c.edicao) then 2
                                when concat(a.name1, a.edicao) = concat(c.alias, c.edicao) then 3
                                else null
                            end asc
                    ) as matched_aliases
                from prep_3 a
                left join {{ ref("dm_contestants") }} c
                    on concat(a.participante_remaining, a.edicao) = concat(c.alias, c.edicao)
                    or concat(a.double_name1, a.edicao) = concat(c.alias, c.edicao)
                    or concat(a.name1, a.edicao) = concat(c.alias, c.edicao)
                group by id_paredao, a.edicao, evento, participante, participante_remaining, name1, double_name1,a.alias,a.alias_2
            ),
            select_alias3 as (
                select
                    id_paredao,
                    edicao,
                    evento,
                    participante,
                    matched_aliases[safe_offset(0)] as new_alias,
                    trim(replace(participante_remaining,matched_aliases[safe_offset(0)],'')) as participante_remaining,
                    coalesce(alias,alias_2,matched_aliases[safe_offset(0)]) as alias,
                    coalesce(alias_2,matched_aliases[safe_offset(0)]) as alias_2,
                    coalesce(matched_aliases[safe_offset(0)]) as alias_3
                    from fix_3
            ),
            prep_4 as (
                select
                    id_paredao,
                    edicao,
                    evento,
                    trim(participante) as participante,
                    participante_remaining,
                    trim(split(participante_remaining,' ')[safe_offset(0)]) as name1,
                    trim(regexp_extract(participante_remaining, r'^[^ ]+ [^ ]+')) as double_name1,
                    alias,
                    alias_2,
                    alias_3
                    from select_alias3
            ),
            fix_4 as (
                select
                    id_paredao,
                    a.edicao,
                    evento,
                    participante,
                    participante_remaining,
                    name1,
                    double_name1,
                    a.alias,
                    a.alias_2,
                    a.alias_3,
                    array_agg(case when c.alias is null then '' else c.alias end order by case
                                when concat(a.participante_remaining, a.edicao) = concat(c.alias, c.edicao) then 1
                                when concat(a.double_name1, a.edicao) = concat(c.alias, c.edicao) then 2
                                when concat(a.name1, a.edicao) = concat(c.alias, c.edicao) then 3
                                else null
                            end asc
                    ) as matched_aliases
                from prep_4 a
                left join {{ ref("dm_contestants") }} c
                    on concat(a.participante_remaining, a.edicao) = concat(c.alias, c.edicao)
                    or concat(a.double_name1, a.edicao) = concat(c.alias, c.edicao)
                    or concat(a.name1, a.edicao) = concat(c.alias, c.edicao)
                group by id_paredao, a.edicao, evento, participante, participante_remaining, name1, double_name1,a.alias,a.alias_2,a.alias_3
            ),

            select_alias4 as (
                select
                    id_paredao,
                    edicao,
                    evento,
                    participante,
                    matched_aliases[safe_offset(0)] as new_alias,
                    trim(replace(participante_remaining,matched_aliases[safe_offset(0)],'')) as participante_remaining,
                    coalesce(alias,alias_2,alias_3,matched_aliases[safe_offset(0)]) as alias,
                    coalesce(alias_2,alias_3,matched_aliases[safe_offset(0)]) as alias_2,
                    coalesce(alias_3,matched_aliases[safe_offset(0)]) as alias_3,
                    coalesce(matched_aliases[safe_offset(0)]) as alias_4
                    from fix_4
            ),
            prep_5 as (
                select
                    id_paredao,
                    edicao,
                    evento,
                    trim(participante) as participante,
                    participante_remaining,
                    trim(split(participante_remaining,' ')[safe_offset(0)]) as name1,
                    trim(regexp_extract(participante_remaining, r'^[^ ]+ [^ ]+')) as double_name1,
                    alias,
                    alias_2,
                    alias_3,
                    alias_4
                    from select_alias4
            ),
            fix_5 as (
                select
                    id_paredao,
                    a.edicao,
                    evento,
                    participante,
                    participante_remaining,
                    name1,
                    double_name1,
                    a.alias,
                    a.alias_2,
                    a.alias_3,
                    a.alias_4,
                    array_agg(case when c.alias is null then '' else c.alias end order by case
                                when concat(a.participante_remaining, a.edicao) = concat(c.alias, c.edicao) then 1
                                when concat(a.double_name1, a.edicao) = concat(c.alias, c.edicao) then 2
                                when concat(a.name1, a.edicao) = concat(c.alias, c.edicao) then 3
                                else null
                            end asc
                    ) as matched_aliases
                from prep_5 a
                left join {{ ref("dm_contestants") }} c
                    on concat(a.participante_remaining, a.edicao) = concat(c.alias, c.edicao)
                    or concat(a.double_name1, a.edicao) = concat(c.alias, c.edicao)
                    or concat(a.name1, a.edicao) = concat(c.alias, c.edicao)
                group by id_paredao, a.edicao, evento, participante, participante_remaining, name1, double_name1,a.alias,a.alias_2,a.alias_3,a.alias_4
            ),

            select_alias5 as (
                select
                    id_paredao,
                    edicao,
                    evento,
                    participante,
                    matched_aliases[safe_offset(0)] as new_alias,
                    trim(replace(participante_remaining,matched_aliases[safe_offset(0)],'')) as participante_remaining,
                    coalesce(alias,alias_2,alias_3,alias_4,matched_aliases[safe_offset(0)]) as alias,
                    coalesce(alias_2,alias_3,alias_4,matched_aliases[safe_offset(0)]) as alias_2,
                    coalesce(alias_3,alias_4,matched_aliases[safe_offset(0)]) as alias_3,
                    coalesce(alias_4,matched_aliases[safe_offset(0)]) as alias_4,
                    coalesce(matched_aliases[safe_offset(0)]) as alias_5
                    from fix_5
            ),
            prep_6 as (
                select
                    id_paredao,
                    edicao,
                    evento,
                    trim(participante) as participante,
                    participante_remaining,
                    trim(split(participante_remaining,' ')[safe_offset(0)]) as name1,
                    trim(regexp_extract(participante_remaining, r'^[^ ]+ [^ ]+')) as double_name1,
                    alias,
                    alias_2,
                    alias_3,
                    alias_4,
                    alias_5
                    from select_alias5
            ),
            fix_6 as (
                select
                    id_paredao,
                    a.edicao,
                    evento,
                    participante,
                    participante_remaining,
                    name1,
                    double_name1,
                    a.alias,
                    a.alias_2,
                    a.alias_3,
                    a.alias_4,
                    a.alias_5,
                    array_agg(case when c.alias is null then '' else c.alias end order by case
                                when concat(a.participante_remaining, a.edicao) = concat(c.alias, c.edicao) then 1
                                when concat(a.double_name1, a.edicao) = concat(c.alias, c.edicao) then 2
                                when concat(a.name1, a.edicao) = concat(c.alias, c.edicao) then 3
                                else null
                            end asc
                    ) as matched_aliases
                from prep_6 a
                left join {{ ref("dm_contestants") }} c
                    on concat(a.participante_remaining, a.edicao) = concat(c.alias, c.edicao)
                    or concat(a.double_name1, a.edicao) = concat(c.alias, c.edicao)
                    or concat(a.name1, a.edicao) = concat(c.alias, c.edicao)
                group by id_paredao, a.edicao, evento, participante, participante_remaining, name1, double_name1,a.alias,a.alias_2,a.alias_3,a.alias_4,a.alias_5
            ),

            select_alias6 as (
                select
                    id_paredao,
                    edicao,
                    evento,
                    participante,
                    matched_aliases[safe_offset(0)] as new_alias,
                    trim(replace(participante_remaining,matched_aliases[safe_offset(0)],'')) as participante_remaining,
                    coalesce(alias,alias_2,alias_3,alias_4,alias_5,matched_aliases[safe_offset(0)]) as alias,
                    coalesce(alias_2,alias_3,alias_4,alias_5,matched_aliases[safe_offset(0)]) as alias_2,
                    coalesce(alias_3,alias_4,alias_5,matched_aliases[safe_offset(0)]) as alias_3,
                    coalesce(alias_4,alias_5,matched_aliases[safe_offset(0)]) as alias_4,
                    coalesce(alias_5,matched_aliases[safe_offset(0)]) as alias_5,
                    coalesce(matched_aliases[safe_offset(0)]) as alias_6
                    from fix_6
            ),

            prep_7 as (
                select
                    id_paredao,
                    edicao,
                    evento,
                    trim(participante) as participante,
                    participante_remaining,
                    trim(split(participante_remaining,' ')[safe_offset(0)]) as name1,
                    trim(regexp_extract(participante_remaining, r'^[^ ]+ [^ ]+')) as double_name1,
                    alias,
                    alias_2,
                    alias_3,
                    alias_4,
                    alias_5,
                    alias_6
                    from select_alias6
            ),
            fix_7 as (
                select
                    id_paredao,
                    a.edicao,
                    evento,
                    participante,
                    participante_remaining,
                    name1,
                    double_name1,
                    a.alias,
                    a.alias_2,
                    a.alias_3,
                    a.alias_4,
                    a.alias_5,
                    a.alias_6,
                    array_agg(case when c.alias is null then '' else c.alias end order by case
                                when concat(a.participante_remaining, a.edicao) = concat(c.alias, c.edicao) then 1
                                when concat(a.double_name1, a.edicao) = concat(c.alias, c.edicao) then 2
                                when concat(a.name1, a.edicao) = concat(c.alias, c.edicao) then 3
                                else null
                            end asc
                    ) as matched_aliases
                from prep_7 a
                left join {{ ref("dm_contestants") }} c
                    on concat(a.participante_remaining, a.edicao) = concat(c.alias, c.edicao)
                    or concat(a.double_name1, a.edicao) = concat(c.alias, c.edicao)
                    or concat(a.name1, a.edicao) = concat(c.alias, c.edicao)
                group by id_paredao, a.edicao, evento, participante, participante_remaining, name1, double_name1,a.alias,a.alias_2,a.alias_3,a.alias_4,a.alias_5,a.alias_6
            ),

            select_alias7 as (
                select
                    id_paredao,
                    edicao,
                    evento,
                    participante,
                    matched_aliases[safe_offset(0)] as new_alias,
                    trim(replace(participante_remaining,matched_aliases[safe_offset(0)],'')) as participante_remaining,
                    coalesce(alias,alias_2,alias_3,alias_4,alias_5,alias_6,matched_aliases[safe_offset(0)]) as alias,
                    coalesce(alias_2,alias_3,alias_4,alias_5,alias_6,matched_aliases[safe_offset(0)]) as alias_2,
                    coalesce(alias_3,alias_4,alias_5,alias_6,matched_aliases[safe_offset(0)]) as alias_3,
                    coalesce(alias_4,alias_5,alias_6,matched_aliases[safe_offset(0)]) as alias_4,
                    coalesce(alias_5,alias_6,matched_aliases[safe_offset(0)]) as alias_5,
                    coalesce(alias_6,matched_aliases[safe_offset(0)]) as alias_6,
                    coalesce(matched_aliases[safe_offset(0)]) as alias_7
                    from fix_7
            ),
            prep_8 as (
                select
                    id_paredao,
                    edicao,
                    evento,
                    trim(participante) as participante,
                    participante_remaining,
                    trim(split(participante_remaining,' ')[safe_offset(0)]) as name1,
                    trim(regexp_extract(participante_remaining, r'^[^ ]+ [^ ]+')) as double_name1,
                    alias,
                    alias_2,
                    alias_3,
                    alias_4,
                    alias_5,
                    alias_6,
                    alias_7
                    from select_alias7
            ),
            fix_8 as (
                select
                    id_paredao,
                    a.edicao,
                    evento,
                    participante,
                    participante_remaining,
                    name1,
                    double_name1,
                    a.alias,
                    a.alias_2,
                    a.alias_3,
                    a.alias_4,
                    a.alias_5,
                    a.alias_6,
                    a.alias_7,
                    array_agg(case when c.alias is null then '' else c.alias end order by case
                                when concat(a.participante_remaining, a.edicao) = concat(c.alias, c.edicao) then 1
                                when concat(a.double_name1, a.edicao) = concat(c.alias, c.edicao) then 2
                                when concat(a.name1, a.edicao) = concat(c.alias, c.edicao) then 3
                                else null
                            end asc
                    ) as matched_aliases
                from prep_8 a
                left join {{ ref("dm_contestants") }} c
                    on concat(a.participante_remaining, a.edicao) = concat(c.alias, c.edicao)
                    or concat(a.double_name1, a.edicao) = concat(c.alias, c.edicao)
                    or concat(a.name1, a.edicao) = concat(c.alias, c.edicao)
                group by id_paredao, a.edicao, evento, participante, participante_remaining, name1, double_name1,a.alias,a.alias_2,a.alias_3,a.alias_4,a.alias_5,a.alias_6,a.alias_7
            ),

            select_alias8 as (
                select
                    id_paredao,
                    edicao,
                    evento,
                    participante,
                    matched_aliases[safe_offset(0)] as new_alias,
                    trim(replace(participante_remaining,matched_aliases[safe_offset(0)],'')) as participante_remaining,
                    coalesce(alias,alias_2,alias_3,alias_4,alias_5,alias_6,alias_7,matched_aliases[safe_offset(0)]) as alias,
                    coalesce(alias_2,alias_3,alias_4,alias_5,alias_6,alias_7,matched_aliases[safe_offset(0)]) as alias_2,
                    coalesce(alias_3,alias_4,alias_5,alias_6,alias_7,matched_aliases[safe_offset(0)]) as alias_3,
                    coalesce(alias_4,alias_5,alias_6,alias_7,matched_aliases[safe_offset(0)]) as alias_4,
                    coalesce(alias_5,alias_6,alias_7,matched_aliases[safe_offset(0)]) as alias_5,
                    coalesce(alias_6,alias_7,matched_aliases[safe_offset(0)]) as alias_6,
                    coalesce(alias_7,matched_aliases[safe_offset(0)]) as alias_7,
                    coalesce(matched_aliases[safe_offset(0)]) as alias_8
                    from fix_8
            ),
            prep_9 as (
                select
                    id_paredao,
                    edicao,
                    evento,
                    trim(participante) as participante,
                    participante_remaining,
                    trim(split(participante_remaining,' ')[safe_offset(0)]) as name1,
                    trim(regexp_extract(participante_remaining, r'^[^ ]+ [^ ]+')) as double_name1,
                    alias,
                    alias_2,
                    alias_3,
                    alias_4,
                    alias_5,
                    alias_6,
                    alias_7,
                    alias_8
                    from select_alias8
            ),
            fix_9 as (
                select
                    id_paredao,
                    a.edicao,
                    evento,
                    participante,
                    participante_remaining,
                    name1,
                    double_name1,
                    a.alias,
                    a.alias_2,
                    a.alias_3,
                    a.alias_4,
                    a.alias_5,
                    a.alias_6,
                    a.alias_7,
                    a.alias_8,
                    array_agg(case when c.alias is null then '' else c.alias end order by case
                                when concat(a.participante_remaining, a.edicao) = concat(c.alias, c.edicao) then 1
                                when concat(a.double_name1, a.edicao) = concat(c.alias, c.edicao) then 2
                                when concat(a.name1, a.edicao) = concat(c.alias, c.edicao) then 3
                                else null
                            end asc
                    ) as matched_aliases
                from prep_9 a
                left join {{ ref("dm_contestants") }} c
                    on concat(a.participante_remaining, a.edicao) = concat(c.alias, c.edicao)
                    or concat(a.double_name1, a.edicao) = concat(c.alias, c.edicao)
                    or concat(a.name1, a.edicao) = concat(c.alias, c.edicao)
                group by id_paredao, a.edicao, evento, participante, participante_remaining, name1, double_name1,a.alias,a.alias_2,a.alias_3,a.alias_4,a.alias_5,a.alias_6,a.alias_7,a.alias_8
            ),

            select_alias9 as (
                select
                    id_paredao,
                    edicao,
                    evento,
                    participante,
                    matched_aliases[safe_offset(0)] as new_alias,
                    trim(replace(participante_remaining,matched_aliases[safe_offset(0)],'')) as participante_remaining,
                    coalesce(alias,alias_2,alias_3,alias_4,alias_5,alias_6,alias_7,alias_8,matched_aliases[safe_offset(0)]) as alias,
                    coalesce(alias_2,alias_3,alias_4,alias_5,alias_6,alias_7,alias_8,matched_aliases[safe_offset(0)]) as alias_2,
                    coalesce(alias_3,alias_4,alias_5,alias_6,alias_7,alias_8,matched_aliases[safe_offset(0)]) as alias_3,
                    coalesce(alias_4,alias_5,alias_6,alias_7,alias_8,matched_aliases[safe_offset(0)]) as alias_4,
                    coalesce(alias_5,alias_6,alias_7,alias_8,matched_aliases[safe_offset(0)]) as alias_5,
                    coalesce(alias_6,alias_7,alias_8,matched_aliases[safe_offset(0)]) as alias_6,
                    coalesce(alias_7,alias_8,matched_aliases[safe_offset(0)]) as alias_7,
                    coalesce(alias_8,matched_aliases[safe_offset(0)]) as alias_8,
                    coalesce(matched_aliases[safe_offset(0)]) as alias_9
                    from fix_9
            ),
            prep_10 as (
                select
                    id_paredao,
                    edicao,
                    evento,
                    trim(participante) as participante,
                    participante_remaining,
                    trim(split(participante_remaining,' ')[safe_offset(0)]) as name1,
                    trim(regexp_extract(participante_remaining, r'^[^ ]+ [^ ]+')) as double_name1,
                    alias,
                    alias_2,
                    alias_3,
                    alias_4,
                    alias_5,
                    alias_6,
                    alias_7,
                    alias_8,
                    alias_9
                    from select_alias9
            ),
            fix_10 as (
                select
                    id_paredao,
                    a.edicao,
                    evento,
                    participante,
                    participante_remaining,
                    name1,
                    double_name1,
                    a.alias,
                    a.alias_2,
                    a.alias_3,
                    a.alias_4,
                    a.alias_5,
                    a.alias_6,
                    a.alias_7,
                    a.alias_8,
                    a.alias_9,
                    array_agg(case when c.alias is null then '' else c.alias end order by case
                                when concat(a.participante_remaining, a.edicao) = concat(c.alias, c.edicao) then 1
                                when concat(a.double_name1, a.edicao) = concat(c.alias, c.edicao) then 2
                                when concat(a.name1, a.edicao) = concat(c.alias, c.edicao) then 3
                                else null
                            end asc
                    ) as matched_aliases
                from prep_10 a
                left join {{ ref("dm_contestants") }} c
                    on concat(a.participante_remaining, a.edicao) = concat(c.alias, c.edicao)
                    or concat(a.double_name1, a.edicao) = concat(c.alias, c.edicao)
                    or concat(a.name1, a.edicao) = concat(c.alias, c.edicao)
                group by id_paredao, a.edicao, evento, participante, participante_remaining, name1, double_name1,a.alias,a.alias_2,a.alias_3,a.alias_4,a.alias_5,a.alias_6,a.alias_7,a.alias_8,a.alias_9
            ),

            select_alias10 as (
                select
                    id_paredao,
                    edicao,
                    evento,
                    participante,
                    matched_aliases[safe_offset(0)] as new_alias,
                    trim(replace(participante_remaining,matched_aliases[safe_offset(0)],'')) as participante_remaining,
                    coalesce(alias,alias_2,alias_3,alias_4,alias_5,alias_6,alias_7,alias_8,alias_9,matched_aliases[safe_offset(0)]) as alias,
                    coalesce(alias_2,alias_3,alias_4,alias_5,alias_6,alias_7,alias_8,alias_9,matched_aliases[safe_offset(0)]) as alias_2,
                    coalesce(alias_3,alias_4,alias_5,alias_6,alias_7,alias_8,alias_9,matched_aliases[safe_offset(0)]) as alias_3,
                    coalesce(alias_4,alias_5,alias_6,alias_7,alias_8,alias_9,matched_aliases[safe_offset(0)]) as alias_4,
                    coalesce(alias_5,alias_6,alias_7,alias_8,alias_9,matched_aliases[safe_offset(0)]) as alias_5,
                    coalesce(alias_6,alias_7,alias_8,alias_9,matched_aliases[safe_offset(0)]) as alias_6,
                    coalesce(alias_7,alias_8,alias_9,matched_aliases[safe_offset(0)]) as alias_7,
                    coalesce(alias_8,alias_9,matched_aliases[safe_offset(0)]) as alias_8,
                    coalesce(alias_9,matched_aliases[safe_offset(0)]) as alias_9,
                    coalesce(matched_aliases[safe_offset(0)]) as alias_10
                    from fix_10
            ),
            prep_11 as (
                select
                    id_paredao,
                    edicao,
                    evento,
                    trim(participante) as participante,
                    participante_remaining,
                    trim(split(participante_remaining,' ')[safe_offset(0)]) as name1,
                    trim(regexp_extract(participante_remaining, r'^[^ ]+ [^ ]+')) as double_name1,
                    alias,
                    alias_2,
                    alias_3,
                    alias_4,
                    alias_5,
                    alias_6,
                    alias_7,
                    alias_8,
                    alias_9,
                    alias_10
                    from select_alias10
            ),
            fix_11 as (
                select
                    id_paredao,
                    a.edicao,
                    evento,
                    participante,
                    participante_remaining,
                    name1,
                    double_name1,
                    a.alias,
                    a.alias_2,
                    a.alias_3,
                    a.alias_4,
                    a.alias_5,
                    a.alias_6,
                    a.alias_7,
                    a.alias_8,
                    a.alias_9,
                    a.alias_10,
                    array_agg(case when c.alias is null then '' else c.alias end order by case
                                when concat(a.participante_remaining, a.edicao) = concat(c.alias, c.edicao) then 1
                                when concat(a.double_name1, a.edicao) = concat(c.alias, c.edicao) then 2
                                when concat(a.name1, a.edicao) = concat(c.alias, c.edicao) then 3
                                else null
                            end asc
                    ) as matched_aliases
                from prep_11 a
                left join {{ ref("dm_contestants") }} c
                    on concat(a.participante_remaining, a.edicao) = concat(c.alias, c.edicao)
                    or concat(a.double_name1, a.edicao) = concat(c.alias, c.edicao)
                    or concat(a.name1, a.edicao) = concat(c.alias, c.edicao)
                group by id_paredao, a.edicao, evento, participante, participante_remaining, name1, double_name1,a.alias,a.alias_2,a.alias_3,a.alias_4,a.alias_5,a.alias_6,a.alias_7,a.alias_8,a.alias_9,a.alias_10
            ),

            select_alias11 as (
                select
                    id_paredao,
                    edicao,
                    evento,
                    participante,
                    matched_aliases[safe_offset(0)] as new_alias,
                    trim(replace(participante_remaining,matched_aliases[safe_offset(0)],'')) as participante_remaining,
                    coalesce(alias,alias_2,alias_3,alias_4,alias_5,alias_6,alias_7,alias_8,alias_9,alias_10,matched_aliases[safe_offset(0)]) as alias,
                    coalesce(alias_2,alias_3,alias_4,alias_5,alias_6,alias_7,alias_8,alias_9,alias_10,matched_aliases[safe_offset(0)]) as alias_2,
                    coalesce(alias_3,alias_4,alias_5,alias_6,alias_7,alias_8,alias_9,alias_10,matched_aliases[safe_offset(0)]) as alias_3,
                    coalesce(alias_4,alias_5,alias_6,alias_7,alias_8,alias_9,alias_10,matched_aliases[safe_offset(0)]) as alias_4,
                    coalesce(alias_5,alias_6,alias_7,alias_8,alias_9,alias_10,matched_aliases[safe_offset(0)]) as alias_5,
                    coalesce(alias_6,alias_7,alias_8,alias_9,alias_10,matched_aliases[safe_offset(0)]) as alias_6,
                    coalesce(alias_7,alias_8,alias_9,alias_10,matched_aliases[safe_offset(0)]) as alias_7,
                    coalesce(alias_8,alias_9,alias_10,matched_aliases[safe_offset(0)]) as alias_8,
                    coalesce(alias_9,alias_10,matched_aliases[safe_offset(0)]) as alias_9,
                    coalesce(alias_10,matched_aliases[safe_offset(0)]) as alias_10,
                    coalesce(matched_aliases[safe_offset(0)]) as alias_11
                    from fix_11
            ),
            prep_12 as (
                select
                    id_paredao,
                    edicao,
                    evento,
                    trim(participante) as participante,
                    participante_remaining,
                    trim(split(participante_remaining,' ')[safe_offset(0)]) as name1,
                    trim(regexp_extract(participante_remaining, r'^[^ ]+ [^ ]+')) as double_name1,
                    alias,
                    alias_2,
                    alias_3,
                    alias_4,
                    alias_5,
                    alias_6,
                    alias_7,
                    alias_8,
                    alias_9,
                    alias_10,
                    alias_11
                    from select_alias11
            ),
            fix_12 as (
                select
                    id_paredao,
                    a.edicao,
                    evento,
                    participante,
                    participante_remaining,
                    name1,
                    double_name1,
                    a.alias,
                    a.alias_2,
                    a.alias_3,
                    a.alias_4,
                    a.alias_5,
                    a.alias_6,
                    a.alias_7,
                    a.alias_8,
                    a.alias_9,
                    a.alias_10,
                    a.alias_11,
                    array_agg(case when c.alias is null then '' else c.alias end order by case
                                when concat(a.participante_remaining, a.edicao) = concat(c.alias, c.edicao) then 1
                                when concat(a.double_name1, a.edicao) = concat(c.alias, c.edicao) then 2
                                when concat(a.name1, a.edicao) = concat(c.alias, c.edicao) then 3
                                else null
                            end asc
                    ) as matched_aliases
                from prep_12 a
                left join {{ ref("dm_contestants") }} c
                    on concat(a.participante_remaining, a.edicao) = concat(c.alias, c.edicao)
                    or concat(a.double_name1, a.edicao) = concat(c.alias, c.edicao)
                    or concat(a.name1, a.edicao) = concat(c.alias, c.edicao)
                group by id_paredao, a.edicao, evento, participante, participante_remaining, name1, double_name1,a.alias,a.alias_2,a.alias_3,a.alias_4,a.alias_5,a.alias_6,a.alias_7,a.alias_8,a.alias_9,a.alias_10,a.alias_11
            ),

            select_alias12 as (
                select
                    id_paredao,
                    edicao,
                    evento,
                    participante,
                    matched_aliases[safe_offset(0)] as new_alias,
                    trim(replace(participante_remaining,matched_aliases[safe_offset(0)],'')) as participante_remaining,
                    coalesce(alias,alias_2,alias_3,alias_4,alias_5,alias_6,alias_7,alias_8,alias_9,alias_10,alias_11,matched_aliases[safe_offset(0)]) as alias,
                    coalesce(alias_2,alias_3,alias_4,alias_5,alias_6,alias_7,alias_8,alias_9,alias_10,alias_11,matched_aliases[safe_offset(0)]) as alias_2,
                    coalesce(alias_3,alias_4,alias_5,alias_6,alias_7,alias_8,alias_9,alias_10,alias_11,matched_aliases[safe_offset(0)]) as alias_3,
                    coalesce(alias_4,alias_5,alias_6,alias_7,alias_8,alias_9,alias_10,alias_11,matched_aliases[safe_offset(0)]) as alias_4,
                    coalesce(alias_5,alias_6,alias_7,alias_8,alias_9,alias_10,alias_11,matched_aliases[safe_offset(0)]) as alias_5,
                    coalesce(alias_6,alias_7,alias_8,alias_9,alias_10,alias_11,matched_aliases[safe_offset(0)]) as alias_6,
                    coalesce(alias_7,alias_8,alias_9,alias_10,alias_11,matched_aliases[safe_offset(0)]) as alias_7,
                    coalesce(alias_8,alias_9,alias_10,alias_11,matched_aliases[safe_offset(0)]) as alias_8,
                    coalesce(alias_9,alias_10,alias_11,matched_aliases[safe_offset(0)]) as alias_9,
                    coalesce(alias_10,alias_11,matched_aliases[safe_offset(0)]) as alias_10,
                    coalesce(alias_11,matched_aliases[safe_offset(0)]) as alias_11,
                    coalesce(matched_aliases[safe_offset(0)]) as alias_12
                    from fix_12
            ),
            prep_13 as (
                select
                    id_paredao,
                    edicao,
                    evento,
                    trim(participante) as participante,
                    participante_remaining,
                    trim(split(participante_remaining,' ')[safe_offset(0)]) as name1,
                    trim(regexp_extract(participante_remaining, r'^[^ ]+ [^ ]+')) as double_name1,
                    alias,
                    alias_2,
                    alias_3,
                    alias_4,
                    alias_5,
                    alias_6,
                    alias_7,
                    alias_8,
                    alias_9,
                    alias_10,
                    alias_11,
                    alias_12
                    from select_alias12
            ),
            fix_13 as (
                select
                    id_paredao,
                    a.edicao,
                    evento,
                    participante,
                    participante_remaining,
                    name1,
                    double_name1,
                    a.alias,
                    a.alias_2,
                    a.alias_3,
                    a.alias_4,
                    a.alias_5,
                    a.alias_6,
                    a.alias_7,
                    a.alias_8,
                    a.alias_9,
                    a.alias_10,
                    a.alias_11,
                    a.alias_12,
                    array_agg(case when c.alias is null then '' else c.alias end order by case
                                when concat(a.participante_remaining, a.edicao) = concat(c.alias, c.edicao) then 1
                                when concat(a.double_name1, a.edicao) = concat(c.alias, c.edicao) then 2
                                when concat(a.name1, a.edicao) = concat(c.alias, c.edicao) then 3
                                else null
                            end asc
                    ) as matched_aliases
                from prep_13 a
                left join {{ ref("dm_contestants") }} c
                    on concat(a.participante_remaining, a.edicao) = concat(c.alias, c.edicao)
                    or concat(a.double_name1, a.edicao) = concat(c.alias, c.edicao)
                    or concat(a.name1, a.edicao) = concat(c.alias, c.edicao)
                group by id_paredao, a.edicao, evento, participante, participante_remaining, name1, double_name1,a.alias,a.alias_2,a.alias_3,a.alias_4,a.alias_5,a.alias_6,a.alias_7,a.alias_8,a.alias_9,a.alias_10,a.alias_11,a.alias_12
            ),

            select_alias13 as (
                select
                    id_paredao,
                    edicao,
                    evento,
                    participante,
                    matched_aliases[safe_offset(0)] as new_alias,
                    trim(replace(participante_remaining,matched_aliases[safe_offset(0)],'')) as participante_remaining,
                    coalesce(alias,alias_2,alias_3,alias_4,alias_5,alias_6,alias_7,alias_8,alias_9,alias_10,alias_11,alias_12,matched_aliases[safe_offset(0)]) as alias,
                    coalesce(alias_2,alias_3,alias_4,alias_5,alias_6,alias_7,alias_8,alias_9,alias_10,alias_11,alias_12,matched_aliases[safe_offset(0)]) as alias_2,
                    coalesce(alias_3,alias_4,alias_5,alias_6,alias_7,alias_8,alias_9,alias_10,alias_11,alias_12,matched_aliases[safe_offset(0)]) as alias_3,
                    coalesce(alias_4,alias_5,alias_6,alias_7,alias_8,alias_9,alias_10,alias_11,alias_12,matched_aliases[safe_offset(0)]) as alias_4,
                    coalesce(alias_5,alias_6,alias_7,alias_8,alias_9,alias_10,alias_11,alias_12,matched_aliases[safe_offset(0)]) as alias_5,
                    coalesce(alias_6,alias_7,alias_8,alias_9,alias_10,alias_11,alias_12,matched_aliases[safe_offset(0)]) as alias_6,
                    coalesce(alias_7,alias_8,alias_9,alias_10,alias_11,alias_12,matched_aliases[safe_offset(0)]) as alias_7,
                    coalesce(alias_8,alias_9,alias_10,alias_11,alias_12,matched_aliases[safe_offset(0)]) as alias_8,
                    coalesce(alias_9,alias_10,alias_11,alias_12,matched_aliases[safe_offset(0)]) as alias_9,
                    coalesce(alias_10,alias_11,alias_12,matched_aliases[safe_offset(0)]) as alias_10,
                    coalesce(alias_11,alias_12,matched_aliases[safe_offset(0)]) as alias_11,
                    coalesce(alias_12,matched_aliases[safe_offset(0)]) as alias_12,
                    coalesce(matched_aliases[safe_offset(0)]) as alias_13
                    from fix_13
            ),
            prep_14 as (
                select
                    id_paredao,
                    edicao,
                    evento,
                    trim(participante) as participante,
                    participante_remaining,
                    trim(split(participante_remaining,' ')[safe_offset(0)]) as name1,
                    trim(regexp_extract(participante_remaining, r'^[^ ]+ [^ ]+')) as double_name1,
                    alias,
                    alias_2,
                    alias_3,
                    alias_4,
                    alias_5,
                    alias_6,
                    alias_7,
                    alias_8,
                    alias_9,
                    alias_10,
                    alias_11,
                    alias_12,
                    alias_13
                    from select_alias13
            ),
            fix_14 as (
                select
                    id_paredao,
                    a.edicao,
                    evento,
                    participante,
                    participante_remaining,
                    name1,
                    double_name1,
                    a.alias,
                    a.alias_2,
                    a.alias_3,
                    a.alias_4,
                    a.alias_5,
                    a.alias_6,
                    a.alias_7,
                    a.alias_8,
                    a.alias_9,
                    a.alias_10,
                    a.alias_11,
                    a.alias_12,
                    a.alias_13,
                    array_agg(case when c.alias is null then '' else c.alias end order by case
                                when concat(a.participante_remaining, a.edicao) = concat(c.alias, c.edicao) then 1
                                when concat(a.double_name1, a.edicao) = concat(c.alias, c.edicao) then 2
                                when concat(a.name1, a.edicao) = concat(c.alias, c.edicao) then 3
                                else null
                            end asc
                    ) as matched_aliases
                from prep_14 a
                left join {{ ref("dm_contestants") }} c
                    on concat(a.participante_remaining, a.edicao) = concat(c.alias, c.edicao)
                    or concat(a.double_name1, a.edicao) = concat(c.alias, c.edicao)
                    or concat(a.name1, a.edicao) = concat(c.alias, c.edicao)
                group by id_paredao, a.edicao, evento, participante, participante_remaining, name1, double_name1,a.alias,a.alias_2,a.alias_3,a.alias_4,a.alias_5,a.alias_6,a.alias_7,a.alias_8,a.alias_9,a.alias_10,a.alias_11,a.alias_12,a.alias_13
            ),

            select_alias14 as (
                select
                    id_paredao,
                    edicao,
                    evento,
                    participante,
                    matched_aliases[safe_offset(0)] as new_alias,
                    trim(replace(participante_remaining,matched_aliases[safe_offset(0)],'')) as participante_remaining,
                    coalesce(alias,alias_2,alias_3,alias_4,alias_5,alias_6,alias_7,alias_8,alias_9,alias_10,alias_11,alias_12,alias_13,matched_aliases[safe_offset(0)]) as alias,
                    coalesce(alias_2,alias_3,alias_4,alias_5,alias_6,alias_7,alias_8,alias_9,alias_10,alias_11,alias_12,alias_13,matched_aliases[safe_offset(0)]) as alias_2,
                    coalesce(alias_3,alias_4,alias_5,alias_6,alias_7,alias_8,alias_9,alias_10,alias_11,alias_12,alias_13,matched_aliases[safe_offset(0)]) as alias_3,
                    coalesce(alias_4,alias_5,alias_6,alias_7,alias_8,alias_9,alias_10,alias_11,alias_12,alias_13,matched_aliases[safe_offset(0)]) as alias_4,
                    coalesce(alias_5,alias_6,alias_7,alias_8,alias_9,alias_10,alias_11,alias_12,alias_13,matched_aliases[safe_offset(0)]) as alias_5,
                    coalesce(alias_6,alias_7,alias_8,alias_9,alias_10,alias_11,alias_12,alias_13,matched_aliases[safe_offset(0)]) as alias_6,
                    coalesce(alias_7,alias_8,alias_9,alias_10,alias_11,alias_12,alias_13,matched_aliases[safe_offset(0)]) as alias_7,
                    coalesce(alias_8,alias_9,alias_10,alias_11,alias_12,alias_13,matched_aliases[safe_offset(0)]) as alias_8,
                    coalesce(alias_9,alias_10,alias_11,alias_12,alias_13,matched_aliases[safe_offset(0)]) as alias_9,
                    coalesce(alias_10,alias_11,alias_12,alias_13,matched_aliases[safe_offset(0)]) as alias_10,
                    coalesce(alias_11,alias_12,alias_13,matched_aliases[safe_offset(0)]) as alias_11,
                    coalesce(alias_12,alias_13,matched_aliases[safe_offset(0)]) as alias_12,
                    coalesce(alias_13,matched_aliases[safe_offset(0)]) as alias_13,
                    coalesce(matched_aliases[safe_offset(0)]) as alias_14
                    from fix_14
            )

        select *
        from select_alias14

        {% endmacro %}