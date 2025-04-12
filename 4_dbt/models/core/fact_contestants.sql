{{ config(materialized="view") }}

with
    contestant_table as (
        select
            id_participante,
            edicao,
            alias,
            (select count(n.evento) from {{ ref("dm_nominations") }} n
            where n.evento = 'lider' and n.id_participante = c.id_participante) as n_lider,
            (select count(n.evento) from {{ ref("dm_nominations") }} n
            where n.evento = 'anjo' and n.id_participante = c.id_participante) as n_anjo,
            (select count(n.evento) from {{ ref("dm_nominations") }} n
            where n.evento = 'indicado' and n.id_participante = c.id_participante) as n_indicado,
            (select count(n.tipo_evento) from {{ ref("dm_nominations") }} n
            where n.tipo_evento = 'indicado_lider' and n.id_participante = c.id_participante) as n_indicado_lider,
            (select count(n.tipo_evento) from {{ ref("dm_nominations") }} n
            where n.tipo_evento = 'indicado_casa' and n.id_participante = c.id_participante) as n_indicado_casa,
            (select count(n.tipo_evento) from {{ ref("dm_nominations") }} n
            where n.tipo_evento = 'indicado_big_fone' and n.id_participante = c.id_participante) as n_indicado_big_fone,
            (select count(n.tipo_evento) from {{ ref("dm_nominations") }} n
            where strpos(n.tipo_evento,'indicado') > 0 and not n.tipo_evento in ('indicado_casa','indicado_big_fone','indicado_lider') and n.id_participante = c.id_participante) as n_indicado_outros


        from {{ ref("dm_contestants") }} c
        group by id_participante, edicao, alias

            )

            select * from contestant_table
