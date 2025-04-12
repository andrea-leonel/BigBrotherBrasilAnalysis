{{ config(materialized="view") }}


with
    edicao_table as (
        select
            c.edicao,
            count(distinct c.id_participante) as n_participantes,
            count(distinct n.semana) as n_semanas,
            count(distinct n.id_paredao) as n_paredoes,

            (
                select count(n.id_participante)
                from {{ ref("dm_nominations") }} n
                where n.evento = 'lider' and n.edicao = c.edicao
            ) as n_lider,
            (
                select count(n.id_participante)
                from {{ ref("dm_nominations") }} n
                where n.evento = 'anjo' and n.edicao = c.edicao
            ) as n_anjo,
            (
                select count(n.id_participante)
                from {{ ref("dm_nominations") }} n
                where n.evento = 'big_fone' and n.edicao = c.edicao
            ) as n_big_fone,
            (
                select count(n.id_participante)
                from {{ ref("dm_nominations") }} n
                where n.evento = 'expulso' and n.edicao = c.edicao
            ) as n_expulsoes,
            (
                select count(n.id_participante)
                from {{ ref("dm_nominations") }} n
                where n.evento = 'desistente' and n.edicao = c.edicao
            ) as n_desistencias,

            round(cast(avg(r.pontos) as float64), 1) as pontos_media_diario,
            round(cast(min(r.pontos) as float64), 1) as pontos_min_diario,
            round(cast(max(r.pontos) as float64), 1) as pontos_max_diario,
            cast(avg(r.domicilios_dia) as int64) as domicilios_media_diario,
            cast(min(r.domicilios_dia) as int64) as domicilios_min_diario,
            cast(max(r.domicilios_dia) as int64) as domicilios_max_diario

        from {{ ref("dm_contestants") }} c
        left join {{ ref("dm_nominations") }} n on c.edicao = n.edicao
        left join {{ ref("dm_ratings") }} r on c.edicao = r.edicao
        left join {{ ref("dm_nom_desc") }} d on c.edicao = d.edicao
        group by c.edicao
        order by edicao asc
    ),

    

    female as (
        select edicao, count(genero) as n_mulheres
        from {{ ref("dm_contestants") }}
        where genero = 'F'
        group by edicao),

    male as (
        select edicao, count(genero) as n_homens
        from {{ ref("dm_contestants") }}
        where genero = 'M'
        group by edicao),

    join_tables as (
        select e.*, f.n_mulheres, m.n_homens
        from edicao_table e
        left join female f on f.edicao = e.edicao
        left join male m on m.edicao = e.edicao)

select *
from
    join_tables

    
-- add votos
