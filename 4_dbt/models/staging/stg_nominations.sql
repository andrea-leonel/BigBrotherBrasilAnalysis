with
    merged_tables as (
        {{
            dbt_utils.union_relations(
                relations=[
                    source("big-brother-brasil-454420", "nominations1"),
                    source("big-brother-brasil-454420", "nominations2"),
                    source("big-brother-brasil-454420", "nominations3"),
                    source("big-brother-brasil-454420", "nominations4"),
                    source("big-brother-brasil-454420", "nominations5"),
                    source("big-brother-brasil-454420", "nominations6"),
                    source("big-brother-brasil-454420", "nominations7"),
                    source("big-brother-brasil-454420", "nominations8"),
                    source("big-brother-brasil-454420", "nominations9"),
                    source("big-brother-brasil-454420", "nominations10"),
                    source("big-brother-brasil-454420", "nominations11"),
                    source("big-brother-brasil-454420", "nominations12"),
                    source("big-brother-brasil-454420", "nominations13"),
                    source("big-brother-brasil-454420", "nominations14"),
                    source("big-brother-brasil-454420", "nominations15"),
                    source("big-brother-brasil-454420", "nominations16"),
                    source("big-brother-brasil-454420", "nominations17"),
                    source("big-brother-brasil-454420", "nominations18"),
                    source("big-brother-brasil-454420", "nominations19"),
                    source("big-brother-brasil-454420", "nominations20"),
                    source("big-brother-brasil-454420", "nominations21"),
                    source("big-brother-brasil-454420", "nominations22"),
                    source("big-brother-brasil-454420", "nominations23"),
                    source("big-brother-brasil-454420", "nominations24"),
                    source("big-brother-brasil-454420", "nominations25"),
                ],
                source_column_name=None,
                exclude=["int64_field_0"],
            )
        }}
    ),

    pre_fix_table as (
        select
        {{ dbt_utils.generate_surrogate_key(['edicao','semana','dinamica']) }} AS id_paredao,
        edicao,
        semana,
        dinamica,
        lider,
        anjo,
        poder_do_veto,
        poder_curinga,
        big_fone,
        imunizado,
        veto,
        indicado_lider,
        indicado_big_fone,
        indicado_casa,
        indicado_contragolpe,
        bate_e_volta_salvo
        from
        merged_tables
    ),

    remove_invalid_nominations as (
        SELECT * from pre_fix_table
        WHERE NOT id_paredao = '94fcb63b6dba53e9e2c712dd387db615'
    ),

    fixed_nominations as (
        select 
        id_paredao,
        edicao,
        REPLACE(semana, 'Sem ', 'Semana ') as Semana,
        dinamica,
        lider,
        anjo,
        poder_do_veto,
        poder_curinga,
        case
            when id_paredao = '8376e7332baa81af18156b24d2c51356' then 'Rafael'
            else big_fone
        end as big_fone,
        imunizado,
        veto,
        case
            when id_paredao = '03cf86eb2739b35eabcf531c66ddf6cf' then null
            when id_paredao = '32242ef3c0b288fa123610c6a1f81b37' then null
            when id_paredao = 'c9226d3e47fdf2aa2c95cc00b43096cb' then null
            when id_paredao = '7448ab3edde8fa94fe1598350a3a8528' then null
            when id_paredao = '26fc8ded2769d6aeb93e0c0ce87aeb7e' then null
            when id_paredao = 'ddec75beb2ca45d08e72ef41277ce72e' then null
            when id_paredao = '9d3100c5cddac8d16d48d34dbf64e738' then null
            when id_paredao = '9bd8e8393dce5b37e128f4eddd20740e' then null
            when id_paredao = '1b911ce9d447929ff180f54954d0800c' then null
            when id_paredao = '88e0549528abfe19e3e1760339b60b67' then null
            when id_paredao = '5643a2dc50deb4eb2d52cdcc1d69c21d' then null
            when id_paredao = '2007fd1a7206483bbd65fdc109f1d80a' then null
            when id_paredao = 'aa29788f025bf60311c248ffe78b4b42' then null
            when id_paredao = 'c1bea8c0d7df9160e93a9bc0d7154420' then null
            else indicado_lider
        end as indicado_lider,
        case
            when id_paredao = '03cf86eb2739b35eabcf531c66ddf6cf' then null
            when id_paredao = '32242ef3c0b288fa123610c6a1f81b37' then null
            when id_paredao = 'c9226d3e47fdf2aa2c95cc00b43096cb' then null
            when id_paredao = '7448ab3edde8fa94fe1598350a3a8528' then null
            when id_paredao = '26fc8ded2769d6aeb93e0c0ce87aeb7e' then null
            when id_paredao = 'ddec75beb2ca45d08e72ef41277ce72e' then null
            when id_paredao = '9d3100c5cddac8d16d48d34dbf64e738' then null
            when id_paredao = '1b911ce9d447929ff180f54954d0800c' then null
            when id_paredao = '88e0549528abfe19e3e1760339b60b67' then null
            when id_paredao = '5643a2dc50deb4eb2d52cdcc1d69c21d' then null
            when id_paredao = '2007fd1a7206483bbd65fdc109f1d80a' then null
            when id_paredao = 'aa29788f025bf60311c248ffe78b4b42' then null
            when id_paredao = '40b50e2a4ccd3f2ed3b9b6a19915dee3' then null
            when id_paredao = 'c1bea8c0d7df9160e93a9bc0d7154420' then null
            else indicado_big_fone
        end as indicado_big_fone,
        case
            when id_paredao = '03cf86eb2739b35eabcf531c66ddf6cf' then null
            when id_paredao = '32242ef3c0b288fa123610c6a1f81b37' then null
            when id_paredao = 'c9226d3e47fdf2aa2c95cc00b43096cb' then null
            when id_paredao = '7448ab3edde8fa94fe1598350a3a8528' then null
            when id_paredao = '26fc8ded2769d6aeb93e0c0ce87aeb7e' then null
            when id_paredao = 'ddec75beb2ca45d08e72ef41277ce72e' then null
            when id_paredao = '9d3100c5cddac8d16d48d34dbf64e738' then null
            when id_paredao = '9bd8e8393dce5b37e128f4eddd20740e' then null
            when id_paredao = '1b911ce9d447929ff180f54954d0800c' then null
            when id_paredao = '88e0549528abfe19e3e1760339b60b67' then null
            when id_paredao = '5643a2dc50deb4eb2d52cdcc1d69c21d' then null
            when id_paredao = '2007fd1a7206483bbd65fdc109f1d80a' then null
            when id_paredao = 'aa29788f025bf60311c248ffe78b4b42' then null
            when id_paredao = 'c1bea8c0d7df9160e93a9bc0d7154420' then null
            else indicado_casa
        end as indicado_casa,
        case
            when id_paredao = '03cf86eb2739b35eabcf531c66ddf6cf' then null
            when id_paredao = '32242ef3c0b288fa123610c6a1f81b37' then null
            when id_paredao = 'c9226d3e47fdf2aa2c95cc00b43096cb' then null
            when id_paredao = '7448ab3edde8fa94fe1598350a3a8528' then null
            when id_paredao = '26fc8ded2769d6aeb93e0c0ce87aeb7e' then null
            when id_paredao = 'ddec75beb2ca45d08e72ef41277ce72e' then null
            when id_paredao = '9d3100c5cddac8d16d48d34dbf64e738' then null
            when id_paredao = '1b911ce9d447929ff180f54954d0800c' then null
            else indicado_contragolpe
        end as indicado_contragolpe,
        case
            when id_paredao = '03cf86eb2739b35eabcf531c66ddf6cf' then 'Fred Nicacio Marilia'
            when id_paredao = '40b50e2a4ccd3f2ed3b9b6a19915dee3' then 'Angelica'
            else null
        end as indicado_quarto_especial,
        case
            when id_paredao = '9bd8e8393dce5b37e128f4eddd20740e' then 'Airton Alan Pierre Flavia Juliana'
            else null
        end as indicado_prova_especial,
        case
            when id_paredao = '9d3100c5cddac8d16d48d34dbf64e738' then 'Alan Carolina Diego Elana Gabriela Hana Hariany Isabella Maycon Rizia Rodrigo Tereza Vanderson Vinicius'
            when id_paredao = '2007fd1a7206483bbd65fdc109f1d80a' then 'Daniel Diana Rodrigao'
            else null
        end as indicado_super_paredao, -- when all but immune are nominated
        case
            when id_paredao = '1b911ce9d447929ff180f54954d0800c' then 'Hariany Tereza'
            else null
        end as indicado_paredao_interno,
        case
            when id_paredao = '32242ef3c0b288fa123610c6a1f81b37' then 'Alane Isabelle Matteus'
            when id_paredao = 'c9226d3e47fdf2aa2c95cc00b43096cb' then 'Aline Bruna Larissa'
            when id_paredao = '7448ab3edde8fa94fe1598350a3a8528' then 'Camilla Gilberto Juliette'
            when id_paredao = '26fc8ded2769d6aeb93e0c0ce87aeb7e' then 'Arthur Douglas Eliezer'
            when id_paredao = 'ddec75beb2ca45d08e72ef41277ce72e' then 'Babu Rafa Thelma'
            when id_paredao = '88e0549528abfe19e3e1760339b60b67' then 'Amanda Fernando'
            when id_paredao = '5643a2dc50deb4eb2d52cdcc1d69c21d' then 'Munik Ronan'
            when id_paredao = 'aa29788f025bf60311c248ffe78b4b42' then 'Diana Maria Wesley'
            when id_paredao = 'c1bea8c0d7df9160e93a9bc0d7154420' then 'Gyselle Natalia'
            else null
        end as indicado_prova_finalista,
        bate_e_volta_salvo
        from remove_invalid_nominations
    )

    SELECT * from fixed_nominations
    
