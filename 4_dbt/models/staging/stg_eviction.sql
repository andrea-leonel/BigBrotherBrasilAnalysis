with
    merged_tables as (
        {{
            dbt_utils.union_relations(
                relations=[
                    source("big-brother-brasil-454420", "evictionresults1"),
                    source("big-brother-brasil-454420", "evictionresults2"),
                    source("big-brother-brasil-454420", "evictionresults13"),
                    source("big-brother-brasil-454420", "evictionresults14"),
                    source("big-brother-brasil-454420", "evictionresults15"),
                    source("big-brother-brasil-454420", "evictionresults16"),
                    source("big-brother-brasil-454420", "evictionresults17"),
                    source("big-brother-brasil-454420", "evictionresults18"),
                    source("big-brother-brasil-454420", "evictionresults19"),
                    source("big-brother-brasil-454420", "evictionresults10"),
                    source("big-brother-brasil-454420", "evictionresults11"),
                    source("big-brother-brasil-454420", "evictionresults12"),
                    source("big-brother-brasil-454420", "evictionresults13"),
                    source("big-brother-brasil-454420", "evictionresults14"),
                    source("big-brother-brasil-454420", "evictionresults15"),
                    source("big-brother-brasil-454420", "evictionresults16"),
                    source("big-brother-brasil-454420", "evictionresults17"),
                    source("big-brother-brasil-454420", "evictionresults18"),
                    source("big-brother-brasil-454420", "evictionresults19"),
                    source("big-brother-brasil-454420", "evictionresults20"),
                    source("big-brother-brasil-454420", "evictionresults21"),
                    source("big-brother-brasil-454420", "evictionresults22"),
                    source("big-brother-brasil-454420", "evictionresults23"),
                    source("big-brother-brasil-454420", "evictionresults24"),
                    source("big-brother-brasil-454420", "evictionresults25"),
                ],
                source_column_name=None,
                exclude=["int64_field_0", "Notas"],
            )
        }}
    ),

    remove_finals as (select * from merged_tables where not dinamica = 'Final'),

    consolidate_columns as (
        select
            {{ dbt_utils.generate_surrogate_key(["edicao", "semana", "dinamica"]) }}
            as id_paredao,
            edicao,
            semana,
            dinamica,
            coalesce(concat(eliminado, ' | '), '') as eliminado_1,
            coalesce(concat(eliminado_5, ' | '), '') as eliminado_2,
            coalesce(concat(eliminado_6, ' | '), '') as eliminado_3,
            coalesce(concat(eliminado_7, ' | '), '') as eliminado_4,
            coalesce(concat(eliminado_8, ' | '), '') as eliminado_5,
            coalesce(concat(eliminado_9, ' | '), '') as eliminado_6,
            coalesce(concat(expulso, ' | '), '') as expulso_1,
            coalesce(concat(expulso_5, ' | '), '') as expulso_2,
            coalesce(concat(outras_porcent, ' | '), '') as porcent_outros_1,
            coalesce(concat(outras_porcent_10, ' | '), '') as porcent_outros_2,
            coalesce(concat(outras_porcent_11, ' | '), '') as porcent_outros_3,
            coalesce(concat(outras_porcent_12, ' | '), '') as porcent_outros_4,
            coalesce(concat(outras_porcent_13, ' | '), '') as porcent_outros_5,
            coalesce(concat(outras_porcent_14, ' | '), '') as porcent_outros_6,
            coalesce(concat(outras_porcent_15, ' | '), '') as porcent_outros_7,
            coalesce(concat(outras_porcent_16, ' | '), '') as porcent_outros_8,
            coalesce(concat(outras_porcent_17, ' | '), '') as porcent_outros_9,
            coalesce(concat(outras_porcent_18, ' | '), '') as porcent_outros_10,
            coalesce(concat(outras_porcent_19, ' | '), '') as porcent_outros_11,
            coalesce(concat(outras_porcent_20, ' | '), '') as porcent_outros_12,
            coalesce(concat(outras_porcent_21, ' | '), '') as porcent_outros_13,
            coalesce(concat(outras_porcent_7, ' | '), '') as porcent_outros_14,
            coalesce(concat(outras_porcent_8, ' | '), '') as porcent_outros_15,
            coalesce(concat(outras_porcent_9, ' | '), '') as porcent_outros_16,
            coalesce(concat(outras_porcent_ou_pontos, ' | '), '') as porcent_outros_17,
            coalesce(concat(outras_porcent_ou_pontos_10, ' | '), '') as porcent_outros_18,
            coalesce(concat(outras_porcent_ou_votos, ' | '), '') as porcent_outros_19,
            coalesce(concat(outras_porcent_ou_votos_10, ' | '), '') as porcent_outros_20,
            coalesce(concat(outras_porcent_ou_votos_11, ' | '), '') as porcent_outros_21,
            coalesce(concat(outras_porcent_ou_votos_12, ' | '), '') as porcent_outros_22,
            coalesce(concat(outras_porcent_ou_votos_13, ' | '), '') as porcent_outros_23,
            coalesce(concat(outras_porcent_ou_votos_14, ' | '), '') as porcent_outros_24,
            coalesce(concat(outras_porcent_ou_votos_15, ' | '), '') as porcent_outros_25,
            coalesce(concat(outras_porcent_ou_votos_16, ' | '), '') as porcent_outros_26,
            coalesce(concat(outras_porcent_ou_votos_17, ' | '), '') as porcent_outros_27,
            coalesce(concat(outras_porcent_ou_votos_18, ' | '), '') as porcent_outros_28,
            coalesce(concat(outras_porcent_ou_votos_19, ' | '), '') as porcent_outros_29,
            coalesce(concat(outras_porcent_ou_votos_20, ' | '), '') as porcent_outros_30,
            coalesce(concat(outras_porcent_ou_votos_8, ' | '), '') as porcent_outros_31,
            coalesce(concat(outras_porcent_ou_votos_9, ' | '), '') as porcent_outros_32,
            coalesce(concat(paredao, ' | '), '') as paredao_1,
            coalesce(concat(paredao_4, ' | '), '') as paredao_2,
            coalesce(concat(paredao_5, ' | '), '') as paredao_3,
            coalesce(concat(paredao_7, ' | '), '') as paredao_4,
            votos
        from remove_finals
    ),

    consolidate_columns_2 as (
        select
            id_paredao,
            edicao,
            semana,
            dinamica,
            CONCAT(eliminado_1,eliminado_2,eliminado_3,eliminado_4,eliminado_5,eliminado_6) as eliminado_cons,
            CONCAT(expulso_1,expulso_2) as expulso_cons,
            CONCAT(porcent_outros_1,porcent_outros_2,porcent_outros_3,porcent_outros_4,porcent_outros_5,porcent_outros_6,porcent_outros_7,porcent_outros_8,porcent_outros_9,porcent_outros_10,porcent_outros_11,porcent_outros_12,porcent_outros_13,porcent_outros_14,porcent_outros_15,porcent_outros_16,porcent_outros_17,porcent_outros_18,porcent_outros_19,porcent_outros_20,porcent_outros_21,porcent_outros_22,porcent_outros_23,porcent_outros_24,porcent_outros_25,porcent_outros_26,porcent_outros_27,porcent_outros_28,porcent_outros_29,porcent_outros_30,porcent_outros_31,porcent_outros_32) as outras_porcent_cons,
            CONCAT(paredao_1,paredao_2,paredao_3,paredao_4) as paredao_cons,
            votos
        from consolidate_columns
    )

select *
from consolidate_columns_2
