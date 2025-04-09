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

    renamed as (
        select
            edicao,
            semana,
            case
                when dinamica = 'Puxadinho.1' then 'Puxadinho' else dinamica
            end as dinamica,
            desistente,
            expulso as expulso_1,
            expulso0 as expulso_2,
            expulso1 as expulso_3,
            berlinda__b__paredao as paredao_1,
            paredao as paredao_2,
            paredao0 as paredao_3,
            paredao1 as paredao_4,
            eliminado as eliminado_1,
            eliminado0 as eliminado_2,
            eliminado1 as eliminado_3,
            eliminado2 as eliminado_4,
            eliminado3 as eliminado_5,
            porcent_outros_ as porcent_outros_1,
            porcent_outros_0 as porcent_outros_2,
            porcent_outros_1 as porcent_outros_3,
            porcent_outros_2 as porcent_outros_4,
            porcent_outros_3 as porcent_outros_5,
            porcent_outros_4 as porcent_outros_6,
            porcent_outros_5 as porcent_outros_7,
            porcent_outros_6 as porcent_outros_8,
            porcent_outros_7 as porcent_outros_9,
            porcent_outros_8 as porcent_outros_10,
            porcent_outros_9 as porcent_outros_11,
            porcent_outros_10 as porcent_outros_12,
            porcent_outros_11 as porcent_outros_13,
            porcent_outros_12 as porcent_outros_14,
            votos
        from merged_tables
    )

select *
from renamed
