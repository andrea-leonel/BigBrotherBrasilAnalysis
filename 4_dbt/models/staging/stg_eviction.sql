WITH merged_tables AS (
    {{ dbt_utils.union_relations(
        relations=[
            source('big-brother-brasil-454420', 'evictionresults1'),
            source('big-brother-brasil-454420', 'evictionresults2'),
            source('big-brother-brasil-454420', 'evictionresults13'),
            source('big-brother-brasil-454420', 'evictionresults14'),
            source('big-brother-brasil-454420', 'evictionresults15'),
            source('big-brother-brasil-454420', 'evictionresults16'),
            source('big-brother-brasil-454420', 'evictionresults17'),
            source('big-brother-brasil-454420', 'evictionresults18'),
            source('big-brother-brasil-454420', 'evictionresults19'),
            source('big-brother-brasil-454420', 'evictionresults10'),
            source('big-brother-brasil-454420', 'evictionresults11'),
            source('big-brother-brasil-454420', 'evictionresults12'),
            source('big-brother-brasil-454420', 'evictionresults13'),
            source('big-brother-brasil-454420', 'evictionresults14'),
            source('big-brother-brasil-454420', 'evictionresults15'),
            source('big-brother-brasil-454420', 'evictionresults16'),
            source('big-brother-brasil-454420', 'evictionresults17'),
            source('big-brother-brasil-454420', 'evictionresults18'),
            source('big-brother-brasil-454420', 'evictionresults19'),
            source('big-brother-brasil-454420', 'evictionresults20'),
            source('big-brother-brasil-454420', 'evictionresults21'),
            source('big-brother-brasil-454420', 'evictionresults22'),
            source('big-brother-brasil-454420', 'evictionresults23'),
            source('big-brother-brasil-454420', 'evictionresults24'),
            source('big-brother-brasil-454420', 'evictionresults25')
        ],
        source_column_name = None,
        exclude = ['int64_field_0','Notas']
    ) }}
)
SELECT * FROM merged_tables

--create unique identifier Semana+dinamica+edicao, delete paredao, filter out finals (another table), remove duplicate tables, make decision about duos.