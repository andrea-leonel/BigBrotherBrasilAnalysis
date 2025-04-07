WITH merged_tables AS (
    {{ dbt_utils.union_relations(
        relations=[
            source('big-brother-brasil-454420', 'nominations1'),
            source('big-brother-brasil-454420', 'nominations2'),
            source('big-brother-brasil-454420', 'nominations3'),
            source('big-brother-brasil-454420', 'nominations4'),
            source('big-brother-brasil-454420', 'nominations5'),
            source('big-brother-brasil-454420', 'nominations6'),
            source('big-brother-brasil-454420', 'nominations7'),
            source('big-brother-brasil-454420', 'nominations8'),
            source('big-brother-brasil-454420', 'nominations9'),
            source('big-brother-brasil-454420', 'nominations10'),
            source('big-brother-brasil-454420', 'nominations11'),
            source('big-brother-brasil-454420', 'nominations12'),
            source('big-brother-brasil-454420', 'nominations13'),
            source('big-brother-brasil-454420', 'nominations14'),
            source('big-brother-brasil-454420', 'nominations15'),
            source('big-brother-brasil-454420', 'nominations16'),
            source('big-brother-brasil-454420', 'nominations17'),
            source('big-brother-brasil-454420', 'nominations18'),
            source('big-brother-brasil-454420', 'nominations19'),
            source('big-brother-brasil-454420', 'nominations20'),
            source('big-brother-brasil-454420', 'nominations21'),
            source('big-brother-brasil-454420', 'nominations22'),
            source('big-brother-brasil-454420', 'nominations23'),
            source('big-brother-brasil-454420', 'nominations24'),
            source('big-brother-brasil-454420', 'nominations25')
        ],
        source_column_name = None,
        exclude = ['int64_field_0']
    ) }}
)
SELECT * FROM merged_tables

-- There are Indicado_lider without a Lider - check, decide what to do with duos, unique identifies semana+dinamica+edicao, remove bate e volta indicados,