WITH merged_tables AS (
    {{ dbt_utils.union_relations(
        relations=[
            source('big-brother-brasil-454420', 'ratingsweekly10'),
            source('big-brother-brasil-454420', 'ratingsweekly11'),
            source('big-brother-brasil-454420', 'ratingsweekly12'),
            source('big-brother-brasil-454420', 'ratingsweekly15'),
            source('big-brother-brasil-454420', 'ratingsweekly16'),
            source('big-brother-brasil-454420', 'ratingsweekly17'),
            source('big-brother-brasil-454420', 'ratingsweekly18'),
            source('big-brother-brasil-454420', 'ratingsweekly19'),
            source('big-brother-brasil-454420', 'ratingsweekly20'),
            source('big-brother-brasil-454420', 'ratingsweekly21'),
            source('big-brother-brasil-454420', 'ratingsweekly22'),
            source('big-brother-brasil-454420', 'ratingsweekly23'),
            source('big-brother-brasil-454420', 'ratingsweekly24')
        ],
        source_column_name = None,
        exclude = ['int64_field_0']
    ) }}
)
SELECT * FROM merged_tables

-- Fix error