with
    merged_tables as (
        {{
            dbt_utils.union_relations(
                relations=[
                    source("big-brother-brasil-454420", "ratingsweekly10"),
                    source("big-brother-brasil-454420", "ratingsweekly11"),
                    source("big-brother-brasil-454420", "ratingsweekly12"),
                    source("big-brother-brasil-454420", "ratingsweekly15"),
                    source("big-brother-brasil-454420", "ratingsweekly16"),
                    source("big-brother-brasil-454420", "ratingsweekly17"),
                    source("big-brother-brasil-454420", "ratingsweekly18"),
                    source("big-brother-brasil-454420", "ratingsweekly19"),
                    source("big-brother-brasil-454420", "ratingsweekly20"),
                    source("big-brother-brasil-454420", "ratingsweekly21"),
                    source("big-brother-brasil-454420", "ratingsweekly22"),
                    source("big-brother-brasil-454420", "ratingsweekly23"),
                    source("big-brother-brasil-454420", "ratingsweekly24"),
                    source("big-brother-brasil-454420", "ratingsweekly25")
                ],
                source_column_name=None,
                column_override={"TER": "FLOAT64"},
                exclude=["int64_field_0"],
            )
        }}
    )

select *
from merged_tables