with 

source as (

    select * from {{ source('big-brother-brasil-454420', 'nominations20') }}

),

renamed as (

    select
        {{ break_week('semana', 'dinamica')}},
        lider,
        indicado__lider_,
        indicado__casa_,
        edicao,
        {{ normalise_columns('nominations20') }}

    from source

)

select * from renamed
