with 

source as (

    select * from {{ source('staging', 'Contestants') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['edicao', 'data_nascimento_formatado','primeiro_nome']) }} AS id_participante,
        nome AS nome_completo,
        INITCAP(primeiro_nome) AS primeiro_nome,
        genero,
        data_nascimento,
        {{ parse_date_macro('data_nascimento') }} AS data_nascimento_formatado,
        (ano_edicao - ano_nascimento) AS idade_participacao,
        nacionalidade,
        estado,
        cidade,
        profissao,
        {{ parse_date_macro('data_resultado') }} AS ultimo_dia,
        edicao,
        ano_edicao
    from source

)

select * from renamed
