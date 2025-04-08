with

    source as (select * from {{ source("big-brother-brasil-454420", "Contestants") }}),

    date_mod as (

        select
            nome as nome_completo,
            initcap(primeiro_nome) as primeiro_nome,
            genero,
            case
                when strpos(data_nascimento, ' - ') > 0
                then regexp_extract(data_nascimento, r'^(.*?) - ')
                else data_nascimento
            end as data_nasc,
            regexp_extract(data_nascimento, r' - (.*)$') as data_falecimento,
            nacionalidade,
            estado,
            cidade,
            profissao,
            ano_edicao,
            edicao,
            case when data_resultado = 'Em andamento' then null else data_resultado end as data_resultado
        from source

    )

select 
{{ dbt_utils.generate_surrogate_key(['edicao','primeiro_nome']) }} AS id_participante,
nome_completo,
primeiro_nome,
genero,
{{ parse_date('data_nasc')}} AS data_nascimento,
safe_subtract(ano_edicao,CAST(RIGHT(data_nasc,4) AS INT)) AS idade_participacao,
{{ parse_date('data_falecimento')}} AS data_falecimento,
nacionalidade,
estado,
cidade,
profissao,
ano_edicao,
edicao,
{{ parse_date('data_resultado')}} AS ultimo_dia,
from date_mod
