with 

source as (

    select * from {{ source('staging', 'ranking') }}

),

renamed as (

    select
        pos_ AS posicao_final,
        participante,
        SPLIT(participante, ' ')[SAFE_OFFSET(0)] AS nome_participante,
        REVERSE(SPLIT(REVERSE(participante), ' ')[SAFE_OFFSET(0)]) AS sobrenome_participante,
        meio_de_indicacao AS meio_de_indicacao_raw,
        CASE
            WHEN meio_de_indicacao = 'Disputa pelas ultimas vagas' THEN 'Dinamica inicial de entrada'
            WHEN meio_de_indicacao = 'Disputa pela ultima vaga' THEN 'Dinamica inicial de entrada'
            WHEN meio_de_indicacao = 'Consequencia Surpresa do Lider' THEN 'Consequencia Lider'
            WHEN meio_de_indicacao = 'Expulsa' THEN 'Expulso'
            WHEN meio_de_indicacao = 'Finalistas' THEN 'Finalista'
            WHEN meio_de_indicacao = 'Lider / Paredao' THEN 'Lider'
            WHEN meio_de_indicacao = 'Lideres' THEN 'Lider'
            WHEN meio_de_indicacao = 'Paredao "Vai e Volta"' THEN 'Participante que retornou de Paredao Falso'

        __dos_votos,
        eliminado_em,
        edicao,
        indicado_por

    from source

)

select * from renamed
