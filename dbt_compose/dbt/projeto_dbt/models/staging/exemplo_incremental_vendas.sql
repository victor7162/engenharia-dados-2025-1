{{
    config(
        materialized='incremental',
        unique_key='id_produto',
        incremental_strategy='merge',
        file_format='delta',
        schema='staging',
        alias='exemplo_incremental_vendas'
    )
}}

SELECT 
    ID_Produto AS id_produto,
    Nome_Produto AS nome_produto,
    Preco_Unitario AS preco,
    Quantidade_Vendida AS quantidade_vendida,
    Categoria AS categoria,
    ID_Cliente AS id_cliente,
    Data_Venda AS data_venda,
    Cidade_Cliente AS cidade_cliente,
    current_timestamp() AS data_atualizacao
FROM 
    {{ ref('exemplo_vendas') }}


{% if is_incremental() %}
WHERE
    data_atualizacao > (SELECT MAX(data_atualizacao) FROM {{ this }})
{% endif %}
