{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='merge',
        file_format='delta',
        schema='staging',
        alias='exemplo_incremental_clientes'
    )
}}

with source as (
    select 
    id AS id_cliente,
    concat(first_name, ' ', last_name) AS nome_completo,
    current_timestamp() AS data_atualizacao
    from {{ ref('raw_clientes') }}
),

-- Padronização dos nomes das colunas 

renamed as (
    select 
        id_cliente,
        nome_completo,
        data_atualizacao
    from source
)

select * from renamed

{% if is_incremental() %}
WHERE
    data_atualizacao > (SELECT MAX(data_atualizacao) FROM {{ this }})
{% endif %}
