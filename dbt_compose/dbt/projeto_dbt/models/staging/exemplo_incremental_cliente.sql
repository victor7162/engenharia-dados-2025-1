{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='merge',
        file_format='delta',
        schema='staging',
        alias='exemplo_incremental_cliente'
    )
}}

SELECT 
    *,
    current_timestamp() AS data_atualizacao
FROM 
    {{ ref('exemplo_clientes') }}


{% if is_incremental() %}
WHERE
    data_atualizacao > (SELECT MAX(data_atualizacao) FROM {{ this }})
{% endif %}
