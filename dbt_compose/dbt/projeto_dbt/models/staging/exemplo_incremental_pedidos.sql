{{
    config(
        schema='staging',
        alias='exemplo_incremental_pedidos'
    )
}}


with source as (

    select * from {{ ref('raw_pedidos') }}

),

renamed as (

    select
        id as id_pedido,
        user_id as id_cliente,
        order_date as data_pedido,
        status,
        current_timestamp AS data_atualizacao

    from source

)

select * from renamed


{% if is_incremental() %}
WHERE
    data_atualizacao > (SELECT MAX(data_atualizacao) FROM {{ this }})
{% endif %}
