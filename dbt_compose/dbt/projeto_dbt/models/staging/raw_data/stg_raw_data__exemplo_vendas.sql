with source as (

    select * from {{ source('raw_data', 'exemplo_vendas_raw') }}

),

renamed as (

    select
        id_pedido,
        id_cliente,
        data_venda,
        produto,
        quantidade,
        valor_unitario,
        valor_total

    from source

)

select * from renamed
