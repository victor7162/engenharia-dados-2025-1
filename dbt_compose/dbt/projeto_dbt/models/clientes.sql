with clientes as (

    select * from {{ ref('exemplo_incremental_clientes') }}

),

pedidos as (

    select * from {{ ref('exemplo_incremental_pedidos') }}

),

pagamentos as (

    select * from {{ ref('exemplo_incremental_pagamentos') }}

),

pedidos_clientes as (

        select
        id_cliente,
        min(data_pedido) as primeiro_pedido,
        max(data_pedido) as pedido_mais_recente
    from pedidos

    group by id_cliente

),

pagamentos_clientes as (

    select
        pedidos.id_cliente,
        sum(valor_pagamento) as pagamento_total

    from pagamentos

    left join pedidos on
         pagamentos.id_pedido = pedidos.id_pedido

    group by pedidos.id_cliente

),

final as (

    select
        clientes.id_cliente,
        clientes.nome_completo,
        pedidos_clientes.primeiro_pedido,
        pedidos_clientes.pedido_mais_recente,
        pagamentos_clientes.pagamento_total
    from clientes

    left join pedidos_clientes
        on clientes.id_cliente = pedidos_clientes.id_cliente
    left join pagamentos_clientes
        on clientes.id_cliente = pagamentos_clientes.id_cliente 

)

select * from final where primeiro_pedido is not null
