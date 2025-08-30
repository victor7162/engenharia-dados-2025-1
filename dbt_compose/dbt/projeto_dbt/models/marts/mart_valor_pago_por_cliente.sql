-- Modelo final que agrega o valor total pago por cada cliente

-- Etapa 1: Referenciar nossos modelos da camada Silver.

with pagamentos as (
    select * from {{ ref('stg_pagamentos') }}
),

pedidos as (
    select * from {{ ref('stg_pedidos') }}
),

clientes as (
    select * from {{ ref('stg_clientes') }}
),

-- Etapa 2: Juntar pagamentos com pedidos para descobrir qual cliente fez cada pagamento.
pagamentos_por_cliente as (
    select
        pedidos.id_cliente,
        pagamentos.id_pagamento,
        pagamentos.valor_pagamento
    from pagamentos
    left join pedidos on pagamentos.id_pedido = pedidos.id_pedido
),

-- Etapa 3: Agrupar tudo por cliente para calcular as métricas finais.
total_pago_por_cliente as (
    select
        id_cliente,
        sum(valor_pagamento) as total_pago,
        count(id_pagamento) as total_de_pagamentos
    from pagamentos_por_cliente
    group by 1 
)

-- Etapa 4: Juntar os dados agregados com os nomes dos clientes para o relatório final.
select
    clientes.id_cliente,
    clientes.primeiro_nome,
    clientes.ultimo_nome,
    total_pago_por_cliente.total_de_pagamentos,
    total_pago_por_cliente.total_pago
from total_pago_por_cliente
left join clientes on total_pago_por_cliente.id_cliente = clientes.id_cliente
order by total_pago_por_cliente.total_pago desc 