

select
    id as id_pedido,
    user_id as id_cliente,
    order_date as data_pedido,
    status
from {{ source('public', 'raw_pedidos') }}
where status = 'completed'