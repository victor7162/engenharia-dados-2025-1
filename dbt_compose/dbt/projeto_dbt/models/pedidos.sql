{% set metodos_pagamento = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}

with pedidos as (

    select * from {{ ref('exemplo_incremental_pedidos') }}

),

pagamentos as (

    select * from {{ ref('exemplo_incremental_pagamentos') }}

),

pagamentos_pedidos as (

    select
        id_pedido,

        {% for metodo_pagamento in metodos_pagamento -%}
        sum(case when metodo_pagamento = '{{ payment_method }}' then valor_pagamento else 0 end) as {{ metodo_pagamento }}_valor_pagamento,
        {% endfor -%}

        sum(valor_pagamento) as pagamento_total

    from pagamentos

    group by id_pedido

),

final as (

    select
        pedidos.id_pedido,
        pedidos.id_cliente,
        pedidos.data_pedido,
        pedidos.status,

        {% for metodo_pagamento in metodos_pagamento -%}

        pagamentos_pedidos.{{ metodo_pagamento }}_valor_pagamento,

        {% endfor -%}

        pagamentos_pedidos.pagamento_total as valor 

    from pedidos


    left join pagamentos_pedidos
        on pedidos.order_id = pagamentos_pedidos.order_id

)

select * from final
