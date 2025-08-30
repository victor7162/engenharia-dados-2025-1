

select
    id as id_pagamento,
    order_id as id_pedido,
    payment_method as metodo_pagamento,
    -- Converte o valor de centavos para a unidade monetária
    amount / 100.0 as valor_pagamento
from {{ source('public', 'raw_pagamentos') }}