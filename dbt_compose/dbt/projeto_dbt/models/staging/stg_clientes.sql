

select
    id as id_cliente,
    first_name as primeiro_nome,
    last_name as ultimo_nome
from {{ source('public', 'raw_clientes') }}