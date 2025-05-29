            sql
            -- models/staging/stg_vendas.sql
            with source_data as (
                -- Referencia a tabela de origem definida em bronze_sources.yml
                -- select * from {{ ('raw_data_source', 'exemplo_vendas_raw') }}
                select * from {{ref('exemplo_vendas') }}
                -- Se voce nao definiu a source, e o seed criou a tabela 'exemplo_vendas_raw'
                -- no schema padrao, voce poderia usar {{ ref('exemplo_vendas_raw') }}
                -- mas usar 'source' e a melhor pratica para dados brutos.
            )

            select
                -- Convertendo tipos e tratando valores
                cast(ID_Produto as varchar) as id_produto, -- Mantendo como varchar por enquanto
                trim(lower(Nome_Produto)) as nome_produto,
                trim(lower(Categoria)) as categoria,
                
                -- Tratamento de Preco_Unitario (lidar com string vazia e converter para numerico)
                case 
                    when Preco_Unitario is null or trim(Preco_Unitario) = '' then null
                    else cast(replace(Preco_Unitario, ',', '.') as double)
                end as preco_unitario,
                
                -- Tratamento de Quantidade_Vendida (lidar com string vazia, NULL e negativos)
                case
                    when Quantidade_Vendida is null or trim(Quantidade_Vendida) = '' or upper(trim(Quantidade_Vendida)) = 'NULL' then 0
                    when cast(Quantidade_Vendida as integer) < 0 then 0
                    else cast(Quantidade_Vendida as integer)
                end as quantidade_vendida,
                
                -- Convertendo Data_Venda para tipo DATE
                -- DuckDB e flexivel com formatos de data, mas para outros DBs pode precisar de to_date com formato
                cast(Data_Venda as date) as data_venda,
                
                cast(ID_Cliente as varchar) as id_cliente,
                trim(lower(Cidade_Cliente)) as cidade_cliente
            from source_data
            