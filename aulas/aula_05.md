# Atividade instalação dbt: 
## Iniciando um Projeto dbt e Conectando a uma Fonte

*   **Objetivo:** Familiarizar-se com a estrutura de um projeto dbt, configurar uma conexão e criar um primeiro modelo simples.
*   **Formato:** Exercício prático individual, guiado.
*   **Pré-requisitos:** dbt instalado, acesso a um banco de dados (pode ser DuckDB local para simplicidade, ou PostgreSQL/SQLite).
*   **Tarefa:**
    1.  **Instalar o adaptador dbt:**
        *   Se for usar PostgreSQL: `pip install dbt-postgres`.
    2.  **Inicializar um projeto dbt:**
        *   No terminal, navegue até um diretório de sua escolha e execute `dbt init meu_projeto_dbt`.
        *   Siga as instruções, fornecendo um nome para o projeto.
        *   Escolha o tipo de banco de dados que você vai usar (e.g., duckdb, postgresql).
        *   Configure o arquivo `profiles.yml` (geralmente localizado em `~/.dbt/profiles.yml` ou dentro do projeto se você escolher essa opção durante o `dbt init`).
            *   **Para PostgreSQL:** Forneça host, port, user, password, dbname.
    3.  **Explorar a estrutura do projeto:**
        *   Abra o diretório `meu_projeto_dbt` em um editor de código.
        *   Identifique os diretórios `models`, `seeds`, `tests`, `macros` e o arquivo `dbt_project.yml`.
    4.  **Criar um arquivo `seed` (dados de exemplo):**
        *   No diretório `seeds`, crie um arquivo `exemplo_clientes.csv` com alguns dados de clientes (e.g., id, nome, email).
        *   ```csv
            id,nome,email
            1,Alice,alice@example.com
            2,Bob,bob@example.com
            3,Charlie,charlie@example.com
            ```
    5.  **Carregar o `seed` no banco de dados:**
        *   No terminal, dentro do diretório do projeto dbt, execute `dbt seed`.
        *   Verifique no seu banco de dados se uma tabela `exemplo_clientes` foi criada com os dados do CSV.
    6.  **Criar um primeiro modelo:**
        *   No diretório `models/example` (ou crie um novo subdiretório, e.g., `models/bronze`), crie um arquivo `stg_clientes.sql`.
        *   Neste arquivo, escreva uma consulta SQL simples para selecionar todos os dados da tabela `exemplo_clientes` criada pelo `seed`:
            ```sql
            select
                id,
                nome,
                email
            from {{ source('default', 'exemplo_clientes') }} -- ou {{ ref('exemplo_clientes') }} se o dbt tratar seeds como refs automaticamente
            -- Nota: A forma de referenciar seeds pode variar. Se `source` não funcionar, tente `ref`.
            -- Para usar `source`, você precisaria definir 'exemplo_clientes' em um arquivo .yml na pasta models.
            -- Alternativamente, para simplificar esta primeira atividade, pode-se usar diretamente o nome da tabela gerada pelo seed se o dbt a criar no schema padrão.
            -- Uma forma mais robusta para referenciar o seed é: {{ ref('exemplo_clientes') }}
            ```
            *Corrigindo para a forma mais comum de referenciar um seed:* 
            ```sql
            select
                id as cliente_id,
                nome as nome_cliente,
                lower(email) as email_cliente -- Exemplo de transformação simples
            from {{ ref('exemplo_clientes') }}
            ```
    7.  **Executar o modelo:**
        *   No terminal, execute `dbt run --select stg_clientes`.
        *   Verifique no seu banco de dados se uma nova tabela (ou view, dependendo da materialização padrão) chamada `stg_clientes` foi criada.
    8.  **Adicionar um teste genérico:**
        *   No diretório `models/example` (ou onde está `stg_clientes.sql`), crie/edite um arquivo `schema.yml` (ou `models.yml`).
        *   ```yaml
version: 2

models:
  - name: stg_clientes
    columns:
      - name: cliente_id
        tests:
          - unique
          - not_null
```
    9.  **Executar os testes:**
        *   No terminal, execute `dbt test`.
        *   Verifique se os testes passam.


*   **Passos da Tarefa: Criando os primeiros modelos dbt**

    1.  **Explorar a Estrutura do Projeto (5 minutos):**
        *   Abra o diretório do seu projeto dbt (e.g., `meu_primeiro_projeto_dbt`) em um editor de código (como VS Code).
        *   Identifique os diretórios: `models/`, `seeds/`, `tests/`, `macros/`.
        *   Abra e inspecione o arquivo `dbt_project.yml`.

    2.  **Criar um Arquivo `seed` com Dados de Exemplo (10 minutos):**
        *   No diretório `seeds/` do seu projeto, crie um novo arquivo chamado `exemplo_vendas_raw.csv`.
        *   Copie e cole os seguintes dados CSV neste arquivo (estes são os dados do arquivo `/home/ubuntu/exemplo_vendas.csv` que preparamos anteriormente):
            ```csv
            ID_Produto,Nome_Produto,Categoria,Preco_Unitario,Quantidade_Vendida,Data_Venda,ID_Cliente,Cidade_Cliente
            101,Laptop Modelo X,Eletronicos,3500.00,2,2023-10-15,C001,Sao Paulo
            102,Smartphone Y,Eletronicos,1200.50,5,2023-10-15,C002,Rio de Janeiro
            201,Livro de Ficcao,Livros,45.90,10,2023-10-16,C003,Belo Horizonte
            101,Laptop Modelo X,Eletronicos,3500.00,1,2023-10-17,C004,Sao Paulo
            301,Cafe Especial,Alimentos,25.00,20,2023-10-17,C001,Sao Paulo
            202,Livro Tecnico,Livros,120.00,3,2023-10-18,C005,Curitiba
            102,Smartphone Y,Eletronicos,1150.00,3,2023-10-18,C002,Rio de Janeiro
            401,Camiseta Estampada,Vestuario,79.90,7,2023-10-19,C006,Recife
            201,Livro de Ficcao,Livros,42.50,5,2023-10-19,C003,Belo Horizonte
            302,Cha Importado,Alimentos,,12,2023-10-20,C007,Porto Alegre
            103,Tablet Z,Eletronicos,899.90,2,2023-10-20,C001,Sao Paulo
            103,Tablet Z,Eletronicos,899.90,2,2023-10-20,C001,Sao Paulo
            501,Fone de Ouvido BT,Acessorios,299.00,0,2023-10-21,C008,Salvador
            601,Vinho Tinto Seco,Bebidas,65.75,8,2023-10-22,C009,Brasilia
            203,HQ Edicao de Colecionador,Livros,199.50,-1,2023-10-23,C010,Manaus
            701,Monitor Gamer 27,Eletronicos,1850.00,3,2023-10-24,C002,Rio de Janeiro
            301,Cafe Especial,Alimentos,26.50,15,2023-10-25,C004,Sao Paulo
            402,Calca Jeans,Vestuario,150.00,4,2023-10-26,C005,Curitiba
            101,  Laptop Modelo X  ,eletronicos,3450.00,1,2023-10-27,C011,Goiania
            801,Teclado Mecanico,Acessorios,450.00,NULL,2023-10-28,C012,Fortaleza
            ```
        *   **Carregar o `seed` no DuckDB:** No terminal, dentro do diretório do seu projeto dbt, execute:
            `dbt seed`
        *   Este comando lerá o arquivo `exemplo_vendas_raw.csv` e criará uma tabela chamada `exemplo_vendas_raw` no seu banco de dados DuckDB (`meu_banco_dbt.duckdb`) com esses dados. O dbt infere os tipos de dados das colunas do CSV.

    3.  **Definir a Fonte de Dados Brutos (Opcional, mas boa prática) (5 minutos):**
        *   No diretório `models/`, crie um subdiretório chamado `staging` (ou `bronze`).
        *   Dentro de `models/staging/`, crie um arquivo YAML chamado `bronze_sources.yml` (ou `schema.yml` se preferir manter tudo em um só lugar, mas separar por propósito pode ser mais organizado).
        *   Adicione o seguinte conteúdo para definir a tabela `exemplo_vendas_raw` (que foi criada pelo `dbt seed`) como uma fonte:
            ```yaml
            # models/staging/bronze_sources.yml
            version: 2

            sources:
              - name: raw_data_source # Um nome logico para o grupo de fontes
                description: "Fonte de dados brutos carregados via seed ou ingestao externa."
                # Opcional: especifique o schema e database se nao for o padrao do seu target
                # database: meu_banco_dbt # Para DuckDB, o path e o database
                # schema: main # DuckDB usa 'main' como schema padrao
                tables:
                  - name: exemplo_vendas_raw # O nome da tabela como foi criada pelo dbt seed
                    description: "Tabela de vendas brutas do arquivo CSV."
                    # Voce pode adicionar testes ou descricoes de colunas aqui tambem
                    columns:
                      - name: ID_Produto
                        description: "ID do produto da venda."
            ```
            *Nota:* O dbt, ao executar `dbt seed`, geralmente cria a tabela no schema padrão do seu target. Para DuckDB, se você não especificar um schema no `profiles.yml`, ele usará `main`. A definição de `source` ajuda a documentar e referenciar essas tabelas de forma mais robusta.

    4.  **Criar um Primeiro Modelo de Staging (15 minutos):**
        *   No diretório `models/staging/`, crie um arquivo SQL chamado `stg_vendas.sql`.
        *   Neste arquivo, vamos escrever uma consulta SQL que seleciona dados da nossa fonte `exemplo_vendas_raw` e faz algumas transformações básicas (como renomear colunas, converter tipos, limpeza básica). Este será nosso primeiro passo para a camada Silver.
            ```sql
            -- models/staging/stg_vendas.sql
            with source_data as (
                -- Referencia a tabela de origem definida em bronze_sources.yml
                select * from {{ source('raw_data_source', 'exemplo_vendas_raw') }}
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
            ```
        *   **Executar o modelo:** No terminal, dentro do diretório do projeto dbt, execute:
            `dbt run --select stg_vendas`
        *   Este comando irá:
            1.  Compilar o SQL do modelo `stg_vendas.sql` (resolvendo a referência `source`).
            2.  Executar o SQL compilado no DuckDB, criando uma nova tabela (ou view, por padrão o dbt cria views se não especificado de outra forma) chamada `stg_vendas` no seu schema de desenvolvimento.
        *   Você pode verificar se a tabela/view foi criada usando uma ferramenta de SQL para DuckDB ou adicionando um `LIMIT 10` ao final do seu modelo e usando `dbt show --select stg_vendas` (embora `dbt show` seja mais para inspecionar resultados de testes ou modelos específicos).

    5.  **Adicionar Testes Genéricos ao Modelo (10 minutos):**
        *   No diretório `models/staging/`, crie (ou edite, se já existir um `schema.yml` genérico) um arquivo chamado `stg_vendas_schema.yml` (ou adicione a configuração abaixo a um `schema.yml` existente).
        *   Adicione o seguinte conteúdo para definir testes para o modelo `stg_vendas`:
            ```yaml
            # models/staging/stg_vendas_schema.yml
            version: 2

            models:
              - name: stg_vendas # O nome do seu modelo (arquivo stg_vendas.sql)
                description: "Modelo de staging para dados de vendas. Inclui limpeza basica, normalizacao de strings e conversao de tipos. Representa a primeira transformacao para a camada Silver."
                columns:
                  - name: id_produto
                    description: "Identificador unico do produto."
                    tests:
                      - not_null
                  - name: nome_produto
                    description: "Nome do produto normalizado."
                  - name: categoria
                    description: "Categoria do produto normalizada."
                  - name: preco_unitario
                    description: "Preco unitario do produto. Nulos sao mantidos, strings vazias viram nulo."
                    tests:
                      - dbt_utils.not_negative # Requer o pacote dbt-utils
                  - name: quantidade_vendida
                    description: "Quantidade vendida. Nulos, strings vazias ou 'NULL' viram 0. Negativos viram 0."
                    tests:
                      - not_null
                      - dbt_utils.not_negative # Requer o pacote dbt-utils
                  - name: data_venda
                    description: "Data da venda."
                    tests:
                      - not_null
                  - name: id_cliente
                    description: "Identificador unico do cliente."
            ```
            *Nota sobre `dbt_utils.not_negative`*: Este é um teste comum do pacote `dbt-utils`. Para usá-lo, você precisaria adicionar `dbt-utils` ao seu projeto. Crie um arquivo `packages.yml` na raiz do seu projeto dbt com:
            ```yaml
            # packages.yml
            packages:
              - package: dbt-labs/dbt_utils
                version: 1.0.0 # Ou a versao mais recente
            ```
            E então execute `dbt deps` no terminal para instalar o pacote. Se não quiser instalar `dbt-utils` agora, você pode remover esses testes específicos ou substituí-los por testes mais simples como `not_null` onde aplicável, ou criar um teste singular para verificar se não há negativos.

        *   **Executar os testes:** No terminal, execute:
            `dbt test`
        *   O dbt executará todos os testes definidos. Se houver falhas (e.g., um `id_produto` nulo nos dados de origem que não foi tratado), o teste falhará, e você precisará investigar e corrigir seu modelo ou os dados de origem (ou ajustar o teste se ele estiver incorreto).

    6.  **Visualizar a Documentação (Opcional, 5 minutos):**
        *   Gere a documentação:
            `dbt docs generate`
        *   Sirva a documentação localmente:
            `dbt docs serve`
        *   Abra o link fornecido (geralmente `http://localhost:8000`) no seu navegador para explorar a documentação do seu projeto, incluindo o DAG de dependências e as descrições dos modelos e colunas.

*   **Entregável Esperado:**
    *   A estrutura de diretórios e todos os arquivos do projeto dbt criados/modificados: `dbt_project.yml`, `profiles.yml` (configuração), `seeds/exemplo_vendas_raw.csv`, `models/staging/bronze_sources.yml`, `models/staging/stg_vendas.sql`, `models/staging/stg_vendas_schema.yml` (e opcionalmente `packages.yml`).
    *   Capturas de tela mostrando a execução bem-sucedida de `dbt seed`, `dbt run --select stg_vendas`, e `dbt test` (idealmente com todos os testes passando).

Este primeiro contato prático com dbt deve dar uma boa ideia do seu fluxo de trabalho e do poder que ele oferece para transformar e validar dados de forma estruturada e confiável. Nas próximas aulas, exploraremos funcionalidades mais avançadas do dbt e como ele se integra com Spark.

