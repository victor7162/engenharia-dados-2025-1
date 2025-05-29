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


