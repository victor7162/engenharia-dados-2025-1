## Aula 6: dbt Avançado e Introdução ao Apache Spark

**Duração Total Estimada:** 4 horas

### Objetivos de Aprendizagem da Aula

Aprofundar os conhecimentos em dbt e dar os primeiros passos significativos com Apache Spark, uma das ferramentas mais poderosas para processamento de big data. Especificamente, poderemos:

*   **Implementar transformações de dados mais complexas e reutilizáveis com dbt**, dominando o uso de macros para encapsular lógica SQL customizada e aplicando testes de dados personalizados (singulares) para garantir a robustez e a qualidade dos seus modelos.
*   **Gerenciar o ciclo de vida de modelos dbt de forma mais eficaz**, compreendendo diferentes estratégias de materialização e como documentar seus projetos dbt de maneira clara e abrangente para facilitar a colaboração e a manutenção.
*   **Compreender a arquitetura e os componentes fundamentais do Apache Spark**, incluindo o papel do Driver, Executors e Cluster Manager, e como uma Spark Application é executada.
*   **Realizar operações básicas de processamento de dados distribuídos com PySpark**, a API Python para Spark. Vamos aprender a trabalhar com as abstrações de dados fundamentais do Spark: Resilient Distributed Datasets (RDDs) e DataFrames.
*   **Configurar um ambiente Spark básico localmente (ou através do Docker)** para fins de experimentação e desenvolvimento inicial, permitindo que possamos executar nossos primeiros scripts PySpark.

Esta aula combina o aprofundamento em uma ferramenta de transformação (dbt) com a introdução a uma poderosa plataforma de processamento (Spark), preparando a infraestrutura básica para construir pipelines de dados ainda mais sofisticados e escaláveis.

### Roteiro Detalhado da Aula

#### **Hora 1: Tópicos Avançados em dbt (60 minutos)**

Na primeira hora, vamos elevar nossas habilidades com dbt, explorando macros para criar transformações mais dinâmicas e reutilizáveis, e aprofundando em testes e documentação para garantir a qualidade e a manutenibilidade dos nossos projetos.

##### **1.1 Macros no dbt: Escrevendo SQL Reutilizável com Jinja (30 minutos)**

Na Aula 2, aprendemos o básico sobre modelos dbt. Agora, vamos explorar como as **macros** podem tornar nosso código dbt ainda mais poderoso e eficiente. Macros no dbt são pedaços de código SQL (ou Jinja que gera SQL) que podem ser reutilizados em múltiplos modelos ou até mesmo em outros macros. Elas são análogas a funções em linguagens de programação e são uma das características que tornam o dbt tão flexível.

**O que são Macros e Por Que Usá-las?**

*   **Reutilização de Código (DRY - Don't Repeat Yourself):** Se você tem uma lógica SQL complexa ou um padrão de transformação que se repete em vários modelos (e.g., converter uma string para um formato de data específico, calcular uma métrica comum, limpar um tipo de campo consistentemente), você pode encapsular essa lógica em uma macro e chamá-la onde for necessário. Isso reduz a duplicação de código, tornando seus modelos mais limpos e fáceis de manter. Uma mudança na lógica só precisa ser feita em um lugar (na macro).
*   **Lógica Customizada e Abstração:** Macros permitem que você crie suas próprias "funções" SQL que não existem nativamente no seu data warehouse, ou que abstraem a sintaxe específica do seu data warehouse. Por exemplo, a sintaxe para obter a data atual pode variar entre PostgreSQL, Snowflake e BigQuery. Uma macro pode abstrair essa diferença.
*   **Controle de Fluxo e Geração Dinâmica de SQL:** Usando o poder do Jinja dentro das macros, você pode usar condicionais (`if/else`), laços (`for`) e outras construções de template para gerar SQL dinamicamente com base em configurações do projeto, variáveis ou metadados dos modelos.

**Sintaxe Jinja em Macros:**

Macros são definidas em arquivos `.sql` dentro do diretório `macros/` do projeto dbt. A sintaxe para definir uma macro é:

```sql
{% macro nome_da_macro(argumento1, argumento2, ...) %}
    -- Seu codigo SQL ou Jinja que gera SQL aqui
    -- Voce pode usar os argumentos como {{ argumento1 }}
    select 1 as id -- Exemplo simples
{% endmacro %}
```

*   `{% macro ... %}` e `{% endmacro %}`: Delimitadores Jinja que definem o início e o fim de uma macro.
*   `nome_da_macro`: O nome que você usará para chamar a macro.
*   `argumento1, argumento2, ...`: Argumentos opcionais que sua macro pode aceitar.
*   Dentro da macro, você pode usar `{{ argumento }}` para injetar o valor de um argumento no seu SQL.
*   Você também pode usar outras tags Jinja como `{% if ... %}`, `{% for ... %}`, etc.

**Chamando uma Macro:**

Para usar uma macro em um modelo (ou em outra macro), você a chama usando a sintaxe Jinja:

```sql
select
    {{ nome_da_macro(valor1, valor2) }} as resultado_da_macro,
    outra_coluna
from {{ ref("meu_modelo") }}
```

Se a macro não receber argumentos, você a chama com `{{ nome_da_macro() }}`.

**Criando e Utilizando Macros Simples:**

*   **Exemplo 1: Macro para Conversão de Tipo (Abstraindo a Sintaxe do DW)**
    Suponha que você frequentemente precise converter uma coluna para `VARCHAR(255)`.
    ```sql
    -- macros/casting_utils.sql
    {% macro cast_to_varchar255(column_name) %}
        cast({{ column_name }} as varchar(255))
    {% endmacro %}
    ```
    Uso no modelo:
    `select {{ cast_to_varchar255("descricao_produto") }} as descricao_limpa from ...`

*   **Exemplo 2: Macro para Formatação de Datas (Lógica Reutilizável)**
    Converter uma string de data `YYYYMMDD` para o tipo `DATE`.
    ```sql
    -- macros/datetime_utils.sql
    {% macro format_yyyymmdd_as_date(date_string_column) %}
        -- A sintaxe exata pode depender do seu data warehouse
        -- Exemplo para PostgreSQL/DuckDB:
        to_date({{ date_string_column }}, 'YYYYMMDD')
        -- Exemplo para BigQuery:
        -- parse_date('%Y%m%d', {{ date_string_column }})
    {% endmacro %}
    ```
    Uso no modelo:
    `select {{ format_yyyymmdd_as_date("data_evento_str") }} as data_evento from ...`

*   **Exemplo 3: Macro para Pagamentos (com Argumentos e Lógica)**
    ```sql
    -- macros/payment_macros.sql
    {% macro get_payment_status_description(payment_status_code) %}
        case {{ payment_status_code }}
            when 1 then 'Pago'
            when 2 then 'Pendente'
            when 3 then 'Falhou'
            else 'Desconhecido'
        end
    {% endmacro %}
    ```
    Uso no modelo:
    `select {{ get_payment_status_description("status_pagamento_id") }} as descricao_status_pagamento from ...`

**Exemplos de Macros Úteis em Projetos de Engenharia de Dados:**

*   **Auditoria de Colunas:** Macros que geram colunas de auditoria automaticamente (e.g., `data_carga`, `data_atualizacao`, `usuario_carga`).
*   **Geração de Chaves Surrogadas (Surrogate Keys):** O pacote `dbt-utils` (um pacote dbt muito popular) fornece uma macro `generate_surrogate_key` que cria um hash consistente a partir de uma lista de colunas, útil para criar chaves primárias para modelos.
    Exemplo (requer `dbt-utils` instalado via `packages.yml` e `dbt deps`):
    `select {{ dbt_utils.generate_surrogate_key(["coluna1", "coluna2"]) }} as minha_chave_primaria, ...`
*   **Pivotagem de Dados:** Macros que ajudam a transformar linhas em colunas (pivotar), uma operação comum mas muitas vezes verbosa em SQL.
*   **Validação de Datas:** Macros para verificar se uma string é uma data válida antes de tentar convertê-la.

**Atividade Prática 1: Criando e Usando uma Macro no dbt**

*   **Objetivo:** Desenvolver a habilidade de criar e aplicar uma macro simples no dbt para reutilização de código SQL, especificamente para normalizar strings.
*   **Formato:** Exercício prático individual, guiado.
*   **Pré-requisitos:** Projeto dbt configurado da Aula 2 (e.g., `meu_primeiro_projeto_dbt` com o modelo `stg_vendas`).
*   **Passos da Tarefa:**

    1.  **Definir a Macro (10 minutos):**
        *   No diretório `macros/` do seu projeto dbt, crie um novo arquivo SQL chamado `text_utils.sql` (ou similar).
        *   Dentro deste arquivo, defina uma macro chamada `normalizar_string` que receba um nome de coluna (ou uma expressão de string) como argumento. A macro deve realizar duas operações:
            1.  Converter a string para letras minúsculas.
            2.  Remover espaços em branco no início e no fim da string (trim).
        *   O corpo da macro (para a maioria dos SQLs, incluindo DuckDB/PostgreSQL) seria:
            ```sql
            -- macros/text_utils.sql
            {% macro normalizar_string(string_expression) %}
                trim(lower({{ string_expression }}))
            {% endmacro %}
            ```

    2.  **Aplicar a Macro em um Modelo Existente (10 minutos):**
        *   Abra seu modelo `models/staging/stg_vendas.sql` (criado na Aula 2).
        *   Identifique colunas de string que se beneficiariam da normalização, como `Nome_Produto` e `Categoria` (ou `nome_produto` e `categoria` se você já as renomeou e aplicou `lower` e `trim` manualmente antes).
        *   Modifique o SQL do modelo `stg_vendas.sql` para usar sua nova macro `normalizar_string` nessas colunas. Por exemplo, se você estava fazendo `trim(lower(Nome_Produto)) as nome_produto`, agora você fará `{{ normalizar_string("Nome_Produto") }} as nome_produto`.
            ```sql
            -- models/staging/stg_vendas.sql (trecho modificado)
            select
                cast(ID_Produto as varchar) as id_produto,
                {{ normalizar_string("Nome_Produto") }} as nome_produto, -- Usando a macro
                {{ normalizar_string("Categoria") }} as categoria,   -- Usando a macro
                -- ... resto das colunas ...
                {{ normalizar_string("Cidade_Cliente") }} as cidade_cliente -- Usando a macro
            from {{ source("raw_data_source", "exemplo_vendas_raw") }}
            ```

    3.  **Compilar e Executar o Modelo (5 minutos):**
        *   No terminal, dentro do diretório do seu projeto dbt, execute `dbt compile` para verificar se a macro é processada corretamente e o SQL gerado está como esperado (você pode inspecionar o SQL compilado em `target/compiled/...`).
        *   Em seguida, execute `dbt run --select stg_vendas` para materializar o modelo com a macro aplicada.

    4.  **Verificar os Resultados (5 minutos):**
        *   Use uma ferramenta de SQL para se conectar ao seu banco de dados e consulte a tabela (ou view) `stg_vendas`.
        *   Verifique se as colunas `nome_produto`, `categoria` e `cidade_cliente` estão de fato em minúsculas e sem espaços extras no início ou no fim.


##### **1.2 Testes Personalizados (Singulares) e Documentação Avançada (30 minutos)**

Na Aula 5, introduzimos os testes genéricos (schema tests) que são definidos em arquivos `.yml`. Agora, vamos explorar os **testes singulares (data tests)**, que oferecem mais flexibilidade para lógicas de validação complexas, e como enriquecer nossa documentação.

**Testes Singulares (Data Tests):**

Testes singulares são consultas SQL que você escreve e armazena em arquivos `.sql` dentro do diretório `tests/` (ou subdiretórios como `tests/singular/`).

*   **Como Funcionam:** Um teste singular no dbt é considerado **aprovado (pass)** se a consulta SQL que ele contém retorna **zero linhas**. Se a consulta retornar uma ou mais linhas, o teste é considerado **reprovado (fail)**, e cada linha retornada representa um registro que falhou na validação.
*   **Casos de Uso:** São ideais para validações que não são facilmente cobertas por testes genéricos, como:
    *   Verificar a consistência entre múltiplas colunas dentro do mesmo registro (e.g., `data_fim` deve ser sempre maior ou igual a `data_inicio`).
    *   Garantir a integridade referencial complexa que não se encaixa no teste genérico `relationships`.
    *   Validar regras de negócio específicas (e.g., clientes premium devem ter um gasto mínimo nos últimos 6 meses).
    *   Verificar se não há lacunas em sequências numéricas ou de datas.

*   **Exemplo de Teste Singular:**
    Suponha que no nosso modelo `stg_vendas`, queremos garantir que `preco_unitario` nunca seja negativo (embora já tenhamos um teste genérico `dbt_utils.not_negative` para isso, vamos ilustrar como seria um teste singular).
    ```sql
    -- tests/singular/assert_stg_vendas_preco_unitario_nao_negativo.sql
    select
        id_venda, -- Supondo que stg_vendas tenha uma chave primaria id_venda
        preco_unitario
    from {{ ref("stg_vendas") }}
    where preco_unitario < 0
    ```
    Se esta consulta retornar alguma linha, o teste `assert_stg_vendas_preco_unitario_nao_negativo` falhará.

**Testes Genéricos Mais Complexos (com Argumentos):**

Além dos testes genéricos padrão, você pode criar suas próprias **macros de teste genérico**. Estas são macros que aceitam `model` e `column_name` como argumentos (e outros argumentos customizados) e retornam uma consulta SQL que, se resultar em linhas, indica uma falha no teste. Elas são definidas no diretório `macros/` e podem ser referenciadas na seção `tests:` dos seus arquivos `.yml`.

Exemplo: Uma macro de teste genérico para verificar se uma string corresponde a um padrão regex.
```sql
-- macros/generic_tests.sql
{% macro test_must_match_regex(model, column_name, regex_pattern) %}
    select {{ column_name }}
    from {{ model }}
    where not {{ column_name }} ~ '{{ regex_pattern }}' -- Sintaxe regex pode variar com o DW
{% endmacro %}
```
Uso no `schema.yml`:
```yaml
models:
  - name: stg_clientes
    columns:
      - name: codigo_postal
        tests:
          - must_match_regex: { regex_pattern: '^[0-9]{5}(?:-[0-9]{4})?$' }
```

**Documentação Avançada:**

O dbt não apenas gera documentação, mas permite que você a enriqueça significativamente.

*   **Descrições em Arquivos `.yml`:** Você pode (e deve!) adicionar descrições para seus modelos, colunas, fontes, seeds e testes diretamente nos arquivos de configuração `.yml` (como `schema.yml`, `sources.yml`).
    ```yaml
    models:
      - name: stg_vendas
        description: "Este modelo limpa e prepara os dados de vendas da fonte bruta. Contem uma linha por item de venda."
        columns:
          - name: id_produto
            description: "Chave estrangeira para a tabela de produtos. Garante que cada venda esteja associada a um produto valido."
            tests:
              - not_null
    ```
*   **Descrições em Arquivos Markdown (`.md`):** Para descrições mais longas ou mais ricas (com formatação, links, imagens), você pode criar arquivos Markdown no diretório `models/` (ou subdiretórios) com o mesmo nome do seu modelo (e.g., `models/staging/stg_vendas.md`). O dbt associará automaticamente o conteúdo deste arquivo Markdown à documentação do modelo `stg_vendas`.
    Você pode usar a tag Jinja `{{ doc("nome_do_bloco_doc") }}` para referenciar blocos de documentação reutilizáveis definidos em arquivos `.md` no diretório `docs/` (ou em qualquer lugar, configurável no `dbt_project.yml`).

*   **Gerando e Hospedando a Documentação:**
    *   `dbt docs generate`: Compila as informações do seu projeto e gera os arquivos estáticos (HTML, JSON) da documentação (geralmente em `target/`).
    *   `dbt docs serve`: Inicia um servidor web local (padrão na porta 8000) para visualizar a documentação interativa no seu navegador.
    *   Para compartilhar a documentação, você pode hospedar os arquivos estáticos gerados (o conteúdo do diretório `target/`) em qualquer servidor web (e.g., GitHub Pages, AWS S3, Netlify).

Manter uma documentação rica e atualizada é crucial para a colaboração, o onboarding de novos membros da equipe e a compreensão geral do seu pipeline de dados.

**Atividade Prática 2: Implementando um Teste de Dados Singular no dbt**

*   **Objetivo:** Aprender a escrever um teste de dados personalizado (singular) no dbt para garantir uma regra de negócio específica que não é facilmente coberta por testes genéricos.
*   **Formato:** Exercício prático individual, guiado.
*   **Pré-requisitos:** Projeto dbt configurado com o modelo `stg_vendas` da atividade anterior.
*   **Passos da Tarefa:**

    1.  **Cenário do Teste (5 minutos):**
        *   No nosso modelo `stg_vendas`, temos as colunas `preco_unitario` e `quantidade_vendida`. Vamos criar uma nova coluna calculada chamada `valor_total_item` que seja `preco_unitario * quantidade_vendida`.
        *   **Regra de Negócio para Testar:** Queremos garantir que, para todos os itens vendidos, o `valor_total_item` calculado seja igual ao produto de `preco_unitario` e `quantidade_vendida` armazenados, e que este valor nunca seja negativo (assumindo que descontos não resultam em valor negativo aqui).
        *   Primeiro, modifique seu modelo `models/staging/stg_vendas.sql` para adicionar a coluna `valor_total_item`:
            ```sql
            -- models/staging/stg_vendas.sql (adicionar ao final da lista de select)
            -- ... (colunas anteriores)
            (case 
                when Preco_Unitario is null or trim(Preco_Unitario) = '' then null
                else cast(replace(Preco_Unitario, ',', '.') as double)
            end) * 
            (case
                when Quantidade_Vendida is null or trim(Quantidade_Vendida) = '' or upper(trim(Quantidade_Vendida)) = 'NULL' then 0
                when cast(Quantidade_Vendida as integer) < 0 then 0
                else cast(Quantidade_Vendida as integer)
            end) as valor_total_item
            ```
        *   Execute `dbt run --select stg_vendas` para recriar o modelo com a nova coluna.

    2.  **Escrever o Teste Singular (15 minutos):**
        *   No diretório `tests/singular/` do seu projeto dbt (crie o subdiretório `singular` se não existir), crie um novo arquivo SQL chamado `assert_stg_vendas_valor_total_consistente.sql`.
        *   Neste arquivo, escreva uma consulta SQL que selecione todos os registros do modelo `stg_vendas` onde:
            *   `valor_total_item` seja negativo, OU
            *   `valor_total_item` não seja igual a `preco_unitario * quantidade_vendida` (cuidado com nulos aqui; pode ser mais simples testar apenas a não negatividade se o cálculo já estiver no modelo).
            Lembre-se, o teste passa se esta consulta retornar zero linhas.
        *   Exemplo de SQL para o teste (focando na não negatividade e consistência básica, assumindo que `preco_unitario` e `quantidade_vendida` no modelo `stg_vendas` já foram tratados para serem numéricos e não nulos onde o cálculo é possível):
            ```sql
            -- tests/singular/assert_stg_vendas_valor_total_consistente.sql
            select
                id_produto, -- ou alguma chave primaria do stg_vendas
                data_venda,
                preco_unitario,
                quantidade_vendida,
                valor_total_item
            from {{ ref("stg_vendas") }}
            where valor_total_item < 0
               or abs(valor_total_item - (preco_unitario * quantidade_vendida)) > 0.001 -- Testando consistencia com uma pequena tolerancia para floats
               -- Adicionar tratamento para casos onde preco_unitario ou quantidade_vendida podem ser nulos no stg_vendas, se aplicavel
            ```
            *Nota sobre comparação de floats:* Comparar números de ponto flutuante para igualdade exata pode ser problemático. Usar uma pequena tolerância (`abs(a - b) > epsilon`) é mais robusto.

    3.  **Executar o Teste (5 minutos):**
        *   No terminal, dentro do diretório do projeto dbt, execute `dbt test`.
        *   Se o teste falhar, examine as linhas retornadas (o dbt mostrará o SQL compilado e os resultados da falha). Investigue se o problema está nos dados de origem, na lógica do seu modelo `stg_vendas` (especificamente no cálculo de `valor_total_item`), ou na lógica do seu teste.
        *   Ajuste conforme necessário até que o teste passe (ou até que você confirme que ele está corretamente identificando um problema nos dados/lógica).

*   **Entregável Esperado:**
    *   O arquivo `models/staging/stg_vendas.sql` atualizado com a coluna `valor_total_item`.
    *   O arquivo SQL do teste singular: `tests/singular/assert_stg_vendas_valor_total_consistente.sql`.
    *   Uma captura de tela do comando `dbt test` mostrando o resultado do seu novo teste (passando ou, se falhar, uma explicação do porquê).

Dominar macros e testes (genéricos e singulares) eleva significativamente a qualidade, a robustez e a manutenibilidade dos seus projetos dbt.




#### **Hora 2: Gerenciamento de Projetos dbt e Melhores Práticas (60 minutos)**

Com um bom entendimento de macros e testes, podemos agora focar em como organizar e gerenciar nossos projetos dbt de forma eficiente, especialmente à medida que eles crescem em complexidade. Abordaremos também as diferentes maneiras como o dbt pode materializar nossos modelos e como ele se encaixa em um ecossistema de dados maior.

##### **2.1 Organização de Projetos dbt (20 minutos)**

Manter um projeto dbt bem organizado é crucial para a sua manutenibilidade, escalabilidade e para a colaboração em equipe. Algumas práticas recomendadas incluem:

**Estruturando Modelos para as Camadas Bronze, Silver e Gold (Arquitetura Medallion):**

A arquitetura Medallion (Bronze, Silver, Gold) fornece um excelente framework para organizar seus modelos dbt:

*   **`models/staging/` (ou `models/bronze/` ou `models/sources/`)**: Esta área é frequentemente usada para modelos de "staging" que representam a primeira camada de transformação sobre seus dados brutos (fontes). As tarefas aqui geralmente incluem:
    *   Renomear colunas para seguir um padrão consistente (e.g., snake_case).
    *   Converter tipos de dados para os formatos corretos.
    *   Realizar limpezas muito básicas (e.g., trim de strings).
    *   Selecionar apenas as colunas necessárias da fonte.
    *   Idealmente, cada modelo de staging corresponde a uma tabela de origem (fonte). Frequentemente, estes são materializados como views para evitar duplicação de dados e refletir o estado mais recente da fonte.
    *   Exemplo: `models/staging/stg_erp__clientes.sql`, `models/staging/stg_analytics_events__sessoes.sql`.

*   **`models/intermediate/` (Opcional, parte da Silver):** Para transformações intermediárias complexas que são usadas por múltiplos modelos da camada Gold (marts), mas que não são diretamente expostas aos usuários finais. Isso ajuda a evitar a duplicação de lógica complexa.

*   **`models/marts/` (ou `models/gold/`)**: Esta é a camada final, onde você constrói seus modelos de dados que serão consumidos por ferramentas de BI, analistas, cientistas de dados ou aplicações. Estes modelos são geralmente mais agregados, juntam dados de diferentes fontes (staging/intermediate) e são otimizados para consulta.
    *   **Modelos de Dimensão:** Descrevem as entidades de negócio (e.g., `dim_clientes`, `dim_produtos`, `dim_datas`).
    *   **Modelos de Fato:** Contêm as métricas e eventos de negócio, com chaves estrangeiras para as dimensões (e.g., `fct_vendas`, `fct_pedidos`).
    *   Estes são frequentemente materializados como tabelas para melhor performance de consulta, ou como tabelas incrementais para processar grandes volumes de dados de forma eficiente.

**Uso de Subdiretórios e Tags para Organizar Modelos:**

*   **Subdiretórios:** Além de organizar por camada (staging, marts), você pode usar subdiretórios dentro dessas pastas para agrupar modelos por área de negócio, fonte de dados ou projeto.
    *   Exemplo: `models/marts/finance/`, `models/marts/marketing/`.
*   **Tags:** O dbt permite que você adicione tags aos seus modelos (no `dbt_project.yml` ou em blocos de configuração dentro dos arquivos SQL dos modelos). Tags são úteis para:
    *   Executar ou testar subconjuntos de modelos (e.g., `dbt run --tag financeiro`, `dbt test --tag pii`).
    *   Agrupar modelos logicamente para documentação ou governança.
    ```yaml
    # dbt_project.yml
    models:
      meu_projeto_dbt:
        marts:
          finance:
            +tags:
              - "financeiro"
              - "relatorio_mensal"
    ```
    Ou no próprio modelo:
    ```sql
    {{ config(tags=["financeiro", "relatorio_mensal"]) }}
    select ...
    ```

**Convenções de Nomenclatura:**

Adotar convenções de nomenclatura consistentes é vital.

*   **Modelos:**
    *   Prefixos para indicar a camada: `stg_` para staging, `int_` para intermediário, `dim_` para dimensão, `fct_` para fato.
    *   Incluir a granularidade ou a entidade principal: `stg_erp__clientes`, `fct_vendas_diarias_por_produto`.
    *   Usar `snake_case`.
*   **Colunas:**
    *   Usar `snake_case`.
    *   Ser descritivo (e.g., `preco_unitario_brl` em vez de `prc`).
    *   Manter consistência para colunas comuns (e.g., `id_cliente` sempre se refere à mesma coisa).
*   **Arquivos:** O nome do arquivo SQL deve corresponder ao nome do modelo (e.g., `stg_clientes.sql` para o modelo `stg_clientes`).

##### **2.2 Ciclo de Vida e Deploy de Modelos dbt (20 minutos)**

**Desenvolvimento, Teste e Produção com dbt:**

O dbt suporta um ciclo de vida de desenvolvimento semelhante ao de software:

1.  **Desenvolvimento (`dev`):**
    *   Analistas e engenheiros trabalham em seus próprios schemas de desenvolvimento no data warehouse (configurado no `profiles.yml` target `dev`).
    *   Isso permite que eles criem e modifiquem modelos sem afetar a produção ou outros desenvolvedores.
    *   Uso de `dbt run`, `dbt test` iterativamente.
    *   Frequentemente, os modelos são materializados como views no desenvolvimento para builds mais rápidos.
2.  **Teste/Staging (`qa` ou `staging` - opcional):**
    *   Antes de ir para produção, os modelos podem ser implantados em um ambiente de QA ou staging, que pode usar uma cópia recente dos dados de produção.
    *   Testes mais rigorosos são executados.
3.  **Produção (`prod`):**
    *   Os modelos são implantados no schema de produção, usando um target `prod` no `profiles.yml`.
    *   As execuções são geralmente agendadas por um orquestrador (ver próxima seção).
    *   Materializações como tabelas ou incrementais são mais comuns para performance.

**Estratégias de Materialização:**

O dbt permite que você escolha como seus modelos serão "materializados" no data warehouse. Isso é configurado no `dbt_project.yml` ou diretamente no arquivo SQL do modelo usando `{{ config(materialized=	extlessmaterialization_type	extgreater) }}`.

*   **`view` (Padrão):** O modelo é criado como uma view no data warehouse. A consulta SQL do modelo é executada toda vez que a view é consultada. Não armazena dados fisicamente.
    *   **Prós:** Sempre reflete os dados mais recentes das tabelas subjacentes. Builds rápidos (só cria a DDL da view).
    *   **Contras:** Pode ser lento para consultar se a lógica do modelo for complexa ou se as tabelas subjacentes forem grandes.
    *   **Uso:** Bom para modelos de staging ou modelos que não são consultados frequentemente e precisam de dados frescos.
*   **`table`:** O modelo é criado como uma nova tabela no data warehouse. A consulta SQL é executada durante o `dbt run`, e os resultados são armazenados na tabela.
    *   **Prós:** Consultas subsequentes à tabela são rápidas.
    *   **Contras:** Os dados ficam "congelados" no momento do `dbt run`. Requer a reconstrução completa da tabela a cada execução, o que pode ser lento e custoso para modelos grandes.
    *   **Uso:** Bom para modelos menores ou modelos da camada Gold que precisam de performance de consulta e podem ser reconstruídos regularmente.
*   **`incremental`:** O modelo é criado como uma tabela. Na primeira execução, a tabela é construída completamente. Nas execuções subsequentes, o dbt insere ou atualiza apenas os registros novos ou alterados desde a última execução, em vez de reconstruir a tabela inteira.
    *   **Prós:** Reduz significativamente o tempo de build para modelos grandes, processando apenas dados incrementais.
    *   **Contras:** Requer uma lógica mais complexa no SQL do modelo para identificar os registros incrementais (geralmente usando uma coluna de data/timestamp de atualização e a função `is_incremental()` do dbt).
    *   **Uso:** Essencial para modelos de fato grandes ou qualquer tabela onde a reconstrução completa é inviável.
*   **`ephemeral`:** O modelo não é criado diretamente no data warehouse. Em vez disso, seu SQL é interpolado como uma Common Table Expression (CTE) em todos os modelos que o referenciam.
    *   **Prós:** Ajuda a organizar lógicas SQL complexas sem criar objetos desnecessários no banco de dados. Pode melhorar a clareza do código.
    *   **Contras:** Não pode ser selecionado diretamente. A lógica é duplicada em cada modelo dependente (embora o otimizador do banco de dados possa lidar bem com isso).
    *   **Uso:** Para modelos intermediários muito leves que são usados apenas como blocos de construção para outros modelos e não precisam ser consultados independentemente.

**Comandos `dbt seed` e `dbt snapshot` (Visão Geral):**

*   **`dbt seed`:** Como vimos na Aula 2, carrega dados de arquivos CSV (localizados no diretório `seeds/`) para o seu data warehouse. Útil para dados de lookup, mapeamentos ou pequenos datasets estáticos.
*   **`dbt snapshot`:** Usado para capturar mudanças em dados ao longo do tempo, implementando Slowly Changing Dimensions (SCD) Tipo 2. Ele rastreia o histórico de alterações em uma tabela de origem, adicionando colunas como `dbt_valid_from` e `dbt_valid_to` para indicar o período em que uma versão de um registro era válida. Snapshots são definidos em arquivos `.sql` no diretório `snapshots/`.

##### **2.3 Integração do dbt com Ferramentas de Orquestração (20 minutos)**

Embora o dbt seja excelente para definir e executar transformações de dados, ele não é um orquestrador de pipelines por si só. Em um ambiente de produção, você normalmente usará uma ferramenta de orquestração para agendar e gerenciar a execução dos seus jobs dbt como parte de um pipeline de dados maior.

**O que é Orquestração de Pipelines?**

Orquestração envolve o agendamento, execução, monitoramento e gerenciamento de dependências entre diferentes tarefas em um fluxo de trabalho de dados. Um pipeline de dados pode envolver várias etapas, como:

1.  Ingestão de dados de fontes externas (e.g., APIs, bancos de dados transacionais).
2.  Carregamento dos dados brutos para o data lake ou data warehouse (camada Bronze).
3.  Execução de transformações dbt (Bronze -> Silver -> Gold).
4.  Exportação de dados para sistemas downstream ou ferramentas de BI.
5.  Execução de verificações de qualidade de dados.

**Ferramentas de Orquestração Populares:**

*   **Apache Airflow:** Uma das ferramentas de orquestração open-source mais populares. Permite definir pipelines (DAGs - Directed Acyclic Graphs) como código Python. Possui operadores para interagir com diversos sistemas, incluindo um operador para executar comandos dbt (`BashOperator` para `dbt run`, ou operadores da comunidade como `DbtCloudRunJobOperator` ou `DbtRunOperator`).
*   **Dagster:** Uma ferramenta de orquestração mais nova, focada em desenvolvimento e teste local, e com forte ênfase em ativos de dados (data assets). Também permite definir pipelines em Python e tem integrações com dbt.
*   **Prefect:** Outra ferramenta moderna de orquestração em Python, conhecida por sua API dinâmica e flexível.
*   **dbt Cloud:** A oferta SaaS do dbt Labs inclui um agendador integrado, eliminando a necessidade de um orquestrador externo para jobs dbt simples, mas pode ainda ser integrado com orquestradores para pipelines mais complexos.
*   **Outros:** Azure Data Factory, AWS Step Functions, Google Cloud Composer (gerenciado Airflow).

**Como Integrar dbt com Orquestradores:**

A integração geralmente envolve configurar o orquestrador para executar comandos CLI do dbt em momentos específicos ou após a conclusão de tarefas anteriores.

*   **Exemplo com Airflow (usando `BashOperator`):**
    ```python
    # Exemplo de tarefa Airflow para executar dbt run
    from airflow.operators.bash import BashOperator

    dbt_run_task = BashOperator(
        task_id=	extquotesinglemeu_dbt_run_job	extquotesingle,
        bash_command=	extquotesingledbt run --project-dir /caminho/para/meu_projeto_dbt --profiles-dir /caminho/para/perfis	extquotesingle,
        dag=meu_dag
    )
    ```
    É crucial garantir que o ambiente onde o comando dbt é executado pelo orquestrador tenha acesso ao executável do dbt, ao código do projeto dbt e ao arquivo `profiles.yml` com as credenciais corretas.

*   **Dependências:** O orquestrador gerencia as dependências. Por exemplo, uma tarefa de ingestão de dados deve ser concluída antes que a tarefa `dbt run` seja iniciada.
*   **Monitoramento e Alertas:** Orquestradores fornecem interfaces para monitorar o status das execuções, logs e configurar alertas em caso de falhas.

Ao integrar o dbt com uma ferramenta de orquestração, você pode automatizar seus pipelines de transformação de dados, garantindo que eles sejam executados de forma confiável e na ordem correta, como parte de um fluxo de trabalho de dados mais amplo.

#### **Hora 3: Introdução ao Apache Spark (60 minutos)**

Até agora, focamos em Docker para ambientes e dbt para transformações dentro do data warehouse. Agora, vamos introduzir o **Apache Spark**, um poderoso motor de processamento distribuído de big data. O Spark é frequentemente usado para tarefas de ETL mais pesadas, processamento de dados em larga escala (que podem não caber ou ser eficientes em um data warehouse tradicional para certas transformações), streaming de dados e machine learning.

##### **3.1 O que é Apache Spark? (20 minutos)**

**História e Motivação:**

Apache Spark foi originalmente desenvolvido na Universidade da Califórnia, Berkeley, no AMPLab em 2009, e depois doado à Apache Software Foundation em 2013. Ele surgiu como uma resposta às limitações do Apache Hadoop MapReduce, que, embora poderoso para processamento em batch de grandes volumes de dados, era lento devido à sua dependência de operações de leitura/escrita em disco entre as etapas Map e Reduce.

O Spark foi projetado para ser mais rápido e mais geral, especialmente para algoritmos iterativos (comuns em machine learning) e consultas interativas, ao realizar o processamento em memória sempre que possível.

**Principais Características:**

*   **Velocidade:** O Spark pode ser significativamente mais rápido que o Hadoop MapReduce (até 100x em memória e 10x em disco) para muitas aplicações, devido ao processamento em memória e a um motor de execução otimizado (DAG scheduler, Catalyst optimizer).
*   **Processamento em Memória (In-Memory Processing):** O Spark armazena dados intermediários em memória (RAM) em vez de escrevê-los em disco, o que reduz drasticamente a latência de I/O para muitas operações.
*   **Tolerância a Falhas:** Os dados no Spark são representados como Resilient Distributed Datasets (RDDs) ou DataFrames/Datasets, que são coleções distribuídas e tolerantes a falhas. Se uma parte dos dados ou um nó de processamento falhar, o Spark pode reconstruir os dados perdidos a partir da linhagem (lineage) das transformações.
*   **Escalabilidade:** O Spark é projetado para escalar horizontalmente, de um único laptop a clusters de milhares de nós, permitindo o processamento de petabytes de dados.
*   **Generalidade:** O Spark não é apenas para processamento em batch. Seu ecossistema inclui bibliotecas para diversas cargas de trabalho:
    *   **Spark SQL:** Para trabalhar com dados estruturados usando SQL ou uma API semelhante a DataFrames.
    *   **Spark Streaming (e Structured Streaming):** Para processamento de fluxos de dados em tempo real ou quase real.
    *   **MLlib:** Uma biblioteca de machine learning com algoritmos comuns.
    *   **GraphX (e GraphFrames):** Para processamento de grafos e análise de redes.
*   **APIs em Múltiplas Linguagens:** O Spark fornece APIs para Scala (sua linguagem nativa), Java, Python (PySpark) e R, tornando-o acessível a uma ampla gama de desenvolvedores e cientistas de dados.

**Casos de Uso Comuns:**

*   **ETL (Extract, Transform, Load) de Big Data:** Processar e transformar grandes volumes de dados de diversas fontes antes de carregá-los em data warehouses ou data lakes.
*   **Análise de Dados Interativa:** Cientistas de dados podem usar shells interativos do Spark (PySpark, Spark-shell) para explorar e analisar grandes datasets.
*   **Machine Learning em Larga Escala:** Treinar modelos de machine learning em grandes conjuntos de dados usando MLlib.
*   **Processamento de Dados de Streaming:** Analisar dados de sensores, logs de aplicações, feeds de mídias sociais em tempo real.
*   **Análise de Grafos:** Analisar relacionamentos e padrões em dados de redes sociais, sistemas de recomendação, etc.

##### **3.2 Arquitetura do Spark (20 minutos)**

Entender a arquitetura do Spark é fundamental para escrever aplicações eficientes e para depurar problemas.

**Componentes Principais de uma Aplicação Spark:**

Uma aplicação Spark consiste em um programa **driver** e múltiplos processos **executors** que rodam em um **cluster**.

*   **Spark Driver (Programa Principal):**
    *   É o processo que executa a função `main()` da sua aplicação Spark e onde o `SparkContext` (ou `SparkSession`, que encapsula o `SparkContext`) é criado.
    *   Responsável por:
        *   Converter o código do usuário em tarefas (jobs, stages, tasks).
        *   Agendar a execução dessas tarefas nos executors.
        *   Coordenar os executors e manter o estado geral da aplicação.
    *   O driver pode rodar no nó mestre do cluster ou em uma máquina cliente separada.

*   **Executors (Executores):**
    *   São processos de trabalho que rodam nos nós do cluster (worker nodes).
    *   Responsáveis por:
        *   Executar as tarefas (tasks) atribuídas pelo driver.
        *   Armazenar partições de dados em memória ou em disco.
        *   Retornar os resultados das tarefas para o driver.
    *   Cada executor tem um número de "slots" (cores) que podem executar tarefas em paralelo.

*   **Cluster Manager (Gerenciador de Cluster):**
    *   É o componente responsável por alocar recursos (CPU, memória) no cluster para a aplicação Spark.
    *   O Spark suporta vários gerenciadores de cluster:
        *   **Standalone:** Um gerenciador de cluster simples incluído com o Spark. Útil para clusters dedicados ao Spark.
        *   **Apache Hadoop YARN (Yet Another Resource Negotiator):** O gerenciador de recursos do Hadoop. Permite que o Spark rode em clusters Hadoop existentes, compartilhando recursos com outras aplicações Hadoop (MapReduce, Hive, etc.).
        *   **Apache Mesos:** Um gerenciador de cluster de propósito geral que também pode rodar Hadoop, Spark e outras aplicações distribuídas.
        *   **Kubernetes:** Uma plataforma de orquestração de contêineres que pode ser usada para gerenciar e escalar aplicações Spark em contêineres.

**Fluxo de Execução de uma Aplicação Spark:**

1.  O usuário submete a aplicação Spark (e.g., um script PySpark) usando `spark-submit`.
2.  O `spark-submit` lança o programa **Driver**.
3.  O Driver solicita recursos ao **Cluster Manager** para lançar os **Executors**.
4.  O Cluster Manager aloca recursos e lança os Executors nos nós de trabalho.
5.  Os Executors se registram de volta com o Driver.
6.  O Driver analisa o código da aplicação e cria um plano de execução lógico (DAG de transformações).
7.  O Driver converte o plano lógico em um plano físico de tarefas e as distribui para os Executors.
8.  Os Executors executam as tarefas nas partições de dados e retornam os resultados (se necessário) para o Driver ou os escrevem em um sistema de armazenamento externo.

**Conceito de Spark Application e Spark Session:**

*   **Spark Application:** Uma instância de um programa Spark, consistindo de um Driver e seus Executors.
*   **`SparkContext` (em RDD API):** O ponto de entrada principal para a funcionalidade do Spark antes do Spark 2.0. Representa a conexão com um cluster Spark e é usado para criar RDDs, acumuladores e broadcast variables.
*   **`SparkSession` (em DataFrame/Dataset API, a partir do Spark 2.0):** O ponto de entrada unificado para interagir com as funcionalidades do Spark. Ele encapsula o `SparkContext` e também fornece acesso ao Spark SQL, Hive e outras funcionalidades. É a maneira recomendada de iniciar uma aplicação Spark moderna.
    ```python
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("MinhaAppSpark").getOrCreate()
    # spark e sua instancia de SparkSession
    # spark.sparkContext da acesso ao SparkContext subjacente
    ```

##### **3.3 Ecossistema Spark (20 minutos)**

O Apache Spark é mais do que apenas um motor de processamento; é uma plataforma com um rico ecossistema de bibliotecas e componentes construídos sobre seu núcleo.

*   **Spark Core:** Contém a funcionalidade básica do Spark, incluindo o agendamento de tarefas, gerenciamento de memória, tolerância a falhas, e a API para Resilient Distributed Datasets (RDDs).

*   **Spark SQL:**
    *   Permite que você execute consultas SQL em dados armazenados em DataFrames do Spark ou em fontes de dados externas (como Hive, Parquet, JSON, JDBC).
    *   Fornece a API de **DataFrames** e **Datasets** (uma extensão do DataFrame com verificação de tipo em tempo de compilação, mais usada em Scala/Java).
    *   Inclui o **Catalyst Optimizer**, um otimizador de consultas extensível que melhora a performance das consultas SQL e das operações de DataFrame.
    *   Pode atuar como um data warehouse distribuído.

*   **Spark Streaming (e Structured Streaming):**
    *   **Spark Streaming (Legado):** Baseado em DStreams (Discretized Streams), processa dados em micro-batches. Ainda usado, mas Structured Streaming é o futuro.
    *   **Structured Streaming (Recomendado):** Uma API de processamento de stream de alto nível construída sobre o motor Spark SQL. Permite que você escreva consultas de streaming da mesma forma que escreveria consultas em batch em dados estáticos (usando DataFrames/Datasets). Fornece garantias de processamento exactly-once (exatamente uma vez) e é mais fácil de usar e mais robusto.
    *   Usado para processar dados de fontes como Kafka, Flume, Kinesis, HDFS/S3 (para arquivos chegando em um diretório).

*   **MLlib (Machine Learning Library):**
    *   A biblioteca de machine learning do Spark. Contém algoritmos de aprendizado comuns e utilitários, incluindo:
        *   Classificação (e.g., Regressão Logística, Naive Bayes, Árvores de Decisão, Random Forests).
        *   Regressão (e.g., Regressão Linear Generalizada, Árvores de Decisão).
        *   Clusterização (e.g., K-means).
        *   Filtragem Colaborativa (para sistemas de recomendação).
        *   Redução de Dimensionalidade (e.g., PCA).
        *   Ferramentas para construção de pipelines de ML, extração de features, avaliação de modelos.
    *   Projetada para escalar para grandes conjuntos de dados.

*   **GraphX (e GraphFrames):**
    *   **GraphX:** A API original do Spark para processamento de grafos e computação paralela em grafos. Baseada em RDDs.
    *   **GraphFrames:** Uma API mais nova para processamento de grafos baseada em DataFrames. Oferece consultas de grafos mais ricas (usando padrões Motifs) e se integra melhor com Spark SQL.
    *   Usado para algoritmos de grafos como PageRank, Connected Components, Triangle Counting.

Esses componentes podem ser usados em conjunto na mesma aplicação Spark, permitindo que você construa pipelines de dados sofisticados que combinam processamento em batch, streaming, SQL e machine learning.

#### **Hora 4: Primeiros Passos com PySpark (60 minutos)**

Nesta hora final, vamos dar nossos primeiros passos práticos com PySpark, a API Python para Apache Spark. Vamos configurar um ambiente básico, aprender sobre RDDs e DataFrames, e realizar algumas operações simples de processamento de dados.

##### **4.1 Configurando o Ambiente Spark (15 minutos)**

Para rodar PySpark, você precisa ter o Spark instalado e configurado. Existem algumas opções:

*   **Instalação Local (Standalone):**
    1.  **Pré-requisitos:** Java (JDK 8 ou 11 geralmente recomendado) e Python (3.x).
    2.  **Download do Spark:** Baixe uma versão pré-compilada do Spark do site oficial da Apache Spark (e.g., "Spark 3.x.x pre-built for Apache Hadoop 3.x and later").
    3.  **Extrair e Configurar Variáveis de Ambiente:**
        *   Extraia o arquivo baixado (e.g., para `/opt/spark` ou `C:\Spark`).
        *   Configure `SPARK_HOME` para apontar para o diretório de instalação do Spark.
        *   Adicione `$SPARK_HOME/bin` (ou `%SPARK_HOME%\bin`) ao seu `PATH`.
        *   (Opcional) Configure `HADOOP_HOME` se estiver usando uma build com Hadoop e precise dos utilitários winutils.exe no Windows.
    4.  **Instalar PySpark via pip:** `pip install pyspark` (geralmente já vem com a distribuição Spark, mas instalar via pip pode garantir a versão correta para seu ambiente Python).

*   **Usando Docker:** Esta é frequentemente a maneira mais fácil de começar, pois encapsula todas as dependências.
    *   Existem várias imagens Docker para Spark disponíveis no Docker Hub (e.g., `jupyter/pyspark-notebook` que fornece um ambiente JupyterLab com PySpark pronto para uso, ou imagens oficiais do Bitnami ou Apache Spark).
    *   Exemplo para rodar um notebook PySpark com Docker:
        ```bash
        docker run -p 8888:8888 jupyter/pyspark-notebook
        ```
        Acesse o JupyterLab no link fornecido no terminal (geralmente com um token).
    *   Na Aula 4, detalharemos como usar Docker para nosso projeto final.

*   **Ambientes de Nuvem:** Plataformas como Databricks, AWS EMR, Google Dataproc, Azure Synapse Analytics oferecem ambientes Spark gerenciados.

**Iniciando uma `SparkSession` em PySpark:**

Independentemente de como você configurou o ambiente, o ponto de partida para qualquer aplicação PySpark (especialmente com DataFrames) é criar uma `SparkSession`.

```python
from pyspark.sql import SparkSession

# Cria uma SparkSession (ou obtem uma existente)
spark = SparkSession.builder \
    .appName("MinhaPrimeiraAppPySpark") \
    .master("local[*]")  # Executa localmente usando todos os cores disponiveis
    .getOrCreate()

print(f"SparkSession iniciada. Versao do Spark: {spark.version}")

# Seu codigo PySpark aqui...

# E importante parar a SparkSession ao final do seu script/notebook
# spark.stop()
```
*   `.appName("NomeDaApp")`: Define um nome para sua aplicação, que aparecerá na UI do Spark.
*   `.master("local[*]")`: Especifica o modo de execução. `local` executa o Spark em um único JVM (sem cluster). `local[K]` usa K threads. `local[*]` usa o número de cores disponíveis na máquina. Para rodar em um cluster, você especificaria a URL do mestre do cluster (e.g., `spark://host:port`, `yarn`, `mesos://host:port`).
*   `.getOrCreate()`: Retorna uma `SparkSession` existente ou cria uma nova se nenhuma existir.

##### **4.2 Resilient Distributed Datasets (RDDs) (20 minutos)**

RDDs foram a principal API do Spark antes dos DataFrames. Embora os DataFrames sejam geralmente preferidos para dados estruturados devido às suas otimizações, entender RDDs é útil pois eles são a base e ainda são usados para dados não estruturados ou quando se precisa de controle de baixo nível.

**O que são RDDs?**

Um RDD é uma coleção **imutável** e **distribuída** de objetos, particionada através dos nós de um cluster, que pode ser operada em **paralelo**. Suas principais características são:

*   **Resilientes (Tolerantes a Falhas):** O Spark rastreia a linhagem (lineage) das transformações usadas para construir cada RDD. Se uma partição de um RDD for perdida (e.g., devido à falha de um nó), o Spark pode reconstruí-la automaticamente a partir da linhagem.
*   **Distribuídos:** Os dados em um RDD são divididos em partições, e essas partições podem ser distribuídas e processadas em diferentes nós do cluster.
*   **Imutáveis:** Uma vez que um RDD é criado, ele não pode ser alterado. Transformações em um RDD criam um *novo* RDD.

**Criação de RDDs:**

1.  **A partir de Coleções Existentes (Paralelizando):**
    ```python
    data_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    rdd_from_list = spark.sparkContext.parallelize(data_list, numSlices=3) # numSlices e o numero de particoes (opcional)
    ```
2.  **A partir de Arquivos Externos:** Lendo arquivos de texto, SequenceFiles, ou outros formatos suportados pelo Hadoop.
    ```python
    # Suponha que voce tenha um arquivo 'meu_texto.txt' no HDFS ou localmente
    # rdd_from_file = spark.sparkContext.textFile("hdfs:///caminho/para/meu_texto.txt")
    # rdd_from_local_file = spark.sparkContext.textFile("file:///caminho/local/meu_texto.txt")
    ```

**Operações em RDDs:**

Existem dois tipos de operações em RDDs:

1.  **Transformações (Transformations):** Criam um novo RDD a partir de um existente. São "preguiçosas" (lazy evaluation), o que significa que não são executadas imediatamente, mas apenas quando uma ação é chamada.
    *   `map(func)`: Aplica uma função a cada elemento do RDD, retornando um novo RDD com os resultados.
        `rdd_mapeado = rdd_from_list.map(lambda x: x * 2)`
    *   `filter(func)`: Retorna um novo RDD contendo apenas os elementos que satisfazem uma condição (a função retorna True).
        `rdd_filtrado = rdd_mapeado.filter(lambda x: x > 10)`
    *   `flatMap(func)`: Similar ao `map`, mas cada elemento de entrada pode ser mapeado para 0 ou mais elementos de saída (a função deve retornar uma sequência).
    *   `distinct()`: Retorna um novo RDD com elementos distintos.
    *   `union(otherRDD)`: Retorna um novo RDD contendo todos os elementos de ambos os RDDs.
    *   `groupByKey()`: Agrupa valores por chave.
    *   `reduceByKey(func)`: Agrupa valores por chave e aplica uma função de redução.

2.  **Ações (Actions):** Calculam um resultado a partir de um RDD e o retornam ao programa driver ou o escrevem em um sistema de armazenamento externo. As ações disparam a execução das transformações.
    *   `collect()`: Retorna todos os elementos do RDD como uma lista no driver. Use com cuidado em RDDs grandes, pois pode estourar a memória do driver.
        `resultados = rdd_filtrado.collect()`
        `print(resultados) # Ex: [12, 14, 16, 18, 20]`
    *   `count()`: Retorna o número de elementos no RDD.
    *   `take(N)`: Retorna os primeiros N elementos do RDD.
    *   `first()`: Retorna o primeiro elemento do RDD.
    *   `foreach(func)`: Aplica uma função a cada elemento do RDD (geralmente para operações com efeito colateral, como salvar em um banco de dados).
    *   `saveAsTextFile(path)`: Salva o RDD como arquivos de texto no sistema de arquivos.

**Lazy Evaluation (Avaliação Preguiçosa):**

As transformações em RDDs são preguiçosas. O Spark não executa uma transformação até que uma ação seja chamada. Isso permite que o Spark otimize o plano de execução, agrupando operações ou evitando cálculos desnecessários.

##### **4.3 Introdução aos DataFrames no Spark (25 minutos)**

Embora os RDDs sejam poderosos, a API de **DataFrames** (introduzida no Spark 1.3 e aprimorada desde então) é geralmente a maneira preferida de trabalhar com dados estruturados e semi-estruturados no Spark. Ela oferece otimizações de performance significativas e uma API mais amigável, semelhante aos DataFrames do Pandas ou R.

**O que são DataFrames?**

Um DataFrame no Spark é uma coleção distribuída de dados organizada em colunas nomeadas. Conceitualmente, é equivalente a uma tabela em um banco de dados relacional ou a um DataFrame em Python (Pandas) ou R, mas com otimizações para processamento distribuído.

*   **Estrutura Tabular:** Os dados têm um schema (uma definição das colunas e seus tipos).
*   **Otimizações do Catalyst Optimizer:** As operações em DataFrames passam pelo Catalyst Optimizer do Spark, que pode aplicar otimizações sofisticadas (como reordenar operações, otimizar joins, empurrar filtros para a fonte de dados) para melhorar a performance.
*   **Construídos sobre RDDs:** Internamente, DataFrames são construídos sobre RDDs, mas com informações de schema adicionais.

**Criando DataFrames:**

1.  **A partir de RDDs Existentes (com um schema):**
    ```python
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType

    # Suponha um RDD de tuplas ou listas
    rdd_pessoas = spark.sparkContext.parallelize([("Alice", 34), ("Bob", 45), ("Charlie", 29)])
    
    schema_pessoas = StructType([
        StructField("Nome", StringType(), True),
        StructField("Idade", IntegerType(), True)
    ])
    
    df_pessoas_from_rdd = spark.createDataFrame(rdd_pessoas, schema=schema_pessoas)
    # Ou, de forma mais concisa, se o RDD for de Row objects ou com toDF()
    # df_pessoas_from_rdd = rdd_pessoas.toDF(["Nome", "Idade"])
    ```
2.  **A partir de Fontes de Dados Externas (CSV, JSON, Parquet, JDBC, etc.):**
    O Spark SQL pode ler dados de muitos formatos.
    ```python
    # Lendo um arquivo CSV (exemplo_vendas.csv que criamos)
    # Primeiro, vamos criar o arquivo CSV no ambiente do Spark se ele nao existir
    # (Em um cenario real, o arquivo estaria em HDFS, S3, ou localmente acessivel aos executors)
    # Para este exemplo, vamos assumir que ele esta no diretorio de trabalho do driver.
    # Se estiver usando Docker, pode precisar montar um volume ou copiar o arquivo para o container.

    # Criando um exemplo_vendas.csv para o Spark ler
    csv_content = """
    ID_Produto,Nome_Produto,Categoria,Preco_Unitario,Quantidade_Vendida,Data_Venda,ID_Cliente,Cidade_Cliente
    101,Laptop Modelo X,Eletronicos,3500.00,2,2023-10-15,C001,Sao Paulo
    102,Smartphone Y,Eletronicos,1200.50,5,2023-10-15,C002,Rio de Janeiro
    201,Livro de Ficcao,Livros,45.90,10,2023-10-16,C003,Belo Horizonte
    """
    with open("exemplo_vendas_spark.csv", "w") as f:
        f.write(csv_content)

    df_vendas = spark.read.csv("exemplo_vendas_spark.csv", header=True, inferSchema=True)
    # df_vendas_json = spark.read.json("arquivo.json")
    # df_vendas_parquet = spark.read.parquet("arquivo.parquet")
    ```
    *   `header=True`: Indica que a primeira linha do CSV é o cabeçalho.
    *   `inferSchema=True`: Tenta inferir os tipos de dados das colunas (pode ser custoso para arquivos grandes; é melhor fornecer o schema explicitamente em produção).

**Operações Básicas com DataFrames:**

DataFrames fornecem uma API rica para transformações de dados.

*   **Visualizando Dados e Schema:**
    *   `df.show(n=5, truncate=False)`: Mostra as primeiras `n` linhas do DataFrame. `truncate=False` mostra o conteúdo completo das colunas.
    *   `df.printSchema()`: Imprime o schema do DataFrame (nomes das colunas e tipos).
    *   `df.columns`: Retorna uma lista dos nomes das colunas.
    *   `df.count()`: Retorna o número de linhas.
    *   `df.describe().show()`: Calcula estatísticas descritivas básicas para colunas numéricas.

*   **Seleção de Colunas:**
    *   `df.select("Nome", "Idade").show()`
    *   `df.select(df["Nome"], (df["Idade"] + 2).alias("Idade_Mais_2")).show()` (usando expressões de coluna)

*   **Filtragem de Linhas:**
    *   `df.filter(df["Idade"] > 30).show()`
    *   `df.where("Idade > 30 AND Nome LIKE 'A%'").show()` (usando uma string SQL)

*   **Adição ou Modificação de Colunas:**
    *   `df_com_nova_coluna = df.withColumn("Adulto", df["Idade"] >= 18)`
    *   `df_coluna_renomeada = df.withColumnRenamed("Idade", "Anos_De_Vida")`

*   **Agregação (GroupBy):**
    *   `df_vendas.groupBy("Categoria").count().show()` (Conta o número de produtos por categoria)
    *   `df_vendas.groupBy("Categoria").agg({"Preco_Unitario": "avg", "Quantidade_Vendida": "sum"}).show()` (Calcula o preço médio e a soma da quantidade vendida por categoria)

*   **Junções (Joins):**
    *   `df1.join(df2, df1["id_comum"] == df2["id_comum"], "inner").show()`

*   **Ordenação:**
    *   `df.orderBy("Idade", ascending=False).show()`

*   **Executando Consultas SQL:**
    Você pode registrar um DataFrame como uma tabela temporária e então executar consultas SQL sobre ele.
    ```python
    df_vendas.createOrReplaceTempView("tabela_vendas_temp")
    df_resultado_sql = spark.sql("SELECT Categoria, count(*) as total FROM tabela_vendas_temp GROUP BY Categoria")
    df_resultado_sql.show()
    ```

**Comparação entre RDDs e DataFrames:**

| Característica      | RDD                                       | DataFrame                                       |
| ------------------- | ----------------------------------------- | ----------------------------------------------- |
| **Abstração**       | Coleção distribuída de objetos JVM        | Coleção distribuída de dados em colunas nomeadas |
| **Schema**          | Sem schema inerente (dados não estruturados) | Possui schema (dados estruturados/semi)       |
| **Otimização**      | Otimizações limitadas                     | Otimizações ricas via Catalyst Optimizer        |
| **Segurança de Tipo** | Verificação em tempo de execução (Python)   | Verificação em tempo de análise (parcial em Py) |
| **API**             | Funcional (map, filter, etc.)             | Declarativa (select, groupBy, SQL) e funcional  |
| **Performance**     | Geralmente mais lento para dados estruturados | Geralmente mais rápido devido a otimizações   |
| **Uso**             | Dados não estruturados, controle baixo nível | Dados estruturados, SQL, alta performance       |

Em geral, para a maioria das tarefas de processamento de dados estruturados em PySpark, **DataFrames são a API recomendada** devido à sua performance e facilidade de uso.

**Atividade Prática 3: Primeiros Passos com PySpark - RDDs e DataFrames**

*   **Objetivo:** Familiarizar-se com a criação e manipulação básica de RDDs e DataFrames no Apache Spark usando PySpark, incluindo a leitura de um arquivo CSV.
*   **Formato:** Exercício prático individual, guiado. O instrutor pode fornecer um ambiente Spark via Docker ou instruções para configuração local básica.
*   **Pré-requisitos:** Acesso a um ambiente PySpark (Jupyter Notebook com PySpark, script Python executado com `spark-submit`, ou um contêiner Docker com Spark).
*   **Passos da Tarefa:**

    1.  **Configurar SparkSession (5 minutos):**
        *   Inicie um script Python ou um notebook Jupyter.
        *   Importe `SparkSession` de `pyspark.sql`.
        *   Crie uma `SparkSession`:
            ```python
            from pyspark.sql import SparkSession

            spark = SparkSession.builder \
                .appName("AtividadePySpark") \
                .master("local[*]") \
                .getOrCreate()
            
            print("SparkSession criada!")
            ```

    2.  **Trabalhando com RDDs (10 minutos):**
        *   Crie um RDD a partir de uma lista de números: `numeros_lista = list(range(1, 21))`
          `numeros_rdd = spark.sparkContext.parallelize(numeros_lista)`
        *   Aplique uma transformação `map` para obter o quadrado de cada número: `quadrados_rdd = numeros_rdd.map(lambda x: x * x)`
        *   Aplique uma transformação `filter` para manter apenas os quadrados que são pares: `quadrados_pares_rdd = quadrados_rdd.filter(lambda x: x % 2 == 0)`
        *   Execute uma ação `collect` para ver os resultados e imprima-os: `resultados_rdd = quadrados_pares_rdd.collect()`
          `print(f"Quadrados pares (RDD): {resultados_rdd}")`

    3.  **Trabalhando com DataFrames (15 minutos):**
        *   **Criar o arquivo `exemplo_vendas_spark.csv`** (se ainda não existir no diretório de trabalho do Spark) com o seguinte conteúdo:
            ```csv
            ID_Produto,Nome_Produto,Categoria,Preco_Unitario,Quantidade_Vendida,Data_Venda,ID_Cliente,Cidade_Cliente
            101,Laptop Modelo X,Eletronicos,3500.00,2,2023-10-15,C001,Sao Paulo
            102,Smartphone Y,Eletronicos,1200.50,5,2023-10-15,C002,Rio de Janeiro
            201,Livro de Ficcao,Livros,45.90,10,2023-10-16,C003,Belo Horizonte
            101,Laptop Modelo X,Eletronicos,3500.00,1,2023-10-17,C004,Sao Paulo
            301,Cafe Especial,Alimentos,25.00,20,2023-10-17,C001,Sao Paulo
            ```
        *   Leia o arquivo CSV para um DataFrame, inferindo o schema e usando o cabeçalho:
            `df_vendas = spark.read.csv("exemplo_vendas_spark.csv", header=True, inferSchema=True)`
        *   Mostre as primeiras 5 linhas do DataFrame e seu schema:
            `print("Schema do DataFrame de Vendas:")`
            `df_vendas.printSchema()`
            `print("Dados do DataFrame de Vendas:")`
            `df_vendas.show(truncate=False)`
        *   Selecione as colunas `Nome_Produto`, `Categoria` e `Preco_Unitario`:
            `df_selecionado = df_vendas.select("Nome_Produto", "Categoria", "Preco_Unitario")`
            `print("Colunas selecionadas:")`
            `df_selecionado.show()`
        *   Filtre as vendas onde a `Categoria` é "Eletronicos":
            `df_eletronicos = df_vendas.filter(df_vendas.Categoria == "Eletronicos")`
            `print("Vendas de Eletronicos:")`
            `df_eletronicos.show()`
        *   Adicione uma nova coluna `Valor_Total` calculada como `Preco_Unitario * Quantidade_Vendida` (Cuidado: `Preco_Unitario` pode ser string, precisa converter se `inferSchema` não funcionou perfeitamente ou se for o caso):
            `from pyspark.sql.functions import col`
            `df_com_total = df_vendas.withColumn("Valor_Total", col("Preco_Unitario") * col("Quantidade_Vendida"))`
            `print("Vendas com Valor Total:")`
            `df_com_total.show()`
        *   Calcule a quantidade total de produtos vendidos por `Categoria`:
            `df_agregado = df_com_total.groupBy("Categoria").sum("Quantidade_Vendida", "Valor_Total")`
            `print("Agregacao por Categoria:")`
            `df_agregado.show()`

    4.  **Encerrar a SparkSession (Opcional, mas boa prática em scripts) (5 minutos):**
        *   `spark.stop()`
        *   `print("SparkSession encerrada.")`

*   **Entregável Esperado:**
    *   O script Python (`.py`) ou notebook Jupyter (`.ipynb`) contendo todo o código PySpark da atividade.
    *   Capturas de tela ou a saída impressa no console mostrando os resultados das operações com RDDs e DataFrames (especialmente as saídas dos comandos `print()` e `.show()`).

Esta atividade prática fornecerá uma base sólida para trabalhar com PySpark, que será expandida na Aula 4, onde integraremos Spark com Docker e dbt para um pipeline de limpeza de dados mais completo.

### Conclusão da Aula 3

Nesta aula, mergulhamos em funcionalidades avançadas do dbt, como macros e testes singulares, e aprendemos sobre as melhores práticas para organizar e gerenciar projetos dbt. Em seguida, fizemos uma introdução abrangente ao Apache Spark, cobrindo sua arquitetura, ecossistema e, crucialmente, demos nossos primeiros passos práticos com PySpark, trabalhando com RDDs e DataFrames. Essas ferramentas e conceitos são pilares da engenharia de dados moderna. Na Aula 4, vamos convergir esses aprendizados, focando em como usar Spark para limpeza de dados mais complexa, como conteinerizar aplicações Spark com Docker e como o dbt pode se encaixar nesse cenário.

---
**Fim do Material da Aula 3.**

