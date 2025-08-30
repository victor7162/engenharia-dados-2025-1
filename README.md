# Projeto Final - Pipeline de Dados com Docker e dbt

Este repositório contém o projeto final da disciplina de Engenharia de Dados, que consiste em um pipeline de dados completo para processar e analisar dados de pagamento de uma plataforma de e-commerce.

**Autor:** Victor [Seu Sobrenome aqui]

---

## Tecnologias Utilizadas

* **Docker & Docker Compose:** Para orquestração e virtualização dos containers.
* **PostgreSQL:** Como Data Warehouse para armazenar os dados brutos e transformados.
* **dbt (Data Build Tool):** Para a modelagem e transformação dos dados (ELT).

---

## Estrutura do Pipeline de Dados

O pipeline foi desenvolvido seguindo a metodologia de camadas (Bronze, Silver, Gold) para garantir a qualidade, rastreabilidade e manutenibilidade dos dados.

### Bronze (Dados Brutos)
* **Fonte:** Arquivos CSV (`raw_clientes.csv`, `raw_pedidos.csv`, `raw_pagamentos.csv`).
* **Processo:** Os dados são carregados no Data Warehouse através do comando `dbt seed`, criando uma cópia fiel da origem.

### Silver (Dados Limpos e Integrados)
* **Fonte:** Tabelas da camada Bronze.
* **Transformações:** Modelos de *staging* (`stg_*.sql`) são aplicados para:
    * Padronizar e renomear colunas para um padrão de negócios.
    * Converter tipos de dados (ex: valor de centavos para unidade monetária).
    * Aplicar filtros de negócio (ex: considerar apenas pedidos com status `completed`).

### Gold (Dados Agregados para Análise)
* **Fonte:** Modelos da camada Silver.
* **Transformação:** Um modelo de *mart* (`mart_valor_pago_por_cliente.sql`) é criado para realizar as junções e agregações finais, gerando uma tabela com o valor total pago por cada cliente, pronta para consumo analítico.

---

## Como Executar o Projeto

1.  **Pré-requisitos:** Ter Docker e Docker Desktop instalados.

2.  **Clonar o repositório:**
    ```bash
    git clone [https://github.com/victor7162/engenharia-dados-2025-1.git](https://github.com/victor7162/engenharia-dados-2025-1.git)
    cd engenharia-dados-2025-1/dbt_compose
    ```

3.  **Iniciar os containers:**
    ```bash
    docker-compose up -d --build
    ```

4.  **Acessar o container do dbt:**
    ```bash
    docker-compose exec dbt bash
    ```

5.  **Executar o pipeline de dados (dentro do container):**
    ```bash
    dbt deps
    dbt seed
    dbt build
    ```

6.  **Verificar o resultado:** Conectar a um cliente de banco de dados (como DBeaver) no `localhost:5432` (usuário/senha: `postgres`) e consultar a tabela `mart_valor_pago_por_cliente`.