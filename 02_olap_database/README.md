# 02 - OLAP Database

Objetivo: preparar os dados para analise, dashboards, metricas e perguntas agregadas.

Motor usado neste projeto: DuckDB.

Ficheiros principais:

- `../python/setup_duckdb.py`: cria a base analitica `../data/retail_analytics.duckdb`.
- `../sql/duckdb/01_analytical_queries.sql`: consultas OLAP com agregacoes e ranking.
- `../dbt_retail_analytics/models/marts/fct_sales.sql`: tabela de factos governada por dbt.
- `../dbt_retail_analytics/models/marts/dim_products.sql`: dimensao de produtos.

Como executar:

```bash
python python/generate_data.py
python python/setup_duckdb.py
```

Resultado esperado:

```text
data/retail_analytics.duckdb
```

Ideia simples:

- OLAP e a base para analisar muitos dados de uma vez.
- A tabela `fct_sales` e a tabela central de vendas.
- As dimensoes, como produtos, clientes e lojas, dao contexto as vendas.
- E boa para perguntas como receita por mes, top produtos e comportamento por segmento.

