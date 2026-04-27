# 01 - OLTP Database

Objetivo: guardar as operacoes do negocio como se fosse a base de dados da aplicacao da loja.

Motor usado neste projeto: SQLite.

Ficheiros principais:

- `../python/generate_data.py`: cria CSVs com clientes, produtos, lojas, transacoes e linhas de venda.
- `../python/setup_sqlite.py`: cria a base `../data/retail_app.db`.
- `../sql/sqlite/01_create_tables.sql`: cria as tabelas OLTP.
- `../sql/sqlite/02_indexes.sql`: cria indices para acelerar pesquisas.
- `../sql/sqlite/03_business_queries.sql`: exemplos de perguntas de negocio.

Como executar:

```bash
python python/generate_data.py
python python/setup_sqlite.py
```

Resultado esperado:

```text
data/retail_app.db
```

Ideia simples:

- OLTP e a base do dia a dia.
- Tem chaves primarias e chaves estrangeiras.
- Guarda eventos detalhados: uma transacao, o cliente, a loja e os produtos comprados.
- E boa para inserir, alterar e consultar registos individuais.

