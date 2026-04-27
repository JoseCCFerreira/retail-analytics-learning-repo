# 05 - Governance With dbt

Objetivo: governar transformacoes de dados com SQL versionado, testado e documentavel.

Ferramenta usada neste projeto: dbt com DuckDB.

Ficheiros principais:

- `../dbt_retail_analytics/dbt_project.yml`: configuracao do projeto dbt.
- `../dbt_retail_analytics/profiles.example.yml`: exemplo de ligacao ao DuckDB.
- `../dbt_retail_analytics/models/staging/`: modelos de preparacao.
- `../dbt_retail_analytics/models/marts/`: modelos finais de analise.
- `../dbt_retail_analytics/models/schema.yml`: testes de qualidade.

Como executar:

```bash
python python/run_pipeline.py
cd dbt_retail_analytics
export DBT_PROFILES_DIR=.
cp profiles.example.yml profiles.yml
dbt run
dbt test
```

No Windows PowerShell:

```powershell
python python/run_pipeline.py
cd dbt_retail_analytics
Copy-Item profiles.example.yml profiles.yml
$env:DBT_PROFILES_DIR="."
dbt run
dbt test
```

Ideia simples:

- dbt transforma dados com SQL.
- `staging` limpa e normaliza nomes/tipos.
- `marts` cria tabelas finais para analise.
- `schema.yml` define testes como `not_null`, `unique` e relacionamentos.
- Isto ajuda a evitar dashboards e modelos ML assentes em dados errados.

