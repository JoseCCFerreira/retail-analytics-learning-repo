# 03 - Visualizations With Streamlit

Objetivo: transformar os dados em graficos e indicadores visuais.

Ferramenta usada neste projeto: Streamlit.

Ficheiros principais:

- `../streamlit/app.py`: dashboard principal.
- `../streamlit/case_black_friday_app.py`: dashboard do caso Black Friday.
- `../data/retail_analytics.duckdb`: base lida pelos dashboards.

Como executar:

```bash
python python/run_pipeline.py
streamlit run streamlit/app.py
```

Resultado esperado:

- Uma pagina local no browser.
- KPIs de transacoes, receita e valor medio.
- Grafico de receita mensal.
- Ranking de produtos.

Ideia simples:

- Streamlit permite criar uma aplicacao de dados em Python.
- O dashboard le o DuckDB, executa SQL e mostra graficos Plotly.
- Serve para apresentar conclusoes sem obrigar o utilizador a escrever SQL.

