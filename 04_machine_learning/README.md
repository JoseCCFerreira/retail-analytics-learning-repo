# 04 - Machine Learning

Objetivo: usar os dados de retalho para prever valores e descobrir padroes.

Biblioteca usada neste projeto: scikit-learn.

Stack opcional para estudo aprofundado:

- PyTorch
- TensorFlow / Keras
- Jupyter
- MLflow
- Optuna
- SHAP

Ficheiro principal:

- `../python/ml_retail.py`

Como executar:

```bash
python python/run_pipeline.py
python python/ml_retail.py
```

Resultados esperados:

```text
data/ml_outputs/model_metrics.json
data/ml_outputs/regression_feature_importance.csv
data/ml_outputs/cluster_assignments.csv
data/ml_outputs/pca_projections.csv
```

Modelos usados:

- Regressao Linear: baseline simples para prever `net_amount`.
- Decision Tree Regressor: cria regras de decisao interpretaveis.
- Random Forest Regressor: junta varias arvores para melhorar estabilidade.
- Gradient Boosting Regressor: aprende erros sucessivos para melhorar previsao.
- Logistic Regression: classifica o segmento do cliente.
- Decision Tree Classifier: classifica com regras simples.
- Random Forest Classifier: classifica com varias arvores.
- K-Means: cria grupos de clientes parecidos.
- DBSCAN: encontra grupos por densidade e identifica outliers.
- PCA: reduz variaveis para 2 dimensoes, util para visualizacao.

Documento complementar para aprendizagem mais avancada:

- `../docs/aprender_deep_learning_pytorch_tensorflow.html`
- `../docs/executar_pytorch_tensorflow_retail.html`
- `../deep_learning/README.md`

Ideia simples:

- Regressao responde: quanto pode valer uma venda?
- Classificacao responde: que tipo de cliente e este?
- Clustering responde: que grupos naturais existem nos clientes?
- PCA responde: como consigo visualizar muitos atributos num grafico 2D?
