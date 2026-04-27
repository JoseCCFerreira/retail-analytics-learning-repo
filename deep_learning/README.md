# Deep Learning

Esta pasta contem exemplos praticos para aprender Deep Learning sobre o dataset retail do projeto.

Ficheiros principais:

- `common_retail_dl.py`: prepara dados e features.
- `pytorch_retail_examples.py`: exemplos de regressao e classificacao com PyTorch.
- `tensorflow_retail_examples.py`: exemplos de regressao e classificacao com TensorFlow / Keras.

Sequencia recomendada:

1. `python python/run_pipeline.py`
2. `python python/ml_retail.py`
3. `pip install -r requirements_deep_learning.txt`
4. `python deep_learning/pytorch_retail_examples.py`
5. `python deep_learning/tensorflow_retail_examples.py`

Outputs esperados:

- `data/deep_learning_outputs/pytorch_metrics.json`
- `data/deep_learning_outputs/tensorflow_metrics.json`
- modelos guardados em `data/deep_learning_outputs/`

