# Retail Analytics Study Case

![Cross Platform Validate](https://github.com/JoseCCFerreira/retail-analytics-learning-repo/actions/workflows/cross-platform-validate.yml/badge.svg)

End-to-end learning repository for retail data engineering, analytics, machine learning, visualization, and repository publishing.

GitHub repository:
- `https://github.com/JoseCCFerreira/retail-analytics-learning-repo`

It includes:
- synthetic retail data generation;
- OLTP storage in SQLite;
- analytical storage in DuckDB;
- dbt staging and marts;
- supervised and unsupervised machine learning;
- Streamlit dashboards;
- PL/SQL and OLAP/OLTP study material;
- a realistic Black Friday test case;
- a cross-platform validation workflow for Windows, macOS, and Linux.

## Project Goals

This repository is meant to help you study and demonstrate:
- data ingestion and local analytics pipelines;
- dimensional modeling with dbt;
- machine learning over retail transactions;
- data visualization with Streamlit and Plotly;
- validation, packaging, and GitHub publication workflows.

## Main Entry Points

- Project hub: [index.html](index.html)
- Handbook: [docs/retail_analytics_handbook.html](docs/retail_analytics_handbook.html)
- Usage and theory tutorial: [docs/tutorial_utilizacao_teoria_retail.html](docs/tutorial_utilizacao_teoria_retail.html)
- Theory, code, and outputs: [docs/explicacao_teorica_codigo_outputs.html](docs/explicacao_teorica_codigo_outputs.html)
- ML experiments, comparison, visualizations, and 2-year data: [docs/ml_experimentos_visualizacoes_2anos.html](docs/ml_experimentos_visualizacoes_2anos.html)
- Code and documentation uniformization: [docs/uniformizacao_codigo_documentacao.html](docs/uniformizacao_codigo_documentacao.html)
- Single beginner document: [docs/retail_case_documento_unico.html](docs/retail_case_documento_unico.html)
- Build-it-yourself document: [docs/criar_tudo_do_zero_retail_case.html](docs/criar_tudo_do_zero_retail_case.html)
- Deep Learning learning path: [docs/aprender_deep_learning_pytorch_tensorflow.html](docs/aprender_deep_learning_pytorch_tensorflow.html)
- Deep Learning execution guide: [docs/executar_pytorch_tensorflow_retail.html](docs/executar_pytorch_tensorflow_retail.html)
- Full ML guide: [docs/machine_learning_guide.html](docs/machine_learning_guide.html)
- Full project guide: [docs/guia_completo.html](docs/guia_completo.html)
- Repository publishing guide: [learning/modules/repository-setup/criar-repositorio-retail-analytics.html](learning/modules/repository-setup/criar-repositorio-retail-analytics.html)

## Repository Structure

```text
.
├── 01_oltp_database/                   # Beginner module for SQLite OLTP
├── 02_olap_database/                   # Beginner module for DuckDB OLAP
├── 03_visualizations_streamlit/        # Beginner module for Streamlit dashboards
├── 04_machine_learning/                # Beginner module for ML models
├── 05_governance_dbt/                  # Beginner module for dbt governance
├── .github/workflows/                  # Cross-platform CI validation
├── data/
│   └── test_cases/                     # Curated realistic test case assets
├── dbt_retail_analytics/               # dbt project
├── docs/                               # HTML learning and project documentation
├── learning/modules/repository-setup/  # Interactive guide for publishing this repo
├── python/                             # Data, ML, validation, and case scripts
├── scripts/                            # Packaging scripts
├── sql/                                # SQLite, DuckDB, and Oracle PL/SQL concepts
├── streamlit/                          # Dashboards
├── index.html                          # Central project hub
├── package.json                        # Helper scripts
└── requirements.txt                    # Python dependencies
```

## Quick Start

### Windows PowerShell

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### macOS / Linux

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### GitHub CLI (`gh`)

Se `gh` não existir no teu sistema, tens duas opções:

Com Homebrew:

```bash
brew install gh
```

Sem Homebrew, instalação local no utilizador:

```bash
mkdir -p ~/.local/gh ~/bin
cd ~/.local/gh
curl -fL -o gh.zip https://github.com/cli/cli/releases/download/v2.91.0/gh_2.91.0_macOS_arm64.zip
unzip -o gh.zip
ln -sf ~/.local/gh/gh_2.91.0_macOS_arm64/bin/gh ~/bin/gh
printf '\nexport PATH="$HOME/bin:$PATH"\n' >> ~/.zprofile
```

Depois abre um novo terminal e valida:

```bash
gh --version
gh auth login
```

## Core Workflows

### 1. Generate the base data pipeline

```bash
python python/run_pipeline.py
```

This runs:
- `python/generate_data.py`
- `python/setup_sqlite.py`
- `python/setup_duckdb.py`

Expected result:
- `data/retail_app.db`
- `data/retail_analytics.duckdb`
- generated CSVs under `data/`

### 2. Run dbt models and tests

Before running dbt manually, prepare your profile using [dbt_retail_analytics/profiles.example.yml](dbt_retail_analytics/profiles.example.yml).

```bash
cd dbt_retail_analytics
dbt run
dbt test
```

### 3. Run machine learning

```bash
python python/ml_retail.py
```

### 3b. Optional deep learning stack for study

```bash
pip install -r requirements_deep_learning.txt
```

Study guide:
- [docs/aprender_deep_learning_pytorch_tensorflow.html](docs/aprender_deep_learning_pytorch_tensorflow.html)
- [docs/executar_pytorch_tensorflow_retail.html](docs/executar_pytorch_tensorflow_retail.html)

Expected result:
- `data/ml_outputs/model_metrics.json`
- `data/ml_outputs/regression_feature_importance.csv`
- `data/ml_outputs/cluster_assignments.csv`
- `data/ml_outputs/pca_projections.csv`

### 4. Run the main dashboard

```bash
streamlit run streamlit/app.py
```

### 5. Run the realistic Black Friday case

```bash
python python/generate_test_case_black_friday.py
python python/analyze_test_case_black_friday.py
streamlit run streamlit/case_black_friday_app.py
```

Expected signals in this case:
- Black Friday revenue spike;
- higher discount pressure during campaign days;
- higher MBWay share after campaign launch;
- visible category mix shift.

## Validation

### Quick validation

```bash
python python/validate_project.py
```

### Full validation including dbt

```bash
python python/validate_project.py --include-dbt
```

### npm helper commands

```bash
npm run docs:serve
npm run ml:run
npm run package:zip
npm run pipeline:full
npm run case:generate
npm run case:analyze
npm run case:dashboard
npm run validate:quick
npm run validate:full
```

## CI Status

The badge at the top of this README already points to the live GitHub Actions workflow for this published repository.

## Cross-Platform CI

A GitHub Actions workflow is included in [cross-platform-validate.yml](.github/workflows/cross-platform-validate.yml).

It validates the repository on:
- Windows
- macOS
- Linux

The workflow installs dependencies and runs:

```bash
python python/validate_project.py --include-dbt
```

## Publish This Repository to GitHub

The full interactive guide is here:
- [learning/modules/repository-setup/criar-repositorio-retail-analytics.html](learning/modules/repository-setup/criar-repositorio-retail-analytics.html)

### Recommended `gh` flow

Authenticate first:

```bash
gh auth login
gh auth setup-git
```

Create a new repository from the current folder and push immediately:

```bash
gh repo create retail-analytics-learning-repo \
  --public \
  --description "Retail analytics study case with data pipeline, dbt, ML, Streamlit, and realistic Black Friday test case" \
  --source . \
  --remote origin \
  --push
```

If you want a private repository instead:

```bash
gh repo create retail-analytics-learning-repo \
  --private \
  --description "Retail analytics study case with data pipeline, dbt, ML, Streamlit, and realistic Black Friday test case" \
  --source . \
  --remote origin \
  --push
```

Open the published repository in the browser:

```bash
gh repo view --web
```

## Packaging a Release ZIP

Create a clean ZIP package locally:

```bash
npm run package:zip
```

Generated file:
- `dist/retail_analytics_learning_repo-package.zip`

The package excludes local-only content such as:
- `.venv/`
- `.git/`
- `dist/`
- cache folders
- temporary dbt profile directories

## Screenshots

### Main Dashboard

Run with `streamlit run streamlit/app.py`.

Shows:
- total revenue, total transactions, average basket value, and unique products KPIs;
- monthly revenue trend (line chart);
- top 10 products by revenue (bar chart);
- raw transactions table.

### Black Friday Case Dashboard

Run with `streamlit run streamlit/case_black_friday_app.py`.

Shows:
- baseline vs campaign KPI comparison (revenue, transactions, average basket, discount ratio);
- daily revenue and discount rate series with campaign period highlighted;
- payment method mix comparison (MBWay shift during campaign);
- category revenue mix comparison.

To generate the case data before opening the dashboard:

```bash
python python/generate_test_case_black_friday.py
```

## Notes

- The Oracle PL/SQL files are educational and conceptual. They are not wired into the Python runtime.
- Root-level generated CSV and DuckDB files are ignored by Git. Curated test-case assets under `data/test_cases/` are allowed in version control.
- The central hub in [index.html](index.html) is the best place to navigate the project end-to-end.
