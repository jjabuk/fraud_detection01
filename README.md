# fraud_detection01

End-to-end MLOps project for fraud detection (IEEE-CIS) on Google Cloud Platform.

## Overview

This repository is being built as a production-style workflow with:

- data ingestion and feature engineering
- model training and experiment tracking
- model serving as an API
- infrastructure as code and CI/CD

Planned core stack:

- Dagster
- LightGBM / scikit-learn
- MLflow
- FastAPI
- OpenTofu (Terraform)
- BigQuery
- Cloud Run

## Data source

The project uses the Kaggle competition dataset:

- IEEE-CIS Fraud Detection: https://www.kaggle.com/c/ieee-fraud-detection

Notes:

- You need a Kaggle account and competition access to download files.
- Raw files are not committed to the repository.

Optional download flow with Kaggle API:

```bash
uv tool install kaggle
mkdir -p ~/.kaggle
# place kaggle.json in ~/.kaggle and set secure permissions
chmod 600 ~/.kaggle/kaggle.json

mkdir -p data/raw
kaggle competitions download -c ieee-fraud-detection -p data/raw
unzip data/raw/ieee-fraud-detection.zip -d data/raw
```

## Repository layout

- `data/` - local data artifacts (if needed)
- `notebooks/` - exploration notebooks
- `src/` - application and pipeline source code
- `tests/` - unit/integration tests
- `iaac/` - infrastructure as code
- `docs/` - project architecture and implementation plan

## Quick start (local)

1. Create or activate the uv environment:

```bash
uv venv
source .venv/bin/activate
```

2. Install dependencies (if not installed yet):

```bash
uv sync
```

3. Run checks:

```bash
uv run ruff check .
uv run pytest
```

## Infrastructure docs

For Terraform/OpenTofu backend setup (including state bucket bootstrap), see:

- [iaac/README.md](iaac/README.md)

## Current status

The project foundation is initialized:

- repository structure created
- uv environment and dependencies configured
- GCS backend bucket configured for IaC state
