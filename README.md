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

## Run Dagster locally

`dagster/` holds only tracked config (`dagster.yaml`, `workspace.yaml`); all
runtime state (run/event history, logs) lives in `.dagster_home/`, which is
gitignored and must not be the same directory as `dagster/`.

One-time setup:

```bash
cp .env.example .env      # then edit the paths inside

mkdir -p .dagster_home
ln -s ../dagster/dagster.yaml .dagster_home/dagster.yaml

# One-time GCP auth: lets local runs write to BigQuery as the project's
# service account, without a long-lived key file.
gcloud iam service-accounts add-iam-policy-binding \
  fraud-mlops-sa-dev@fraud-detection-504617.iam.gserviceaccount.com \
  --member="user:$(gcloud config get-value account)" \
  --role="roles/iam.serviceAccountTokenCreator"

gcloud auth application-default login \
  --impersonate-service-account=fraud-mlops-sa-dev@fraud-detection-504617.iam.gserviceaccount.com

mkdir -p "$(dirname "$(grep GOOGLE_APPLICATION_CREDENTIALS .env | cut -d= -f2)")"
cp ~/.config/gcloud/application_default_credentials.json \
   "$(grep GOOGLE_APPLICATION_CREDENTIALS .env | cut -d= -f2)"
```

Then, any time:

```bash
uv run dagster dev -w dagster/workspace.yaml -p 3000
```

## Infrastructure docs

For Terraform/OpenTofu backend setup (including state bucket bootstrap), see:

- [iaac/README.md](iaac/README.md)

## Current status

The project foundation is initialized:

- repository structure created
- uv environment and dependencies configured
- GCS backend bucket configured for IaC state
