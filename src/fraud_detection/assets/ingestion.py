from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pandas as pd
from google.cloud import bigquery

from dagster import (
    AssetCheckResult,
    Failure,
    Field,
    MaterializeResult,
    asset,
    asset_check,
)

INGESTION_DEFAULTS: dict[str, Any] = {
    "source": "local",
    "local_csv_path": "data/raw/train_transaction_sample.csv",
    "kaggle_csv_path": "data/raw/train_transaction.csv",
    "github_csv_url": "",
    "gcp_project_id": os.getenv("GCP_PROJECT_ID", "fraud-detection-504617"),
    "bq_dataset": os.getenv("BQ_RAW_DATASET", "raw"),
    "bq_table": os.getenv("BQ_RAW_TABLE", "ieee_train_transaction_raw"),
    "write_disposition": "WRITE_TRUNCATE",
    "sample_n_rows": 0,
    "required_columns": ["TransactionID", "TransactionDT", "TransactionAmt"],
    "fraud_label_column": "isFraud",
}

INGESTION_CONFIG_SCHEMA = {
    "source": Field(
        str,
        default_value=INGESTION_DEFAULTS["source"],
        is_required=False,
        description="Data source: local, kaggle, or github.",
    ),
    "local_csv_path": Field(
        str,
        default_value=INGESTION_DEFAULTS["local_csv_path"],
        is_required=False,
    ),
    "kaggle_csv_path": Field(
        str,
        default_value=INGESTION_DEFAULTS["kaggle_csv_path"],
        is_required=False,
    ),
    "github_csv_url": Field(
        str,
        default_value=INGESTION_DEFAULTS["github_csv_url"],
        is_required=False,
    ),
    "gcp_project_id": Field(
        str,
        default_value=INGESTION_DEFAULTS["gcp_project_id"],
        is_required=False,
    ),
    "bq_dataset": Field(
        str,
        default_value=INGESTION_DEFAULTS["bq_dataset"],
        is_required=False,
    ),
    "bq_table": Field(
        str,
        default_value=INGESTION_DEFAULTS["bq_table"],
        is_required=False,
    ),
    "write_disposition": Field(
        str,
        default_value=INGESTION_DEFAULTS["write_disposition"],
        is_required=False,
        description="BigQuery write mode: WRITE_TRUNCATE or WRITE_APPEND.",
    ),
    "sample_n_rows": Field(
        int,
        default_value=INGESTION_DEFAULTS["sample_n_rows"],
        is_required=False,
        description="Optional row cap for local tests. Use 0 for all rows.",
    ),
    "required_columns": Field(
        [str],
        default_value=INGESTION_DEFAULTS["required_columns"],
        is_required=False,
    ),
    "fraud_label_column": Field(
        str,
        default_value=INGESTION_DEFAULTS["fraud_label_column"],
        is_required=False,
    ),
}


def _resolve_config(runtime_config: dict[str, Any] | None) -> dict[str, Any]:
    cfg = dict(INGESTION_DEFAULTS)
    if runtime_config:
        cfg.update(runtime_config)
    return cfg


def _read_source_csv(config: dict[str, Any]) -> pd.DataFrame:
    source = config["source"]
    if source == "local":
        source_path = Path(config["local_csv_path"])
        if not source_path.exists():
            raise Failure(
                f"Local CSV not found at {source_path}. "
                "Download data from Kaggle first or change local_csv_path."
            )
        return pd.read_csv(source_path)

    if source == "kaggle":
        source_path = Path(config["kaggle_csv_path"])
        if not source_path.exists():
            raise Failure(
                f"Kaggle CSV not found at {source_path}. "
                "Expected a pre-downloaded file from Kaggle competition."
            )
        return pd.read_csv(source_path)

    if source == "github":
        if not config["github_csv_url"]:
            raise Failure("github_csv_url must be provided when source='github'.")
        return pd.read_csv(config["github_csv_url"])

    raise Failure("source must be one of: local, kaggle, github.")


def _validate_schema(df: pd.DataFrame, config: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
    required = list(config["required_columns"])
    missing_columns = [col for col in required if col not in df.columns]

    numeric_issues: list[str] = []
    for col in required:
        if col in df.columns and not pd.api.types.is_numeric_dtype(df[col]):
            numeric_issues.append(col)

    label_column = config["fraud_label_column"]
    fraud_label_issues: list[str] = []
    if label_column in df.columns:
        unique_values = sorted(set(df[label_column].dropna().unique().tolist()))
        if any(v not in (0, 1) for v in unique_values):
            fraud_label_issues.append(
                f"{label_column} contains values outside {{0,1}}: {unique_values}"
            )

    passed = not (missing_columns or numeric_issues or fraud_label_issues)
    metadata: dict[str, Any] = {
        "rows": len(df),
        "columns": len(df.columns),
        "missing_columns": missing_columns,
        "non_numeric_required_columns": numeric_issues,
        "fraud_label_issues": fraud_label_issues,
    }
    return passed, metadata


@asset(group_name="ingestion", config_schema=INGESTION_CONFIG_SCHEMA)
def raw_transactions_dataframe(context) -> pd.DataFrame:
    cfg = _resolve_config(context.op_config)
    df = _read_source_csv(cfg)

    sample_n_rows = int(cfg["sample_n_rows"])
    if sample_n_rows > 0:
        df = df.head(sample_n_rows)

    context.log.info(
        "Loaded raw CSV with %s rows and %s columns from source '%s'.",
        len(df),
        len(df.columns),
        cfg["source"],
    )
    return df


@asset_check(asset=raw_transactions_dataframe, config_schema=INGESTION_CONFIG_SCHEMA)
def raw_transactions_schema_check(
    context,
    raw_transactions_dataframe: pd.DataFrame,
) -> AssetCheckResult:
    cfg = _resolve_config(context.op_config)
    passed, metadata = _validate_schema(raw_transactions_dataframe, cfg)
    return AssetCheckResult(
        passed=passed,
        metadata=metadata,
        description="Validate required schema before loading raw data to BigQuery.",
    )


@asset(group_name="ingestion", config_schema=INGESTION_CONFIG_SCHEMA)
def raw_transactions_bigquery(
    context,
    raw_transactions_dataframe: pd.DataFrame,
) -> MaterializeResult:
    cfg = _resolve_config(context.op_config)
    passed, metadata = _validate_schema(raw_transactions_dataframe, cfg)
    if not passed:
        raise Failure("Input schema validation failed before BigQuery load.", metadata=metadata)

    table_id = f"{cfg['gcp_project_id']}.{cfg['bq_dataset']}.{cfg['bq_table']}"
    client = bigquery.Client(project=cfg["gcp_project_id"])

    job_config = bigquery.LoadJobConfig(
        write_disposition=cfg["write_disposition"],
        autodetect=True,
    )

    load_job = client.load_table_from_dataframe(
        raw_transactions_dataframe,
        table_id,
        job_config=job_config,
    )
    load_job.result()

    table = client.get_table(table_id)
    context.log.info("Loaded %s rows into %s.", table.num_rows, table_id)

    return MaterializeResult(
        metadata={
            "table_id": table.full_table_id,
            "rows_in_table": table.num_rows,
            "columns_in_table": len(table.schema),
            "write_disposition": cfg["write_disposition"],
        }
    )
