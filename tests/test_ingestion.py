from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest
from google.cloud import bigquery

import fraud_detection.resources as resources_module
from dagster import Failure, build_asset_context
from fraud_detection.assets import ingestion as ingestion_module
from fraud_detection.assets.ingestion import (
    raw_transactions_bigquery,
    raw_transactions_validation,
)
from fraud_detection.resources import BigQueryResource, RawCsvSourceResource

# ---------------------------------------------------------------------------
# raw_transactions_validation -- exercises the real chunked-read logic
# against real (tiny) CSV files. No BigQuery involved.
# ---------------------------------------------------------------------------


def test_validation_passes_on_committed_sample():
    context = build_asset_context()
    result = raw_transactions_validation(context, RawCsvSourceResource())

    assert result.metadata["rows_validated"] == 5
    assert result.metadata["source_uri"] == "data/raw/train_transaction_sample.csv"


def test_validation_fails_on_missing_required_column(tmp_path):
    bad_csv = tmp_path / "missing_column.csv"
    bad_csv.write_text("TransactionID,TransactionAmt,isFraud\n1,10.0,0\n")

    context = build_asset_context()
    with pytest.raises(Failure, match="missing_columns"):
        raw_transactions_validation(context, RawCsvSourceResource(uri=str(bad_csv)))


def test_validation_fails_on_label_outside_0_1(tmp_path):
    bad_csv = tmp_path / "bad_label.csv"
    bad_csv.write_text(
        "TransactionID,TransactionDT,TransactionAmt,isFraud\n1,100,10.0,2\n"
    )

    context = build_asset_context()
    with pytest.raises(Failure, match="fraud_label_values_outside_0_1"):
        raw_transactions_validation(context, RawCsvSourceResource(uri=str(bad_csv)))


# ---------------------------------------------------------------------------
# raw_transactions_bigquery -- BigQuery client is always mocked. Real
# BigQuery calls are a manual/integration check (see README), not part of
# this suite.
# ---------------------------------------------------------------------------


def _mock_bigquery_client(monkeypatch: pytest.MonkeyPatch, *, num_rows: int = 5) -> MagicMock:
    mock_client = MagicMock()
    mock_client.get_table.return_value = MagicMock(
        num_rows=num_rows, full_table_id="test-project:raw.ieee_train_transaction_raw"
    )
    monkeypatch.setattr(resources_module.bigquery, "Client", lambda *a, **k: mock_client)
    return mock_client


def test_bigquery_asset_local_source_uses_load_table_from_file(monkeypatch):
    mock_client = _mock_bigquery_client(monkeypatch)
    context = build_asset_context()

    result = raw_transactions_bigquery(
        context,
        raw_csv_source=RawCsvSourceResource(),  # default: committed sample
        bigquery_resource=BigQueryResource(project="test-project"),
    )

    mock_client.load_table_from_uri.assert_not_called()
    mock_client.load_table_from_file.assert_called_once()
    args, kwargs = mock_client.load_table_from_file.call_args
    assert args[1] == "test-project.raw.ieee_train_transaction_raw"
    job_config = kwargs["job_config"]
    assert job_config.autodetect is True
    assert job_config.write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE

    assert result.metadata["rows_in_table"] == 5


def test_bigquery_asset_gcs_source_without_pinned_schema_raises(monkeypatch, tmp_path):
    monkeypatch.setattr(ingestion_module, "BQ_SCHEMA_PATH", tmp_path / "does_not_exist.json")
    mock_client = _mock_bigquery_client(monkeypatch)
    context = build_asset_context()

    with pytest.raises(Failure, match="Pinned BigQuery schema not found"):
        raw_transactions_bigquery(
            context,
            raw_csv_source=RawCsvSourceResource(uri="gs://fraud-bucket/train_transaction.csv"),
            bigquery_resource=BigQueryResource(project="test-project"),
        )

    mock_client.load_table_from_uri.assert_not_called()
    mock_client.load_table_from_file.assert_not_called()


def test_bigquery_asset_gcs_source_with_pinned_schema_uses_load_table_from_uri(
    monkeypatch, tmp_path
):
    schema_path = tmp_path / "schema.json"
    schema_path.write_text(json.dumps([{"name": "TransactionID", "type": "INTEGER", "mode": "REQUIRED"}]))
    monkeypatch.setattr(ingestion_module, "BQ_SCHEMA_PATH", schema_path)

    mock_client = _mock_bigquery_client(monkeypatch, num_rows=590_540)
    mock_client.schema_from_json.return_value = [bigquery.SchemaField("TransactionID", "INTEGER")]
    context = build_asset_context()

    result = raw_transactions_bigquery(
        context,
        raw_csv_source=RawCsvSourceResource(uri="gs://fraud-bucket/train_transaction.csv"),
        bigquery_resource=BigQueryResource(project="test-project"),
    )

    mock_client.load_table_from_file.assert_not_called()
    mock_client.load_table_from_uri.assert_called_once()
    args, kwargs = mock_client.load_table_from_uri.call_args
    assert args[0] == "gs://fraud-bucket/train_transaction.csv"
    assert args[1] == "test-project.raw.ieee_train_transaction_raw"
    job_config = kwargs["job_config"]
    assert job_config.write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE
    assert job_config.autodetect is not True  # pinned schema, not autodetected
    mock_client.schema_from_json.assert_called_once_with(str(schema_path))

    assert result.metadata["rows_in_table"] == 590_540
