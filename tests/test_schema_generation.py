from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import fraud_detection.resources as resources_module
from dagster import Failure, build_asset_context
from fraud_detection.assets.schema_generation import (
    SANDBOX_TABLE,
    raw_transaction_bq_schema,
)
from fraud_detection.resources import BigQueryResource

# BigQuery is always mocked here. A real run (against the real GCS-staged
# file) is a rare, by-hand check -- see README -- not part of this suite.


def _mock_bigquery_client(monkeypatch: pytest.MonkeyPatch, *, num_rows: int = 590_540) -> MagicMock:
    mock_client = MagicMock()
    mock_client.get_table.return_value = MagicMock(
        num_rows=num_rows, schema=["fake-field-1", "fake-field-2"]
    )
    monkeypatch.setattr(resources_module.bigquery, "Client", lambda *a, **k: mock_client)
    return mock_client


def test_generates_schema_and_drops_sandbox_table(monkeypatch, tmp_path):
    schema_path = tmp_path / "schema.json"
    monkeypatch.setattr(
        "fraud_detection.assets.schema_generation.BQ_SCHEMA_PATH", schema_path
    )
    mock_client = _mock_bigquery_client(monkeypatch)
    context = build_asset_context()

    result = raw_transaction_bq_schema(context, BigQueryResource(project="test-project"))

    mock_client.load_table_from_uri.assert_called_once()
    args, kwargs = mock_client.load_table_from_uri.call_args
    assert args[1] == f"test-project.raw.{SANDBOX_TABLE}"
    assert kwargs["job_config"].autodetect is True

    mock_client.schema_to_json.assert_called_once_with(
        ["fake-field-1", "fake-field-2"], str(schema_path)
    )
    mock_client.delete_table.assert_called_once_with(
        f"test-project.raw.{SANDBOX_TABLE}", not_found_ok=True
    )

    assert result.metadata["field_count"] == 2
    assert result.metadata["rows_scanned"] == 590_540
    assert result.metadata["schema_path"] == str(schema_path)


def test_load_failure_raises_failure_and_leaves_no_schema_file(monkeypatch, tmp_path):
    schema_path = tmp_path / "schema.json"
    monkeypatch.setattr(
        "fraud_detection.assets.schema_generation.BQ_SCHEMA_PATH", schema_path
    )
    mock_client = _mock_bigquery_client(monkeypatch)
    mock_client.load_table_from_uri.return_value.result.side_effect = RuntimeError("boom")
    context = build_asset_context()

    with pytest.raises(Failure, match="Autodetect load"):
        raw_transaction_bq_schema(context, BigQueryResource(project="test-project"))

    mock_client.schema_to_json.assert_not_called()
    mock_client.delete_table.assert_not_called()
    assert not schema_path.exists()
