from __future__ import annotations

import zipfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests

from dagster import Failure, build_asset_context
from fraud_detection.assets import kaggle_source as kaggle_source_module
from fraud_detection.assets.kaggle_source import raw_transaction_kaggle_to_gcs
from fraud_detection.resources import KaggleRawDumpResource

# Kaggle's own network calls and GCS uploads are always mocked here. A real
# end-to-end run is a rare, manual/by-hand check (see README), not part of
# this suite.


def _patch_kaggle_auth(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(kaggle_source_module.KaggleApi, "authenticate", lambda self: None)


def _patch_gcs_client(monkeypatch: pytest.MonkeyPatch) -> tuple[MagicMock, MagicMock]:
    mock_client = MagicMock()
    mock_blob = MagicMock()
    mock_client.bucket.return_value.blob.return_value = mock_blob
    monkeypatch.setattr(kaggle_source_module.storage, "Client", lambda *a, **k: mock_client)
    return mock_client, mock_blob


def test_success_streams_downloaded_file_to_gcs(monkeypatch):
    _patch_kaggle_auth(monkeypatch)
    mock_client, mock_blob = _patch_gcs_client(monkeypatch)

    def fake_download(self, competition, file_name, path=None, force=False, quiet=False):
        Path(path, file_name).write_bytes(b"fake-csv-bytes")

    monkeypatch.setattr(kaggle_source_module.KaggleApi, "competition_download_file", fake_download)

    context = build_asset_context()
    resource = KaggleRawDumpResource(gcs_uri="gs://test-bucket/ieee-cis/train_transaction.csv")

    result = raw_transaction_kaggle_to_gcs(context, resource)

    mock_client.bucket.assert_called_once_with("test-bucket")
    mock_client.bucket.return_value.blob.assert_called_once_with("ieee-cis/train_transaction.csv")
    mock_blob.upload_from_filename.assert_called_once()
    assert result.metadata["bytes_uploaded"] == len(b"fake-csv-bytes")
    assert result.metadata["gcs_uri"] == "gs://test-bucket/ieee-cis/train_transaction.csv"


def test_zip_fallback_extraction(monkeypatch):
    _patch_kaggle_auth(monkeypatch)
    _mock_client, mock_blob = _patch_gcs_client(monkeypatch)

    def fake_download(self, competition, file_name, path=None, force=False, quiet=False):
        zip_path = Path(path) / f"{Path(file_name).stem}.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr(file_name, "a,b\n1,2\n")

    monkeypatch.setattr(kaggle_source_module.KaggleApi, "competition_download_file", fake_download)

    context = build_asset_context()
    resource = KaggleRawDumpResource(gcs_uri="gs://test-bucket/train_transaction.csv")

    result = raw_transaction_kaggle_to_gcs(context, resource)

    mock_blob.upload_from_filename.assert_called_once()
    assert result.metadata["bytes_uploaded"] == len("a,b\n1,2\n")


def test_kaggle_http_error_becomes_failure_with_rules_hint(monkeypatch):
    _patch_kaggle_auth(monkeypatch)
    _patch_gcs_client(monkeypatch)

    def fake_download(self, competition, file_name, path=None, force=False, quiet=False):
        raise requests.exceptions.HTTPError("403 Forbidden")

    monkeypatch.setattr(kaggle_source_module.KaggleApi, "competition_download_file", fake_download)

    context = build_asset_context()
    with pytest.raises(Failure, match="rules"):
        raw_transaction_kaggle_to_gcs(context, KaggleRawDumpResource())


def test_missing_downloaded_file_raises_failure(monkeypatch):
    _patch_kaggle_auth(monkeypatch)
    _patch_gcs_client(monkeypatch)

    def fake_download(self, competition, file_name, path=None, force=False, quiet=False):
        pass  # writes nothing -- simulates an unexpected empty response

    monkeypatch.setattr(kaggle_source_module.KaggleApi, "competition_download_file", fake_download)

    context = build_asset_context()
    with pytest.raises(Failure, match="did not produce"):
        raw_transaction_kaggle_to_gcs(context, KaggleRawDumpResource())


def test_rejects_non_gcs_destination(monkeypatch):
    _patch_kaggle_auth(monkeypatch)
    _patch_gcs_client(monkeypatch)

    def fake_download(self, competition, file_name, path=None, force=False, quiet=False):
        Path(path, file_name).write_bytes(b"data")

    monkeypatch.setattr(kaggle_source_module.KaggleApi, "competition_download_file", fake_download)

    context = build_asset_context()
    with pytest.raises(Failure, match="gs://"):
        raw_transaction_kaggle_to_gcs(context, KaggleRawDumpResource(gcs_uri="not-a-gcs-uri"))
