from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from google.cloud import bigquery, storage

from dagster import Failure, MaterializeResult, asset
from fraud_detection.resources import (
    BQ_SCHEMA_PATH,
    BigQueryResource,
    RawCsvSourceResource,
)

REQUIRED_COLUMNS = ["TransactionID", "TransactionDT", "TransactionAmt"]
FRAUD_LABEL_COLUMN = "isFraud"
VALIDATION_CHUNK_SIZE = 100_000

RAW_TABLE = "ieee_train_transaction_raw"


def _open_source(uri: str, project: str):
    """Returns a context-manager-compatible, file-like object for `uri`.

    Deliberately uses google-cloud-storage's Blob.open() for gs:// URIs
    instead of handing the URI string straight to pandas, which would
    require pulling in the fsspec/gcsfs ecosystem just for this one call
    site. Works uniformly with `with _open_source(uri) as f: ...` for
    both local paths and gs:// URIs.

    `project` is passed explicitly to storage.Client() because it cannot
    reliably be inferred from an impersonated-service-account ADC (see
    resources.DEFAULT_GCP_PROJECT_ID) -- confirmed the hard way against a
    real run, not a defensive guess.
    """
    if uri.startswith("gs://"):
        bucket_name, _, blob_path = uri.removeprefix("gs://").partition("/")
        return storage.Client(project=project).bucket(bucket_name).blob(blob_path).open("rb")
    return Path(uri).open("rb")


@asset(group_name="ingestion")
def raw_transactions_validation(
    context,
    raw_csv_source: RawCsvSourceResource,
) -> MaterializeResult:
    """Validates the raw CSV before anything gets loaded to BigQuery.

    Streams only REQUIRED_COLUMNS + the fraud label across the FULL file
    (all rows, ~5 columns) rather than reading every one of the source
    file's ~394 columns -- bounded to tens of MB regardless of file size,
    and a real full-file guarantee rather than a sampled one before the
    downstream WRITE_TRUNCATE load fires.
    """
    cols = [*REQUIRED_COLUMNS, FRAUD_LABEL_COLUMN]
    total_rows = 0
    missing: set[str] = set(cols)
    bad_label_values: set[Any] = set()

    with _open_source(raw_csv_source.uri, raw_csv_source.project) as source_file:
        reader = pd.read_csv(
            source_file,
            usecols=lambda c: c in cols,
            chunksize=VALIDATION_CHUNK_SIZE,
        )
        for chunk in reader:
            missing -= set(chunk.columns)
            total_rows += len(chunk)
            if FRAUD_LABEL_COLUMN in chunk.columns:
                bad_label_values |= set(chunk[FRAUD_LABEL_COLUMN].dropna().unique()) - {0, 1}

    if missing or bad_label_values:
        raise Failure(
            f"Schema validation failed for {raw_csv_source.uri}: "
            f"missing_columns={sorted(missing)}, "
            f"fraud_label_values_outside_0_1={sorted(map(str, bad_label_values))}",
            metadata={
                "rows_scanned": total_rows,
                "missing_columns": sorted(missing),
                "bad_label_values": sorted(map(str, bad_label_values)),
            },
        )

    context.log.info(
        "Validated %s rows from '%s': all required columns present, label domain OK.",
        total_rows,
        raw_csv_source.uri,
    )
    return MaterializeResult(
        metadata={"rows_validated": total_rows, "source_uri": raw_csv_source.uri}
    )


@asset(group_name="ingestion", deps=[raw_transactions_validation])
def raw_transactions_bigquery(
    context,
    raw_csv_source: RawCsvSourceResource,
    bigquery_resource: BigQueryResource,
) -> MaterializeResult:
    """Loads the validated raw CSV into BigQuery.

    Production (gs:// source) uses a server-side load_table_from_uri
    against a pinned schema, so this process never holds the file's bytes
    -- footprint stays flat regardless of file size. Local/dev (the
    committed sample) loads directly with autodetect, since the sample's
    5-column shape is a deliberate illustrative subset, not the real
    394-column schema, so pinning wouldn't make sense there.

    Depends on raw_transactions_validation by ordering only (deps=[...]),
    not by receiving its output as an argument -- no multi-GB DataFrame
    round-trips through Dagster's IO manager.
    """
    client = bigquery_resource.get_client()
    table_id = f"{bigquery_resource.project}.raw.{RAW_TABLE}"

    if raw_csv_source.is_gcs:
        if not BQ_SCHEMA_PATH.exists():
            raise Failure(
                f"Pinned BigQuery schema not found at {BQ_SCHEMA_PATH}. "
                "Materialize raw_transaction_bq_schema once before loading "
                "from a gs:// source."
            )
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            schema=client.schema_from_json(str(BQ_SCHEMA_PATH)),
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        load_job = client.load_table_from_uri(raw_csv_source.uri, table_id, job_config=job_config)
    else:
        source_path = Path(raw_csv_source.uri)
        if not source_path.exists():
            raise Failure(
                f"Local CSV not found at {source_path}. "
                "Download data from Kaggle first or change raw_csv_source.uri."
            )
        job_config = bigquery.LoadJobConfig(
            skip_leading_rows=1,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        with source_path.open("rb") as source_file:
            load_job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    load_job.result(timeout=1800)
    table = client.get_table(table_id)

    context.log.info("Loaded %s rows into %s.", table.num_rows, table_id)
    return MaterializeResult(
        metadata={
            "table_id": table.full_table_id,
            "rows_in_table": table.num_rows,
            "write_disposition": str(job_config.write_disposition),
        }
    )
