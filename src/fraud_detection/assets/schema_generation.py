from __future__ import annotations

from google.cloud import bigquery

from dagster import Failure, MaterializeResult, asset
from fraud_detection.resources import BQ_SCHEMA_PATH, RAW_DUMP_GCS_URI, BigQueryResource

SANDBOX_TABLE = "schema_sandbox_train_transaction"


@asset(group_name="ingestion")
def raw_transaction_bq_schema(
    context,
    bigquery_resource: BigQueryResource,
) -> MaterializeResult:
    """Generates the pinned BigQuery schema for the full train_transaction.csv
    and commits it to BQ_SCHEMA_PATH.

    Loads the full file (autodetect) from RAW_DUMP_GCS_URI into a
    throwaway sandbox table, then captures the resulting schema. If the
    load job succeeds, the schema is proven correct against every row in
    the file, not just whatever sample BigQuery's autodetect scans to
    infer types -- empirical, not a guess -- then the sandbox table is
    dropped.

    Rare, by-hand, like raw_transaction_kaggle_to_gcs: run this once (or
    whenever the source file's structure changes), not on every ingestion
    pass. Requires raw_transaction_kaggle_to_gcs to have staged the file
    in GCS first.

    NOTE: writes into the repo's source tree (BQ_SCHEMA_PATH). Commit the
    result to git afterward -- this is intentional one-off codegen, not a
    normal data asset.
    """
    client = bigquery_resource.get_client()
    table_id = f"{bigquery_resource.project}.raw.{SANDBOX_TABLE}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    load_job = client.load_table_from_uri(RAW_DUMP_GCS_URI, table_id, job_config=job_config)

    try:
        load_job.result(timeout=1800)
    except Exception as exc:
        raise Failure(
            f"Autodetect load of {RAW_DUMP_GCS_URI} into sandbox table "
            f"{table_id} failed: {exc}"
        ) from exc

    table = client.get_table(table_id)

    BQ_SCHEMA_PATH.parent.mkdir(parents=True, exist_ok=True)
    client.schema_to_json(table.schema, str(BQ_SCHEMA_PATH))

    rows_scanned = table.num_rows
    field_count = len(table.schema)
    client.delete_table(table_id, not_found_ok=True)

    context.log.info(
        "Captured %s-field schema from %s rows of %s, wrote to %s, dropped sandbox table %s.",
        field_count,
        rows_scanned,
        RAW_DUMP_GCS_URI,
        BQ_SCHEMA_PATH,
        table_id,
    )
    return MaterializeResult(
        metadata={
            "schema_path": str(BQ_SCHEMA_PATH),
            "field_count": field_count,
            "rows_scanned": rows_scanned,
        }
    )
