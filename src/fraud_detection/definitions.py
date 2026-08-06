from dagster import Definitions
from fraud_detection.assets.ingestion import (
    raw_transactions_bigquery,
    raw_transactions_validation,
)
from fraud_detection.resources import BigQueryResource, RawCsvSourceResource

defs = Definitions(
    assets=[raw_transactions_validation, raw_transactions_bigquery],
    resources={
        # Local/dev default lives on RawCsvSourceResource itself (the
        # committed sample). Point at a gs://... URI here (or via run
        # config) once the GCS staging bucket exists -- see README.
        "raw_csv_source": RawCsvSourceResource(),
        "bigquery_resource": BigQueryResource(),
    },
)
