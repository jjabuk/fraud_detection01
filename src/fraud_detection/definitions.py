from dagster import Definitions
from fraud_detection.assets.ingestion import (
    raw_transactions_bigquery,
    raw_transactions_validation,
)
from fraud_detection.assets.kaggle_source import raw_transaction_kaggle_to_gcs
from fraud_detection.resources import (
    BigQueryResource,
    KaggleRawDumpResource,
    RawCsvSourceResource,
)

defs = Definitions(
    assets=[
        raw_transaction_kaggle_to_gcs,
        raw_transactions_validation,
        raw_transactions_bigquery,
    ],
    resources={
        # Local/dev default lives on RawCsvSourceResource itself (the
        # committed sample). Point at RAW_DUMP_GCS_URI (resources.py) here
        # -- or via run config -- once raw_transaction_kaggle_to_gcs has
        # staged the full file. See README.
        "raw_csv_source": RawCsvSourceResource(),
        "bigquery_resource": BigQueryResource(),
        "kaggle_raw_dump": KaggleRawDumpResource(),
    },
)
