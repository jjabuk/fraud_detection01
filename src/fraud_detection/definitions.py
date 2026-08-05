from dagster import Definitions
from fraud_detection.assets.ingestion import (
    raw_transactions_bigquery,
    raw_transactions_dataframe,
    raw_transactions_schema_check,
)

defs = Definitions(
    assets=[raw_transactions_dataframe, raw_transactions_bigquery],
    asset_checks=[raw_transactions_schema_check],
)
