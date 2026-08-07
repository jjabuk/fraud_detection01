from __future__ import annotations

import os

from google.cloud import bigquery

from dagster import ConfigurableResource

# Single source of truth for where the full Kaggle dump lives once staged.
# raw_transaction_kaggle_to_gcs (assets/kaggle_source.py) writes here;
# RawCsvSourceResource.uri gets pointed at this same value once you're
# ready to switch validation/load off the committed sample.
RAW_DUMP_GCS_URI = "gs://fraud-detection-504617-raw-data/ieee-cis/train_transaction.csv"


class RawCsvSourceResource(ConfigurableResource):
    """The one true location of the static Kaggle train_transaction.csv dump.

    `uri` is either a gs:// URI (production: enables load_table_from_uri in
    raw_transactions_bigquery, so the file's bytes never pass through this
    process) or a local filesystem path (dev/test: the committed sample).
    """

    uri: str = "data/raw/train_transaction_sample.csv"

    @property
    def is_gcs(self) -> bool:
        return self.uri.startswith("gs://")


class KaggleRawDumpResource(ConfigurableResource):
    """Where to fetch the full raw dump from Kaggle, and where it lands in
    GCS. Consumed only by raw_transaction_kaggle_to_gcs -- a rare, by-hand
    materialization, not part of the recurring validate/load chain.

    Credentials: kaggle's own KaggleApi.authenticate() resolves them itself
    (checks ~/.kaggle/access_token, then legacy KAGGLE_USERNAME/KAGGLE_KEY,
    then OAuth) -- nothing secret is modeled as resource config here.
    """

    competition: str = "ieee-fraud-detection"
    file_name: str = "train_transaction.csv"
    gcs_uri: str = RAW_DUMP_GCS_URI


class BigQueryResource(ConfigurableResource):
    project: str = os.getenv("GCP_PROJECT_ID", "fraud-detection-504617")
    location: str = "europe-central2"

    def get_client(self) -> bigquery.Client:
        return bigquery.Client(project=self.project, location=self.location)
