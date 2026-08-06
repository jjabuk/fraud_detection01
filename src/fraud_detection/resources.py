from __future__ import annotations

import os

from dagster import ConfigurableResource
from google.cloud import bigquery


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


class BigQueryResource(ConfigurableResource):
    project: str = os.getenv("GCP_PROJECT_ID", "fraud-detection-504617")
    location: str = "europe-central2"

    def get_client(self) -> bigquery.Client:
        return bigquery.Client(project=self.project, location=self.location)
