// Staging bucket for the static Kaggle raw CSV dump. Ingestion's
// load_table_from_uri path (src/fraud_detection/assets/ingestion.py) reads
// directly from here -- location must match bq_location (both
// europe-central2), since load_table_from_uri rejects cross-region
// bucket/dataset pairs.

resource "google_storage_bucket" "raw_data" {
  name                        = "${var.project_id}-raw-data"
  location                    = var.region
  uniform_bucket_level_access = true
  labels                      = var.labels

  lifecycle {
    prevent_destroy = true
  }
}
