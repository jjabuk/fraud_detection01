resource "google_service_account" "mlops" {
  account_id   = var.service_account_id
  display_name = "Fraud MLOps Service Account"
  description  = "Workload identity for fraud data pipelines and model serving"
}

locals {
  iam_profiles = {
    dev = {
      project_roles = [
        "roles/bigquery.jobUser",
        "roles/run.invoker",
      ]
      dataset_roles = {
        raw             = "roles/bigquery.dataEditor"
        features        = "roles/bigquery.dataEditor"
        prediction_logs = "roles/bigquery.dataEditor"
      }
    }
    prod = {
      project_roles = [
        "roles/bigquery.jobUser",
        "roles/run.invoker",
      ]
      dataset_roles = {
        raw             = "roles/bigquery.dataViewer"
        features        = "roles/bigquery.dataEditor"
        prediction_logs = "roles/bigquery.dataEditor"
      }
    }
  }

  selected_iam_profile = local.iam_profiles[var.environment]

  dataset_ids = {
    raw             = google_bigquery_dataset.raw.dataset_id
    features        = google_bigquery_dataset.features.dataset_id
    prediction_logs = google_bigquery_dataset.prediction_logs.dataset_id
  }
}

resource "google_project_iam_member" "mlops_project_roles" {
  for_each = toset(local.selected_iam_profile.project_roles)

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.mlops.email}"
}

resource "google_bigquery_dataset_iam_member" "mlops_dataset_roles" {
  for_each = local.selected_iam_profile.dataset_roles

  dataset_id = local.dataset_ids[each.key]
  role       = each.value
  member     = "serviceAccount:${google_service_account.mlops.email}"
}

output "mlops_service_account_email" {
  description = "Email address of the workload service account"
  value       = google_service_account.mlops.email
}

output "iam_environment" {
  description = "IAM profile currently selected by var.environment"
  value       = var.environment
}
