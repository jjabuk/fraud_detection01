variable "project_id" {
  description = "GCP project ID (the globally-unique ID, not the display name)"
  type        = string
  default     = "fraud-detection-504617"
}

variable "region" {
  description = "Default region for compute resources (Cloud Run, later)"
  type        = string
  default     = "europe-central2"
}

# BigQuery dataset location is IMMUTABLE. Changing this variable after
# the datasets hold data means recreating them, which means reloading
# everything. Keep it equal to `region` so queries never pay
# cross-region transfer to reach Cloud Run.
variable "bq_location" {
  description = "BigQuery dataset location; immutable after creation"
  type        = string
  default     = "europe-central2"
}

// Deliberately not "latest": a job whose image tag never changes gives
// you no way to say WHICH build ran a job, and no way to roll one
// back. Push an immutable tag (a git SHA) and bump this.
# NO DEFAULT on purpose. It used to default to "bootstrap", a MUTABLE tag
# — so any `tofu apply` that forgot `-var image_tag=<sha>` silently rolled
# every job back to whatever that tag pointed at, which is how a job ends
# up running two-week-old code while the plan reports no changes worth
# reading. An error saying "no value for variable" is a far better
# outcome than a silent downgrade.
#
#   tofu apply -var image_tag=$(git rev-parse --short HEAD)
# variable "image_tag" {
#   description = "Container image tag for the Cloud Run jobs (git SHA)"
#   type        = string
# }

variable "labels" {
  description = "Labels applied to every resource, for cost attribution"
  type        = map(string)
  default = {
    app        = "fraud_detection01"
    managed_by = "opentofu"
  }
}
