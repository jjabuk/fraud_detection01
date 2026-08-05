terraform {
  required_version = ">= 1.6"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.42"
    }
  }

  # The bucket is NOT managed by this configuration and must not be: it
  # holds this configuration's own state, so tofu cannot create the thing
  # it needs in order to record that it created it. Bootstrapped once by
  # hand:
  #
  #   gcloud storage buckets create gs://fraud-detection-504617-tfstate \
  #     --project=fraud-detection-504617 --location=europe-central2 \
  #     --uniform-bucket-level-access --public-access-prevention
  #   gcloud storage buckets update gs://fraud-detection-504617-tfstate --versioning
  #
  # Object versioning is the point, not a nicety: a corrupted or
  # truncated state write is recoverable from the previous generation,
  # and state is the one file whose loss turns managed infrastructure
  # back into unmanaged infrastructure.
  backend "gcs" {
    bucket = "fraud-detection-504617-tfstate"
    prefix = "iaac"
  }
}
