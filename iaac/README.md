# IaC Backend Setup

This document explains how to configure remote OpenTofu/Terraform state in Google Cloud Storage and wire it into this repository's IaC configuration.

Current state bucket:

- `gs://fraud-detection-504617-tfstate`

## Why the state bucket is created manually

The backend bucket cannot be managed by the same configuration whose state it stores. OpenTofu needs the backend before it can create any resources, so this bucket is bootstrapped manually with `gcloud`.

## Requirements

- Installed `gcloud`
- Installed `tofu` (OpenTofu)
- Permissions to create buckets and update settings in the target GCP project
- Authenticated account (`gcloud auth login`)

## Step-by-step: state backend setup

1. Set the active GCP project:

```bash
gcloud config set project fraud-detection-504617
```

2. Create the state bucket in `europe-central2`:

```bash
gcloud storage buckets create gs://fraud-detection-504617-tfstate \
	--project=fraud-detection-504617 \
	--location=europe-central2 \
	--uniform-bucket-level-access \
	--public-access-prevention
```

3. Enable object versioning (critical for state recovery):

```bash
gcloud storage buckets update gs://fraud-detection-504617-tfstate --versioning
```

4. Verify bucket configuration:

```bash
gcloud storage buckets describe gs://fraud-detection-504617-tfstate \
	--project=fraud-detection-504617 | rg -i 'versioning|name:|location:'
```

Expected minimum output:

- `name: fraud-detection-504617-tfstate`
- `location: EUROPE-CENTRAL2`
- `versioning_enabled: true`

## What is already configured in this repo

1. GCS backend is configured in [versions.tf](versions.tf):

- bucket: `fraud-detection-504617-tfstate`
- prefix: `iaac`

2. Default `project_id` is configured in [variables.tf](variables.tf):

- `fraud-detection-504617`

## Reinitialize backend after changes

After any backend change, run in this folder:

```bash
tofu init -reconfigure
```

Optional plan check:

```bash
tofu plan
```

## Note about ADC quota project

If `gcloud` shows a warning about mismatched quota project for Application Default Credentials, run:

```bash
gcloud auth application-default set-quota-project fraud-detection-504617
```

This removes the warning and helps avoid unexpected API quota issues.

