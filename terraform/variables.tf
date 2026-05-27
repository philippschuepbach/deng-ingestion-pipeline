variable "project" {
  description = "Google Cloud project ID"
  type        = string
}

variable "region" {
  description = "Google Cloud provider region"
  type        = string
  default     = "eu"
}

variable "location" {
  description = "Google Cloud resource location"
  type        = string
  default     = "EU"
}

variable "bq_dataset_name" {
  description = "BigQuery dataset name"
  type        = string
}

variable "gcs_bucket_name" {
  description = "Globally unique GCS bucket name"
  type        = string
}

variable "gcs_storage_class" {
  description = "Bucket storage class"
  type        = string
  default     = "STANDARD"
}
