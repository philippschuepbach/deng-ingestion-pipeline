variable "credentials" {
  description = "My Credentials"
  default     = "./keys/my-creds.json"
  #ex: if you have a directory where this file is called keys with your service account json file
  #saved there as my-creds.json you could use default = "./keys/my-creds.json"
}


variable "project" {
  description = "Project"
  default     = "gdelt-live-ingestion-489312"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default     = "eu"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default     = "eu"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default     = "gdelt_pipeline_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  #Update the below to a unique bucket name
  default     = "gdelt-pipeline"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}