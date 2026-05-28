#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TERRAFORM_DIR="$ROOT_DIR/terraform"
ENV_FILE="$ROOT_DIR/.env"
ENV_EXAMPLE_FILE="$ROOT_DIR/.env.example"
TFVARS_FILE="$TERRAFORM_DIR/terraform.tfvars"

cd "$ROOT_DIR"

upsert_env_var() {
  local key="$1"
  local value="$2"
  local tmp_file

  tmp_file="$(mktemp)"

  awk -v key="$key" -v value="$value" '
    BEGIN { written = 0 }
    $0 ~ "^" key "=" {
      print key "=" value
      written = 1
      next
    }
    { print }
    END {
      if (written == 0) {
        print key "=" value
      }
    }
  ' "$ENV_FILE" > "$tmp_file"

  mv "$tmp_file" "$ENV_FILE"
}

normalize_env_file() {
  local tmp_file

  tmp_file="$(mktemp)"
  tr -d '\r' < "$ENV_FILE" > "$tmp_file"
  mv "$tmp_file" "$ENV_FILE"
}

load_env() {
  normalize_env_file

  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
}

random_suffix() {
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -hex 4
  else
    printf "%s%04d" "$(date +%s)" "$((RANDOM % 10000))"
  fi
}

bucket_prefix_from_project() {
  local project_id="$1"

  printf "%s" "$project_id" \
    | tr "[:upper:]_" "[:lower:]-" \
    | tr -cd "a-z0-9-" \
    | sed "s/^-*//; s/-*$//" \
    | cut -c1-30
}

is_missing_project() {
  local value="${1:-}"

  case "$value" in
    "" | "your-gcp-project-id" | "CHANGE_ME" | "REPLACE_ME")
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

is_valid_project_id() {
  local value="$1"

  [[ "$value" =~ ^[a-z][a-z0-9-]{4,28}[a-z0-9]$ ]]
}

strip_surrounding_quotes() {
  local value="$1"

  value="${value%\"}"
  value="${value#\"}"
  value="${value%\'}"
  value="${value#\'}"

  printf "%s" "$value"
}

expand_user_path() {
  local path="$1"

  case "$path" in
    "~")
      printf "%s" "$HOME"
      ;;
    "~/"*)
      printf "%s/%s" "$HOME" "${path#~/}"
      ;;
    *)
      printf "%s" "$path"
      ;;
  esac
}

prompt_for_google_cloud_project() {
  local project_id

  load_env

  if ! is_missing_project "${GOOGLE_CLOUD_PROJECT:-}"; then
    return
  fi

  if [ ! -t 0 ]; then
    cat <<EOF
GOOGLE_CLOUD_PROJECT is missing in .env.

Set it to your Google Cloud project ID and rerun this script:

  GOOGLE_CLOUD_PROJECT=<your-project-id>
EOF
    exit 1
  fi

  echo
  echo "GOOGLE_CLOUD_PROJECT is not set yet."
  echo "Enter your Google Cloud project ID, for example: my-gcp-project-123"
  echo

  while true; do
    read -r -p "Google Cloud project ID: " project_id
    project_id="${project_id//[[:space:]]/}"
    project_id="$(strip_surrounding_quotes "$project_id")"

    if is_missing_project "$project_id"; then
      echo "Please enter your own Google Cloud project ID."
      continue
    fi

    if ! is_valid_project_id "$project_id"; then
      echo "Project IDs must be 6-30 chars: lowercase letters, digits, hyphens; start with a letter and end with a letter or digit."
      continue
    fi

    upsert_env_var "GOOGLE_CLOUD_PROJECT" "$project_id"
    load_env
    echo "Saved GOOGLE_CLOUD_PROJECT=$GOOGLE_CLOUD_PROJECT to .env"
    break
  done
}

prompt_for_google_application_credentials() {
  local credentials_path

  load_env

  if [ -n "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]; then
    return
  fi

  if [ ! -t 0 ]; then
    cat <<EOF
GOOGLE_APPLICATION_CREDENTIALS is missing in .env.

Set it to the path of your Google Cloud service account JSON file and rerun this script:

  GOOGLE_APPLICATION_CREDENTIALS=./keys/<service-account-file>.json
EOF
    exit 1
  fi

  echo
  echo "GOOGLE_APPLICATION_CREDENTIALS is not set yet."
  echo "Enter the path to your Google Cloud service account JSON file."
  echo "Example: ./keys/my-service-account.json"
  echo

  while true; do
    read -r -p "Google Cloud credentials JSON path: " credentials_path
    credentials_path="${credentials_path//$'\r'/}"
    credentials_path="$(strip_surrounding_quotes "$credentials_path")"
    credentials_path="$(expand_user_path "$credentials_path")"

    if [ -z "$credentials_path" ]; then
      echo "Please enter a path to a service account JSON file."
      continue
    fi

    if [ ! -f "$credentials_path" ]; then
      echo "Credentials file not found: $credentials_path"
      continue
    fi

    upsert_env_var "GOOGLE_APPLICATION_CREDENTIALS" "$credentials_path"
    load_env
    echo "Saved GOOGLE_APPLICATION_CREDENTIALS=$GOOGLE_APPLICATION_CREDENTIALS to .env"
    break
  done
}

needs_generated_bucket() {
  local value="${1:-}"

  case "$value" in
    "" | "gdelt-pipeline" | "gdelt_pipeline_489312_datalake" | "your-globally-unique-gcs-bucket-name" | "CHANGE_ME"* | "REPLACE_ME"*)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

needs_generated_dataset() {
  local value="${1:-}"

  case "$value" in
    "" | "gdelt_pipeline_dataset" | "gdelt_pipeline_dataset_489312" | "your_bigquery_dataset_name" | "CHANGE_ME"* | "REPLACE_ME"*)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

ensure_cloud_resource_names() {
  load_env

  local suffix
  local project_prefix

  suffix="$(random_suffix)"

  if is_missing_project "${GOOGLE_CLOUD_PROJECT:-}"; then
    project_prefix="gdelt"
  else
    project_prefix="$(bucket_prefix_from_project "$GOOGLE_CLOUD_PROJECT")"
    if [ -z "$project_prefix" ]; then
      project_prefix="gdelt"
    fi
  fi

  if needs_generated_bucket "${OBJECT_STORAGE_BUCKET:-}"; then
    upsert_env_var "OBJECT_STORAGE_BUCKET" "${project_prefix}-gdelt-datalake-${suffix}"
  fi

  if needs_generated_dataset "${BIGQUERY_DATASET:-}"; then
    upsert_env_var "BIGQUERY_DATASET" "gdelt_pipeline_${suffix}"
  fi

  load_env
}

write_tfvars() {
  cat > "$TFVARS_FILE" <<EOF
project           = "$GOOGLE_CLOUD_PROJECT"
region            = "eu"
location          = "EU"
gcs_bucket_name   = "$OBJECT_STORAGE_BUCKET"
bq_dataset_name   = "$BIGQUERY_DATASET"
gcs_storage_class = "STANDARD"
EOF
}

if [ ! -f "$ENV_FILE" ]; then
  cp "$ENV_EXAMPLE_FILE" "$ENV_FILE"
  echo "Created .env from .env.example"
fi

prompt_for_google_cloud_project
prompt_for_google_application_credentials
ensure_cloud_resource_names

write_tfvars
echo "Wrote terraform/terraform.tfvars from .env"
echo "Using Google Cloud project: $GOOGLE_CLOUD_PROJECT"
echo "Using GCS bucket: $OBJECT_STORAGE_BUCKET"
echo "Using BigQuery dataset: $BIGQUERY_DATASET"

cd "$TERRAFORM_DIR"

terraform init
terraform fmt
terraform validate
terraform apply -var-file="terraform.tfvars"
