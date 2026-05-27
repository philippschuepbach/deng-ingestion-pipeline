#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TERRAFORM_DIR="$ROOT_DIR/terraform"
ENV_FILE="$ROOT_DIR/.env"
ENV_EXAMPLE_FILE="$ROOT_DIR/.env.example"
TFVARS_FILE="$TERRAFORM_DIR/terraform.tfvars"
TFVARS_EXAMPLE_FILE="$TERRAFORM_DIR/terraform.tfvars.example"

cd "$ROOT_DIR"

if [ ! -f "$ENV_FILE" ]; then
  cp "$ENV_EXAMPLE_FILE" "$ENV_FILE"
  echo "Created .env from .env.example"
  echo "Please review and update .env, then run this script again."
  exit 1
fi

if [ ! -f "$TFVARS_FILE" ]; then
  cp "$TFVARS_EXAMPLE_FILE" "$TFVARS_FILE"
  echo "Created terraform/terraform.tfvars from terraform.tfvars.example"
fi

set -a
source "$ENV_FILE"
set +a

cd "$TERRAFORM_DIR"

terraform init
terraform fmt
terraform validate
terraform apply -var-file="terraform.tfvars"
