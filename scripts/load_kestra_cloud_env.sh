#!/bin/sh
set -eu

if [ -f /workspace/.env ]; then
  set -a
  . /workspace/.env
  set +a
fi

if [ -n "${KESTRA_GOOGLE_CREDENTIALS_JSON_BASE64:-}" ]; then
  credentials_file="$(mktemp /tmp/kestra-gcp-credentials.XXXXXX.json)"

  if ! printf '%s' "$KESTRA_GOOGLE_CREDENTIALS_JSON_BASE64" \
    | base64 -d > "$credentials_file"; then
    echo "Could not decode google_credentials_json_base64 Kestra input." >&2
    echo "Create it with: base64 -w0 keys/<service-account-file>.json" >&2
    exit 1
  fi

  chmod 600 "$credentials_file"
  export GOOGLE_APPLICATION_CREDENTIALS="$credentials_file"
fi

if [ -n "${GOOGLE_APPLICATION_CREDENTIALS:-}" ] \
  && [ ! -f "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
  credential_basename="$(basename "$GOOGLE_APPLICATION_CREDENTIALS")"

  for candidate in \
    "/workspace/$GOOGLE_APPLICATION_CREDENTIALS" \
    "/workspace/keys/$credential_basename" \
    "/workspace/$credential_basename"; do
    if [ -f "$candidate" ]; then
      export GOOGLE_APPLICATION_CREDENTIALS="$candidate"
      break
    fi
  done
fi

if [ -z "${GOOGLE_CLOUD_PROJECT:-}" ]; then
  echo "Missing GOOGLE_CLOUD_PROJECT in /workspace/.env" >&2
  exit 1
fi

if [ -z "${OBJECT_STORAGE_BUCKET:-}" ]; then
  echo "Missing OBJECT_STORAGE_BUCKET in /workspace/.env" >&2
  exit 1
fi

if [ -z "${BIGQUERY_DATASET:-}" ]; then
  echo "Missing BIGQUERY_DATASET in /workspace/.env" >&2
  exit 1
fi

if [ -z "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]; then
  cat >&2 <<'EOF'
Missing GOOGLE_APPLICATION_CREDENTIALS in /workspace/.env.

For Docker/Kestra, put your service account JSON inside the repository's keys/
folder and set:

  GOOGLE_APPLICATION_CREDENTIALS=./keys/<service-account-file>.json

The repository is mounted at /workspace inside the Kestra container.
EOF
  exit 1
fi

if [ ! -f "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
  cat >&2 <<EOF
Google Cloud credentials file was not found inside the Kestra container:

  GOOGLE_APPLICATION_CREDENTIALS=$GOOGLE_APPLICATION_CREDENTIALS

For Docker/Kestra, put your service account JSON inside:

  /workspace/keys/

From the host repository, that is:

  ./keys/

Then set /workspace/.env to one of:

  GOOGLE_APPLICATION_CREDENTIALS=./keys/$(basename "$GOOGLE_APPLICATION_CREDENTIALS")
  GOOGLE_APPLICATION_CREDENTIALS=/workspace/keys/$(basename "$GOOGLE_APPLICATION_CREDENTIALS")

Host-only absolute paths such as /home/<user>/... are not visible inside the
container unless they are explicitly mounted.
EOF
  exit 1
fi

export GOOGLE_CLOUD_PROJECT
export OBJECT_STORAGE_BUCKET
export BIGQUERY_DATASET
export GOOGLE_APPLICATION_CREDENTIALS
