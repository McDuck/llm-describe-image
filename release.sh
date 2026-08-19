#!/usr/bin/env bash

# Create portable source releases for each deployable application.
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
release_dir="${repo_root}/dist"
applications=(
  "describe_media"
  "recognition_review"
  "external_gpu_host"
)

for command in tar; do
  command -v "$command" >/dev/null 2>&1 || {
    printf 'Required command was not found: %s\n' "$command" >&2
    exit 1
  }
done

mkdir -p -- "$release_dir"

for application in "${applications[@]}"; do
  application_path="${repo_root}/${application}"
  archive_path="${release_dir}/${application}.tar.gz"

  if [[ ! -d "$application_path" ]]; then
    printf 'Application directory was not found: %s\n' "$application_path" >&2
    exit 1
  fi

  printf 'Creating %s\n' "$archive_path"
  tar \
    --create \
    --gzip \
    --file="$archive_path" \
    --directory="$repo_root" \
    --exclude='*.pyc' \
    --exclude='*.egg-info' \
    --exclude='.git' \
    --exclude='__pycache__' \
    --exclude='.pytest_cache' \
    --exclude='.venv' \
    --exclude='.venv-*' \
    --exclude='.env' \
    --exclude='.private' \
    --exclude='.ssh-*-known-hosts' \
    --exclude='build' \
    --exclude='dist' \
    --exclude='sample-output' \
    "$application"
done

printf 'Release archives created in %s\n' "$release_dir"
