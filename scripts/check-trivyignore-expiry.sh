#!/usr/bin/env bash
set -euo pipefail

ignore_file="${1:-.trivyignore.yaml}"

if [[ ! -f "${ignore_file}" ]]; then
  echo "Trivy ignore file not found: ${ignore_file}" >&2
  exit 1
fi

today="$(date -u +%Y-%m-%d)"
today_epoch=""
current_id=""
current_expiry=""
failed=0

date_to_epoch() {
  local value="$1"

  if date -u -d "${value}" +%s >/dev/null 2>&1; then
    date -u -d "${value}" +%s
    return
  fi

  if date -u -j -f "%Y-%m-%d" "${value}" +%s >/dev/null 2>&1; then
    date -u -j -f "%Y-%m-%d" "${value}" +%s
    return
  fi

  return 1
}

today_epoch="$(date_to_epoch "${today}")"

check_current_entry() {
  if [[ -z "${current_id}" ]]; then
    return
  fi

  if [[ -z "${current_expiry}" ]]; then
    echo "${current_id}: missing expired_at" >&2
    failed=1
    return
  fi

  local expiry_epoch
  if ! expiry_epoch="$(date_to_epoch "${current_expiry}")"; then
    echo "${current_id}: invalid expired_at '${current_expiry}'" >&2
    failed=1
    return
  fi

  if (( expiry_epoch < today_epoch )); then
    echo "${current_id}: expired on ${current_expiry}" >&2
    failed=1
  fi
}

while IFS= read -r line || [[ -n "${line}" ]]; do
  if [[ "${line}" =~ ^[[:space:]]*-[[:space:]]id:[[:space:]]*(.+)[[:space:]]*$ ]]; then
    check_current_entry
    current_id="${BASH_REMATCH[1]}"
    current_expiry=""
    continue
  fi

  if [[ "${line}" =~ ^[[:space:]]*expired_at:[[:space:]]*([0-9]{4}-[0-9]{2}-[0-9]{2})[[:space:]]*$ ]]; then
    current_expiry="${BASH_REMATCH[1]}"
  fi
done < "${ignore_file}"

check_current_entry

if [[ "${failed}" -ne 0 ]]; then
  echo "One or more Trivy ignore exceptions are expired or invalid." >&2
  exit 1
fi

echo "All Trivy ignore exceptions are valid through ${today}."
