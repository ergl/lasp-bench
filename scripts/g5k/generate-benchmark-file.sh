#!/usr/bin/env bash

source ./configuration.sh

# The file containing the db load information
LOAD_INFO_FILE="/root/lasp-bench/scripts/load_info.json"

if [[ ! -f "${LOAD_INFO_FILE}" ]]; then
  exit 1
fi

# The file containing the Antidote IP we should target
if [[ ! -f "/root/.antidote_ip_file" ]]; then
  exit 1
fi

run() {
  # We already got this file before during the transferIP phase
  local assigned_ip=$(head -n 1 "/root/.antidote_ip_file")
  local out_file="${BENCH_FILE}"
  pushd /root/lasp-bench/scripts/
  ./genbenchrun.escript "${assigned_ip}" 7878 "${LOAD_INFO_FILE}" > "${out_file}"
  popd
}

run