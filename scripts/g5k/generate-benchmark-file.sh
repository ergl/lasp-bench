#!/usr/bin/env bash

source ./configuration.sh

declare BENCH_FILE
declare LOAD_INFO_FILE
if [[ "${BENCH_TYPE}" =~ ^blotter.* ]]; then
  BENCH_FILE="blotter.config"
else
  BENCH_FILE="rubis.config"
  LOAD_INFO_FILE="/root/lasp-bench/scripts/load_info.json"
  if [[ ! -f "${LOAD_INFO_FILE}" ]]; then
    exit 1
  fi
fi

# The file containing the Antidote IP we should target
if [[ ! -f "/root/.antidote_ip_file" ]]; then
  exit 1
fi

run() {
  # We already got this file before during the transferIP phase
  local assigned_ip=$(head -n 1 "/root/.antidote_ip_file")

  # If we're running blotter, we just need to change the node we're targetting
  if [[ "${BENCH_TYPE}" =~ ^blotter.* ]]; then
    sed -i.bak "s/{rubis_ip.*/{rubis_ip,'${assigned_ip}'}."
    exit 0
  fi

  # If we're running rubis, we need to generate our config file
  # from the load info
  local out_file="/root/lasp-bench/examples/${BENCH_FILE}"
  pushd /root/lasp-bench/scripts/ > /dev/null 2>&1
  ./rubis_generate_bench_config.escript "${assigned_ip}" 7878 "${LOAD_INFO_FILE}" > "${out_file}"
  popd > /dev/null 2>&1
}

run
