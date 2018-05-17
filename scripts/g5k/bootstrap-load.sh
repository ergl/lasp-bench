#!/usr/bin/env bash

if [[ $# -le 2 ]]; then
  exit 1
fi

loadRubis() {
  local antidote_ip="${1}"
  local bench_node="${2}"
  local load_size="${3}"

  local db_properties_file
  if [[ "${load_size}" == "small" ]]; then
    db_properties_file="rubis_db_properties_small.json"
  elif [[ "${load_size}" == "big" ]]; then
    db_properties_file="rubis_db_properties.json"
  fi

  local command="\
      cd /root/lasp-bench/scripts; \
      ./rubis_load.escript ${antidote_ip} 7878 ./${db_properties_file} ./rubis_output.json
  "

  # Perform the database load on only one node
  ssh -i "${EXPERIMENT_PRIVATE_KEY}" -T \
      -o ConnectTimeout=3 \
      -o StrictHostKeyChecking=no \
      root@"${bench_node}" "${command}" > /dev/null 2>&1

  # Copy the file back
  scp -i "${EXPERIMENT_PRIVATE_KEY}" \
      root@"${bench_node}":/root/lasp-bench/scripts/rubis_output.json "${DBLOADDIR}"/rubis_output.json > /dev/null 2>&1
}

loadBlotter() {
  local antidote_ip="${1}"
  local bench_node="${2}"

  local command="\
      cd /root/lasp-bench/scripts; \
      ./blotter_load.escript ${antidote_ip} 7878
  "

  # Perform the database load on only one node
  ssh -i "${EXPERIMENT_PRIVATE_KEY}" -T \
      -o ConnectTimeout=3 \
      -o StrictHostKeyChecking=no \
      root@"${bench_node}" "${command}" > /dev/null 2>&1
}

distributeLoadInfo() {
  # First, copy the load information
  while read bench_node; do
    scp -i "${EXPERIMENT_PRIVATE_KEY}" \
        "${DBLOADDIR}"/rubis_output.json \
        root@"${bench_node}":/root/lasp-bench/scripts/load_info.json > /dev/null 2>&1
  done < "${BENCH_NODES_FILE}"

  # Then, generate the benchmark file from that information
  local command="cd /root/lasp-bench/scripts/g5k/; ./generate-benchmark-file.sh"
  while read bench_node; do
    ssh -i "${EXPERIMENT_PRIVATE_KEY}" -T -n \
        -o ConnectTimeout=3 \
        -o StrictHostKeyChecking=no \
        root@"${bench_node}" "${command}" > /dev/null 2>&1
  done < "${BENCH_NODES_FILE}"
}

run() {
  local antidote_ip="${1}"
  local bench_type="${2}"

  # Bootstrap the database with some sample data
  local head_bench_node=$(head -n 1 "${BENCH_NODES_FILE}")

  if [[ "${bench_type}" == "rubis" ]]; then
    local load_size="${3}"
    loadRubis "${antidote_ip}" "${head_bench_node}" "${load_size}"
    # Distribute load information to all benchmark nodes
    distributeLoadInfo
  else
    loadBlotter "${antidote_ip}" "${head_bench_node}"
  fi
}


run "$@"
