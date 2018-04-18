#!/usr/bin/env bash

if [[ $# -ne 2 ]]; then
  exit 1
fi

loadDatabase() {
  local antidote_ip="${1}"
  local bench_node="${2}"
  local load_size="${3}"

  local db_properties_file
  if [[ "${load_size}" == "small" ]]; then
    db_properties_file="dbPropertiesSmall.json"
  elif [[ "${load_size}" == "big" ]]; then
    db_properties_file="dbProperties.json"
  fi

  local command="\
      cd /root/lasp-bench/scripts; \
      ./initdb.escript ${antidote_ip} 7878 ./${db_properties_file} ./output.json
  "

  # Perform the database load on only one node
  ssh -i ${EXPERIMENT_PRIVATE_KEY} -T \
      -o ConnectTimeout=3 \
      -o StrictHostKeyChecking=no \
      root@"${bench_node}" "${command}" > /dev/null 2>&1

  # Copy the file back
  scp -i ${EXPERIMENT_PRIVATE_KEY} \
      root@"${bench_node}":/root/lasp-bench/scripts/output.json "${DBLOADDIR}"/output.json > /dev/null 2>&1
}

distributeLoadInfo() {
  # First, copy the load information
  while read bench_node; do
    scp -i ${EXPERIMENT_PRIVATE_KEY} \
        "${DBLOADDIR}"/output.json \
        root@"${bench_node}":/root/lasp-bench/scripts/load_info.json > /dev/null 2>&1
  done < "${BENCH_NODEF}"

  # Then, generate the benchmark file from that information
  local command="cd /root/lasp-bench/scripts/g5k/; ./generate-benchmark-file.sh"
  while read bench_node; do
    ssh -i ${EXPERIMENT_PRIVATE_KEY} -T -n \
        -o ConnectTimeout=3 \
        -o StrictHostKeyChecking=no \
        root@"${bench_node}" "${command}" > /dev/null 2>&1
  done < "${BENCH_NODEF}"
}

run() {
  local antidote_ip="${1}"
  local load_size="${2}"

  # Bootstrap the database with some sample data
  local head_bench_node=$(head -n 1 "${BENCH_NODEF}")
  loadDatabase "${antidote_ip}" "${head_bench_node}" "${load_size}"

  # Distribute load information to all benchmark nodes
  distributeLoadInfo
}


run "$@"
