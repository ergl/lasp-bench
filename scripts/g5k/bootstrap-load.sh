#!/usr/bin/env bash

if [[ $# -ne 3 ]]; then
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
      ./initdb.escript ${antidote_ip} 7878 ./${db_properties_file}./output.json
  "

  # Perform the database load on only one node
  ssh -i ${EXPERIMENT_PRIVATE_KEY} -T \
      -o ConnectTimeout=3 \
      -o StrictHostKeyChecking=no \
      root@"${bench_node}" "${command}"

  # Copy the file back
  scp -i ${EXPERIMENT_PRIVATE_KEY} \
      root@"${bench_node}":/root/lasp-bench/scripts/output.json "${BDLOADDIR}"/output.json
}

distributeLoadInfo() {
  local bench_nodes="${1}"

  while read node; do
    # First, copy the load information
    scp -i ${EXPERIMENT_PRIVATE_KEY} \
        "${BDLOADDIR}"/output.json \
        root@"${node}":/root/lasp-bench/scripts/load_info.json

    # Then, generate the benchmark file from that information
    ssh -i ${EXPERIMENT_PRIVATE_KEY} -T \
        -o ConnectTimeout=3 \
        -o StrictHostKeyChecking=no \
        root@"${node}" "./root/lasp-bench/scripts/g5k/generate-benchmark-file.sh"
  done < "${bench_nodes}"
}

run() {
  local antidote_ip="${1}"
  local all_bench_nodes="${2}"
  local load_size="${3}"

  # Bootstrap the database with some sample data
  local head_bench_node=$(head -n 1 "${all_bench_nodes}")
  loadDatabase "${antidote_ip}" "${head_bench_node}" "${load_size}"

  # Distribute load information to all benchmark nodes
  distributeLoadInfo "${all_bench_nodes}"
}


run "$@"