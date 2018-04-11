#!/usr/bin/env bash

set -eo pipefail

source ./configuration.sh

runRemoteBenchmark () {
  local instances="$1"
  local benchmark_configuration_file="$2"
  local antidote_ip_file="$3"

  local bench_nodes=( $(< ${BENCH_NODEF}) )
  for node in "${bench_nodes[@]}"; do
    scp -i ${EXPERIMENT_PRIVATE_KEY} ./run-benchmark-remote.sh root@${node}:/root/
  done

  ./execute-in-nodes.sh "$(< ${BENCH_NODEF})" \
      "./run-benchmark-remote.sh ${antidote_ip_file} ${instances} ${benchmark_configuration_file}"
}

run () {
  local antidote_ip_file=".antidote_ip_file"
  local bench_instances="${BENCH_INSTANCES}"
  local benchmark_configuration_file="${BENCH_FILE}"
  runRemoteBenchmark "${bench_instances}" "${benchmark_configuration_file}" "${antidote_ip_file}"
}

run
