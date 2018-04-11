#!/usr/bin/env bash

set -eo pipefail

source ./configuration.sh

if [[ $# -ne 1 ]]; then
  exit 1
fi

# transferIPs <bench_file> <antidote_ip_file>
# Transfer the antidote_ip_file to every node
# in <bench_file>
transferIPs () {
  local bench_node_file="$1"
  local antidote_ips_file="$2"
  local antidote_ips_file_name=$(basename "${antidote_ips_file}")

  while read node; do
    scp -i "${EXPERIMENT_PRIVATE_KEY}" "${antidote_ips_file}" root@"${node}":/root/"${antidote_ips_file_name}"
  done < "${bench_node_file}"
}

run() {
  local total_nodes="${1}"
  local antidote_ip_file=".antidote_ip_file"

  # This assumes that each bench node only talks to one Antidote node
  local ant_offset=0
  local bench_offset=0
  for _ in $(seq 1 ${total_nodes}); do
    # FIXME(borja): Change this if we allow more than one node per site
    # Each bench node will receive one Antidote node IP to talk to
    head -n $((ANTIDOTE_NODES + ant_offset)) "${ANT_IPS}" > "${antidote_ip_file}"
    head -n $((BENCH_NODES + bench_offset)) "${BENCH_NODEF}" > .lasp_bench_nodes

    transferIPs .lasp_bench_nodes "${antidote_ip_file}"

    ant_offset=$((ant_offset + ANTIDOTE_NODES))
    bench_offset=$((bench_offset + BENCH_NODES))
  done
}

run "$@"
