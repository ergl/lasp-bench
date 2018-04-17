#!/usr/bin/env bash

set -eo pipefail

source ./configuration.sh

if [[ $# -ne 2 ]]; then
  exit 1
fi

# transferIPs <bench_node> <antidote_ip_file>
# Transfer the antidote_ip_file to <bench_node>
transferIPs () {
  local bench_node="$1"
  local antidote_ips_file="$2"
  local antidote_ips_file_name=$(basename "${antidote_ips_file}")
  scp -i "${EXPERIMENT_PRIVATE_KEY}" "${antidote_ips_file}" root@"${bench_node}":/root/"${antidote_ips_file_name}" > /dev/null 2>&1
}

run() {
  local bench_node_file="${1}"
  local total_antidote_nodes="${2}"
  local antidote_ip_file=".antidote_ip_file"

  # This assumes that each bench node only talks to one Antidote node
  local file_offset=1
  while read bench_node; do
    sed -n "${file_offset}{p;q;}" "${ANT_IPS}" > "${antidote_ip_file}"
    transferIPs "${bench_node}" "${antidote_ip_file}"
    # If there are more clients than Antidote nodes, wrap around
    file_offset=$(( ((file_offset + 1) % total_antidote_nodes) + 1 ))
  done < "${bench_node_file}"
}

run "$@"
