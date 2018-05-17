#!/usr/bin/env bash

set -eo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: ${0##*/} total-nodes"
  exit 1
fi

joinLocalDC () {
  local antidote_ips=( $(cat "${ANTIDOTE_IPS_FILE}") )
  # We will execute the command only in one node
  local head="${antidote_ips[0]}"

  local nodes_str
  for antidote_ip in "${antidote_ips[@]}"; do
    nodes_str+="'antidote@${antidote_ip}' "
  done

  # Remove last space
  nodes_str=${nodes_str%?}

  ssh -i "${EXPERIMENT_PRIVATE_KEY}" -T \
          -o ConnectTimeout=3 \
          -o StrictHostKeyChecking=no \
          root@"${head}" "./antidote/bin/join_cluster_script.erl ${nodes_str}"
}

joinNodes () {
  local cluster_size=$1

  # No point in clustering if we have only 1 node
  if [[ ${cluster_size} -le 1 ]]; then
    echo -e "[JOIN_ANTIDOTE_NODES]: Skipping"
  else
    echo -e "[JOIN_ANTIDOTE_NODES]: Starting..."

    joinLocalDC >> "${LOGDIR}/join-local-dc${GLOBAL_TIMESTART}" 2>&1

    echo -e "[JOIN_ANTIDOTE_NODES]: Done"
  fi
}

run () {
  local cluster_size=$1

  joinNodes "${cluster_size}"
}

run "$@"
