#!/usr/bin/env bash

set -eo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: ${0##*/} total-nodes"
  exit 1
fi

joinLocalDC () {
  local antidote_nodes=( $(cat "${ANT_NODES}") )
  # We will execute the command only in one node
  local head="${antidote_nodes[0]}"

  local nodes_str
  for node in "${antidote_nodes[@]}"; do
    nodes_str+="'antidote@${node}' "
  done

  nodes_str=${nodes_str%?}

  local join_dc="\
    ./antidote/bin/join_cluster_script.erl ${nodes_str}
  "

  ./execute-in-nodes.sh "${head}" "${join_dc}" "-debug"
}

joinNodes () {
  local cluster_size=$1

  # No point in clustering if we have only 1 node
  if [[ ${cluster_size} -le 1 ]]; then
    echo -e "\t[JOIN_ANTIDOTE_NODES]: Skipping"
  else
    echo -e "\t[JOIN_ANTIDOTE_NODES]: Starting..."

    joinLocalDC >> "${LOGDIR}"/join-local-dc${GLOBAL_TIMESTART} 2>&1

    echo -e "\t[JOIN_ANTIDOTE_NODES]: Done"
  fi
}

run () {
  local cluster_size=$1

  joinNodes "${cluster_size}"
}

run "$@"
