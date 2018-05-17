#!/usr/bin/env bash

if [[ $# -ne 1 ]]; then
  echo "Usage: ${0##*/} --start | --stop"
  exit 1
fi

startSync () {
  local antidote_nodes=( $(cat ${ANTIDOTE_NODES_FILE}) )
  for node in "${antidote_nodes[@]}"; do
    scp -i ${EXPERIMENT_PRIVATE_KEY} ./sync-time-remote.sh root@${node}:/root/
  done

  ./execute-in-nodes.sh "$(cat ${ANTIDOTE_NODES_FILE})" "./sync-time-remote.sh --start" "-debug"
}

stopSync () {
  ./execute-in-nodes.sh "$(cat ${ANTIDOTE_NODES_FILE})" "./sync-time-remote.sh --stop" "-debug"
}

case "$1" in
  "--start") startSync;;
  "*") stopSync
esac
