#!/usr/bin/env bash

if [[ $# -ne 1 ]]; then
  echo "Usage: ${0##*/} --start | --stop"
  exit 1
fi

startNodes () {
  local antidote_nodes=( $(cat ${ANT_NODES}) )
  for node in "${antidote_nodes[@]}"; do
    scp -i ${EXPERIMENT_PRIVATE_KEY} ./control-nodes-remote.sh root@${node}:/root/
  done

  # Use ANT_IPS so localhost is replaced by the ip of the node
  ./execute-in-nodes.sh "$(cat ${ANT_IPS})" "./control-nodes-remote.sh start localhost" "-debug"
}

stopNodes () {
  local command="\
    [[ -f control-nodes-remote.sh ]] && ./control-nodes-remote.sh stop localhost; \
    pkill beam; \
    rm -rf antidote/_build/default/rel/antidote/data/*; \
    rm -rf antidote/_build/default/rel/antidote/log/*; \
  "
  ./execute-in-nodes.sh "$(cat ${ANT_IPS})" "${command}" "-debug"
}

case "$1" in
  "--start") startNodes;;
  "--stop") stopNodes
esac
