#!/usr/bin/env bash

if [[ $# -ne 1 ]]; then
  echo "Usage: ${0##*/} --start | --stop"
  exit 1
fi

startNodes () {
  # Transfer the remote control script first
  while read node; do
    scp -i "${EXPERIMENT_PRIVATE_KEY}" ./control-nodes-remote.sh root@"${node}":/root/ > /dev/null 2>&1
  done < "${ANTIDOTE_NODES_FILE}"

  # Use ANTIDOTE_IPS_FILE so localhost is replaced by the ip of the node
  ./execute-in-nodes.sh "$(cat ${ANTIDOTE_IPS_FILE})" "./control-nodes-remote.sh start localhost" "-debug"
}

stopNodes () {
  local command="\
    [[ -f control-nodes-remote.sh ]] && ./control-nodes-remote.sh stop localhost; \
    pkill beam; \
    rm -rf antidote/_build/default/rel/antidote/data/*; \
    rm -rf antidote/_build/default/rel/antidote/log/*; \
  "
  ./execute-in-nodes.sh "$(cat ${ANTIDOTE_IPS_FILE})" "${command}" "-debug"
}

case "$1" in
  "--start") startNodes;;
  "--stop") stopNodes
esac
