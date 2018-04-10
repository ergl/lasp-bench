#!/usr/bin/env bash

run () {
  local total_nodes="${1}"

  echo "[STOP_ANTIDOTE]: Starting..."
  ./control-nodes.sh --stop
  echo "[STOP_ANTIDOTE]: Done"

  echo "[START_ANTIDOTE]: Starting..."
  ./control-nodes.sh --start
  echo "[START_ANTIDOTE]: Done"

  # TODO: Find a better way to do this -> Wait until all the nodes respond to ping?
  sleep 30

  echo "[BUILD_CLUSTER]: Starting..."
  ./join-clusters.sh "${total_nodes}"
  echo "[BUILD_CLUSTER]: Done"
}

run "$@"
