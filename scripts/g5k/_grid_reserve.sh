#!/usr/bin/env bash

source ./configuration.sh

sites=( "${SITES[@]}" )

reserveSites () {
  local reservation
  local node_number=$((ANTIDOTE_NODES + BENCH_NODES))
  for site in "${sites[@]}"; do
    reservation+="${site}:rdef=/nodes=${node_number},"
  done
  # Trim the last (,) in the string
  reservation=${reservation%?}

  # Outputs something similar to:
  # ...
  # [OAR_GRIDSUB] Grid reservation id = 56670
  # ...

  local res_id=$(oargridsub -t deploy -w '2:00:00' "${reservation}" \
    | grep "Grid reservation id" \
    | cut -f2 -d=)

  # Trim any leading whitespace
  echo "${res_id## }"
}

reserveSites
