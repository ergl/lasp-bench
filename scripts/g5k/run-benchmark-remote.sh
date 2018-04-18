#!/usr/bin/env bash

set -eo pipefail

runBench () {
  local config_file="$1"
  local config_path="examples/${config_file}"
  pushd /root/lasp-bench > /dev/null 2>&1
  ./_build/default/bin/lasp_bench "${config_path}"
  popd > /dev/null 2>&1
}

collectBench () {
  local own_node_name="${HOSTNAME::-12}" # remove the .grid5000.fr part of the name
  local result_name="results-${own_node_name}.tar"
  pushd /root/lasp-bench/tests > /dev/null 2>&1
  tar -zhcf "${result_name}" current
  mv "${result_name}" /root
  popd > /dev/null 2>&1
}

run () {
  local config_file="$1"
  runBench "${config_file}"
  collectBench
}

run "$@"
