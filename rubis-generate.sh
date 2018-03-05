#!/usr/bin/env bash

generate() {
  local host="${1}"
  local port="${2}"
  local table_file="$(pwd)"/scripts/tables/"${3}"
  local out_file="$(pwd)/${4}"
  pushd scripts/
  local load_file="$(pwd)"/output.json
  if [[ -a "${load_file}" ]]; then
    ./genbenchrun.escript "${host}" "${port}" "${table_file}" "$(pwd)"/output.json > "${out_file}"
  else
    popd
    exit 1
  fi
  popd
}

generate "$@"
