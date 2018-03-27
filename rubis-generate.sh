#!/usr/bin/env bash

generate() {
  local host="${1}"
  local port="${2}"
  local table_file="$(pwd)"/scripts/tables/"${3}"
  local out_file="$(pwd)/${4}"
  pushd scripts/
  local load_file="$(pwd)"/output.json
  local default_ops="$(pwd)"/default_ops.json
  if [[ -a "${load_file}" ]]; then
    if [[ -a "${default_ops}" ]]; then
      ./genbenchrun.escript "${host}" "${port}" "${table_file}" "${load_file}" "${default_ops}" > "${out_file}"
    else
      ./genbenchrun.escript "${host}" "${port}" "${table_file}" "${load_file}" > "${out_file}"
    fi
  else
    popd
    exit 1
  fi
  popd
}

generate "$@"
