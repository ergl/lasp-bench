#!/usr/bin/env bash

generate() {
  local host="${1}"
  local port="${2}"
  local out_file="$(pwd)/${3}"
  pushd scripts/
  local load_file="$(pwd)"/rubis_output.json
  local ops_file="$(pwd)"/rubis_default_ops.json
  if [[ -a "${load_file}" ]]; then
    if [[ -a "${ops_file}" ]]; then
      ./rubis_generate_bench_config.escript "${host}" "${port}" "${load_file}" "${ops_file}" > "${out_file}"
    else
      ./rubis_generate_bench_config.escript "${host}" "${port}" "${load_file}" > "${out_file}"
    fi
  else
    popd
    exit 1
  fi
  popd
}

generate "$@"
