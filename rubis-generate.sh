#!/usr/bin/env bash

generate() {
  local host="${1}"
  local port="${2}"
  local out_file="$(pwd)/${3}"
  pushd scripts/
  local load_file="$(pwd)"/output.json
  local default_ops="$(pwd)"/default_ops.json
  if [[ -a "${load_file}" ]]; then
    if [[ -a "${default_ops}" ]]; then
      ./genbenchrun.escript "${host}" "${port}" "${load_file}" "${default_ops}" > "${out_file}"
    else
      ./genbenchrun.escript "${host}" "${port}" "${load_file}" > "${out_file}"
    fi
  else
    popd
    exit 1
  fi
  popd
}

generate "$@"
