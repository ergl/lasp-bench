#!/usr/bin/env bash

generate() {
  local host="${1}"
  local port="${2}"
  local out_file="$(pwd)/${3}"
  pushd scripts/
  local load_file="$(pwd)"/output.json
  if [[ -a "${load_file}" ]]; then
    ./genbenchrun.escript "${host}" "${port}" "${load_file}" > "${out_file}"
  else
    popd
    exit 1
  fi
  popd
}

generate "$@"
