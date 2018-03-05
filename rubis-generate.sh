#!/usr/bin/env bash

generate() {
  local out_file="$(pwd)/${2}"
  pushd scripts/
  local load_file="$(pwd)"/output.json
  if [[ -a "${load_file}" ]]; then
    ./genbenchrun.escript "$(pwd)"/tables/"${1}" "$(pwd)"/output.json > "${out_file}"
  else
    popd
    exit 1
  fi
  popd
}

generate "$@"
