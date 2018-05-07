#!/usr/bin/env bash

load() {
  pushd scripts/
  ./rubis_load.escript "${1}" "${2}" "$(pwd)"/rubis_db_properties_small.json "$(pwd)"/rubis_output.json
  popd
}

load "$@"

