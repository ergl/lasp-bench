#!/usr/bin/env bash

load() {
  pushd scripts/
  ./initdb.escript "${1}" "${2}" "$(pwd)"/dbPropertiesSmall.json "$(pwd)"/output.json
  popd
}

load "$@"

