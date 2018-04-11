#!/usr/bin/env bash

run () {
  local command="$1"
  local ip="${2}"

  IP="${ip}" ./antidote/_build/default/rel/antidote/bin/env "${command}"
}

run "$@"
