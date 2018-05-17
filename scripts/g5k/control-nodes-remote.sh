#!/usr/bin/env bash

run () {
  local command="$1"
  local ip="${2}"
  local profile="${3:-default}"

  IP="${ip}" ./antidote/_build/"${profile}"/rel/antidote/bin/env "${command}"
}

run "$@"
