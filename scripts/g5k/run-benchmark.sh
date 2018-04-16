#!/usr/bin/env bash

set -eo pipefail

source ./configuration.sh

run () {
  while read bench_node; do
    scp -i ${EXPERIMENT_PRIVATE_KEY} ./run-benchmark-remote.sh root@"${bench_node}":/root/
  done < ${BENCH_NODEF}
  while read bench_node; do
    ssh -i ${EXPERIMENT_PRIVATE_KEY} -T \
          -o ConnectTimeout=3 \
          -o StrictHostKeyChecking=no \
          root@"${bench_node}" "/root/run-benchmark-remote.sh ${BENCH_FILE}" &
  done < ${BENCH_NODEF}

  wait
}

run
