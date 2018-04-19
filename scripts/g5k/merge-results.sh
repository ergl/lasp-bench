#!/usr/bin/env bash

run () {
  local test_directory="$1"

  # Move the processing scripts to the test directory
  cp ../../priv/mergeLatencies.awk "${test_directory}"
  cp ../../priv/mergeSummary.awk  "${test_directory}"

  pushd "${test_directory}" > /dev/null 2>&1

  local index=0
  local -a result_folders
  for result_tar in *.tar; do
    # Remove .tar from file name
    extract_to="${result_tar::-4}"
    result_folders[${index}]="${extract_to}"
    mkdir -p "${extract_to}"
    # Extract the real contents of the tar to the extract_to folder
    tar -xf "${result_tar}" -C "${extract_to}" --strip-components 1
    index=$((index + 1))
  done

  rm ./*.tar

  local summary_str=""
  local latency_str=""
  for folder_name in "${result_folders[@]}"; do
    summary_str+="${folder_name}summary.csv "
    latency_str+="${folder_name}perform-operation_latencies.csv "
  done

  ./mergeSummary.awk "${summary_str}" > summary.csv
  ./mergeLatencies.awk "${latency_str}" > perform-operation_latencies.csv

  rm mergeSummary.awk
  rm mergeLatencies.awk

  popd > /dev/null 2>&1
}

run "$@"
