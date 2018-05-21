#!/usr/bin/env bash

mergeRubis () {
  local -a rubis_folders=("${@}")

  local -a summary_arr
  local -a latency_arr

  local folder_index=0
  for folder_name in "${rubis_folders[@]}"; do
    summary_arr[${folder_index}]="${folder_name}/summary.csv"
    latency_arr[${folder_index}]="${folder_name}/perform-operation_latencies.csv"
    folder_index=$((folder_index + 1))
  done

  ./mergeSummary.awk "${summary_arr[@]}" > summary.csv
  ./mergeLatencies.awk "${latency_arr[@]}" > perform-operation_latencies.csv
}

mergeBlotter () {
  local -a blotter_folders=("${@}")

  local -a summary_arr

  local ping_present; local -a ping_arr
  local readonly_present; local -a readonly_arr
  local readwrite_present; local -a readwrite_arr

  local folder_index=0
  for folder_name in "${blotter_folders[@]}"; do
    summary_arr[${folder_index}]="${folder_name}/summary.csv"

    local ping_file="${folder_name}/ping_latencies.csv"
    local readonly_file="${folder_name}/readonly_latencies.csv"
    local readwrite_file="${folder_name}/readwrite_latencies.csv"

    # Mark the files as existing, but only on the first folder
    # if the first folder contains the files, then all folders do,
    # as they share the same config file
    if [[ "${folder_index}" -eq 0 ]]; then
      if [[ -f "${ping_file}" ]]; then ping_present=1; fi
      if [[ -f "${readonly_file}" ]]; then readonly_present=1; fi
      if [[ -f "${readwrite_file}" ]]; then readwrite_present=1; fi
    fi

    ping_arr[${folder_index}]="${ping_file}"
    readonly_arr[${folder_index}]="${readonly_file}"
    readwrite_arr[${folder_index}]="${readwrite_file}"

    folder_index=$((folder_index + 1))
  done

  ./mergeSummary.awk "${summary_arr[@]}" > summary.csv

  # Now, only merge the latencies of the operations that were present

  if [[ "${ping_present}" -eq 1 ]]; then
    ./mergeLatencies.awk "${ping_arr[@]}" > ping_latencies.csv
  fi

  if [[ "${readonly_present}" -eq 1 ]]; then
    ./mergeLatencies.awk "${readonly_arr[@]}" > readonly_latencies.csv
  fi

  if [[ "${readwrite_present}" -eq 1 ]]; then
    ./mergeLatencies.awk "${readwrite_arr[@]}" > readwrite_latencies.csv
  fi
}

run () {
  local priv_directory="$1"
  local test_directory="$2"
  local benchmark_type="$3"

  # Move the processing scripts to the test directory
  cp "${priv_directory}"/mergeLatencies.awk "${test_directory}"
  cp "${priv_directory}"/mergeSummary.awk  "${test_directory}"

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

  if [[ "${benchmark_type}" == "rubis" ]]; then
    mergeRubis "${result_folders[@]}"
  elif [[ "${benchmark_type}" =~ ^blotter.* ]]; then
    mergeBlotter "${result_folders[@]}"
  fi

  rm mergeSummary.awk
  rm mergeLatencies.awk

  popd > /dev/null 2>&1
}

run "$@"
