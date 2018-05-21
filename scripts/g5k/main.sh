#!/usr/bin/env bash

set -eo pipefail

IFS=$'\r\n' GLOBIGNORE='*' :;

SELF=$(readlink "$0" || true)
if [[ -z ${SELF} ]]; then
  SELF=$0
fi

cd "$(dirname "$SELF")"

source ./configuration.sh
export GLOBAL_TIMESTART=$(date +"%Y-%m-%d-%s")
sites=( "${SITES[@]}" )
export sites_size=${#sites[*]}

reserveSites () {
  local reservation
  local node_number=$((ANTIDOTE_NODES + BENCH_NODES))
  for site in "${sites[@]}"; do
    reservation+="${site}:rdef=/nodes=${node_number},"
  done
  # Trim the last (,) in the string
  reservation=${reservation%?}

  # Outputs something similar to:
  # ...
  # [OAR_GRIDSUB] Grid reservation id = 56670
  # ...

  local res_id=$(oargridsub -t deploy -w '2:00:00' "${reservation}" \
    | grep "Grid reservation id" \
    | cut -f2 -d=)

  # Trim any leading whitespace
  echo "${res_id## }"
}

promptJobCancel() {
  local grid_job="$1"
  local response
  read -r -n 1 -p "Want to cancel reservation? [y/n] " response
  case "${response}" in
    [yY] )
      oargriddel "${grid_job}"
      exit 1 ;;
    *)
      exit 0 ;;
  esac
}

if [[ "${RESERVE_SITES}" == "true" ]]; then
  echo "[RESERVING_SITES]: Starting..."
  export GRID_JOB_ID=$(reserveSites)

  if [[ -z "${GRID_JOB_ID}" ]]; then
    echo "Uh-oh! Something went wrong while reserving. Maybe try again?"
    exit 1
  fi

  sed -i.bak '/^GRID_JOB_ID.*/d' configuration.sh
  echo "GRID_JOB_ID=${GRID_JOB_ID}" >> configuration.sh
  echo "[RESERVING_SITES]: Done. Successfully reserved with id ${GRID_JOB_ID}"
else
  echo "[RESERVING_SITES]: Skipping"
fi

# Delete the reservation if script is killed
trap 'promptJobCancel ${GRID_JOB_ID}' SIGINT SIGTERM

getAntidoteBranch() {
  case "${BENCH_TYPE}" in
    "blotter" )
      echo "pvc-blotter"
      return ;;
    "rubis" )
      echo "pvc-rubis"
      return ;;
    "blotter-cc" )
      echo "tcp-server"
       return ;;
  esac
}
export ANTIDOTE_BRANCH=$(getAntidoteBranch)

getBenchFile() {
  case "${BENCH_TYPE}" in
    "blotter" | "blotter-cc" )
      echo "blotter.config"
      return ;;
    "rubis" )
      echo "rubis.config"
      return ;;
  esac
}
export BENCH_FILE=$(getBenchFile)

export HOMEFOLDER="/home/$(whoami)"
SCRATCHFOLDER="/home/$(whoami)/grid-benchmark-${GRID_JOB_ID}"
export LOGDIR=${SCRATCHFOLDER}/logs/${GLOBAL_TIMESTART}
RESULTSDIR=${SCRATCHFOLDER}/results
export DBLOADDIR=${SCRATCHFOLDER}/dbload

export EXPERIMENT_PRIVATE_KEY=${SCRATCHFOLDER}/key
EXPERIMENT_PUBLIC_KEY=${SCRATCHFOLDER}/exp_key.pub

export ALL_NODES_FILE=${SCRATCHFOLDER}/.all_nodes
export BENCH_NODES_FILE=${SCRATCHFOLDER}/.bench_nodes
export ANTIDOTE_NODES_FILE=${SCRATCHFOLDER}/.antidote_nodes

export ALL_IPS_FILE=${SCRATCHFOLDER}/.all_ips
BENCH_IPS_FILE=${SCRATCHFOLDER}/.bench_ips
export ANTIDOTE_IPS_FILE=${SCRATCHFOLDER}/.antidote_ips

# For each node / ip in a file (one each line),
# ssh into it and run the given command
doForNodesIn () {
  ./execute-in-nodes.sh "$(cat "$1")" "$2"
}

# Node Name -> IP
getIPs () {
  [[ -f "${ALL_IPS_FILE}" ]] && rm "${ALL_IPS_FILE}"
  [[ -f "${BENCH_IPS_FILE}" ]] && rm "${BENCH_IPS_FILE}"
  [[ -f "${ANTIDOTE_IPS_FILE}" ]] && rm "${ANTIDOTE_IPS_FILE}"

  while read n; do dig +short "${n}"; done < "${ANTIDOTE_NODES_FILE}" > "${ANTIDOTE_IPS_FILE}"
  while read n; do dig +short "${n}"; done < "${BENCH_NODES_FILE}" > "${BENCH_IPS_FILE}"
  while read n; do dig +short "${n}"; done < "${ALL_NODES_FILE}" > "${ALL_IPS_FILE}"
}


# Get all nodes in reservation, split them into
# antidote and lasp bench nodes.
gatherMachines () {
  echo "[GATHER_MACHINES]: Starting..."

  local antidote_nodes_per_site="${ANTIDOTE_NODES}"
  local benchmark_nodes_per_site="${BENCH_NODES}"

  [[ -f "${ALL_NODES_FILE}" ]] && rm "${ALL_NODES_FILE}"
  [[ -f "${ANTIDOTE_NODES_FILE}" ]] && rm "${ANTIDOTE_NODES_FILE}"
  [[ -f "${BENCH_NODES_FILE}" ]] && rm "${BENCH_NODES_FILE}"

  # Remove all blank lines and repeats
  # and add those to the full machine list
  oargridstat -w -l "${GRID_JOB_ID}" | sed '/^$/d' \
    | awk '!seen[$0]++' > "${ALL_NODES_FILE}"

  # For each site, get the list of nodes and slice
  # them into antidote and basho bench lists, depending on
  # the configuration given.
  for site in "${sites[@]}"; do
    awk < "${ALL_NODES_FILE}" "/${site}/ {print $1}" \
      | tee >(head -n "${antidote_nodes_per_site}" >> "${ANTIDOTE_NODES_FILE}") \
      | sed "1,${antidote_nodes_per_site}d" \
      | head -n "${benchmark_nodes_per_site}" >> "${BENCH_NODES_FILE}"
  done

  # Override the full node list, in case we didn't pick all the nodes
  cat "${BENCH_NODES_FILE}" "${ANTIDOTE_NODES_FILE}" > "${ALL_NODES_FILE}"

  getIPs

  echo "[GATHER_MACHINES]: Done"
}

# Use kadeploy to provision all the machines
kadeployNodes () {
  for site in "${sites[@]}"; do
    echo -e "[SYNC_IMAGE_${site}]: Starting..."

    local image_dir="$(dirname "${K3_IMAGE}")"

    # Place the K3 environment in every site
    # rsync can only create dirs up to two levels deep, so we create it just in case
    ssh -o StrictHostKeyChecking=no "${site}" "mkdir -p ${image_dir}"
    rsync "${image_dir}"/* "${site}:${image_dir}"

    # Create the results folder in every site
    rsync -r "${SCRATCHFOLDER}"/* "${site}:${SCRATCHFOLDER}"

    echo -e "[SYNC_IMAGE_${site}]: Done"

    echo -e "[DEPLOY_IMAGE_${site}]: Starting..."

    # Deploy the environment in every node in this site
    local command="\
      oargridstat -w -l ${GRID_JOB_ID} \
        | sed '/^$/d' \
        | awk '/${site}/ {print $1}' > ~/.todeploy && \
      kadeploy3 -f ~/.todeploy -a ${K3_IMAGE} -k ${EXPERIMENT_PUBLIC_KEY}
    "

    $(
      ssh -t -o StrictHostKeyChecking=no "${site}" "${command}" \
        > "${LOGDIR}/kadeploy-${site}.log" 2>&1
    ) &

    echo -e "[DEPLOY_IMAGE_${site}]: In progress"
  done
  echo "[DEPLOY_IMAGE]: Waiting. (This may take a while)"
  wait
}


provisionBench () {
  echo -e "[PROVISION_BENCH_NODES]: Starting..."

  local command="\
      rm -rf lasp-bench && \
      git clone ${BENCH_URL} --branch ${BENCH_BRANCH} --single-branch lasp-bench && \
      cd lasp-bench && \
      make all
    "

  while read node; do
    $(
      ssh -i "${EXPERIMENT_PRIVATE_KEY}" -T -n -o ConnectTimeout=3 -o StrictHostKeyChecking=no \
      root@"${node}" "${command}" >> "${LOGDIR}/provision-bench.log" 2>&1
    ) &
  done < "${BENCH_NODES_FILE}"

  wait

  echo -e "[PROVISION_BENCH_NODES]: Done"
}


provisionAntidote () {
  echo -e "[PROVISION_ANTIDOTE_NODES]: Starting... (This may take a while)"

  local rebar_deps
  local rebar_release
  if [[ "${BENCH_TYPE}" == "blotter" ]]; then
    rebar_deps="./rebar3 as microtest compile"
    rebar_release="./rebar3 as microtest release -n antidote"
  else
    rebar_deps="./rebar3 compile"
    rebar_release="./rebar3 release -n antidote"
  fi

  local command="\
    rm -rf antidote && \
    git clone ${ANTIDOTE_URL} --branch ${ANTIDOTE_BRANCH} --single-branch antidote && \
    cd antidote && \
    ${rebar_deps} && \
    ${rebar_release}
  "

  while read node; do
    $(
      ssh -i "${EXPERIMENT_PRIVATE_KEY}" -T -n -o ConnectTimeout=3 -o StrictHostKeyChecking=no \
      root@"${node}" "${command}" >> "${LOGDIR}/provision-antidote.log" 2>&1
    ) &
  done < "${ANTIDOTE_NODES_FILE}"

  wait

  echo -e "[PROVISION_ANTIDOTE_NODES]: Done"
}


rebuildAntidote () {
  echo -e "[REBUILD_ANTIDOTE]: Starting..."

  local rebar_clean
  local relclean_command
  local rebar_release
  if [[ "${BENCH_TYPE}" == "blotter" ]]; then
    rebar_clean="./rebar3 as microtest clean"
    relclean_command="rm -r _build/microtest/rel"
    rebar_release="./rebar3 as microtest release -n antidote"
  else
    rebar_clean="./rebar3 clean"
    relclean_command="rm -r _build/default/rel"
    rebar_release="./rebar3 release -n antidote"
  fi

  local command="\
    cd antidote; \
    pkill beam; \
    ${rebar_clean}; \
    ${relclean_command}; \
    ${rebar_release} \
  "
  # We use the IPs here so that we can change the default (127.0.0.1)
  doForNodesIn "${ANTIDOTE_IPS_FILE}" "${command}" \
    >> "${LOGDIR}/rebuild-antidote.log" 2>&1

  echo -e "[REBUILD_ANTIDOTE]: Done"
}

# Provision all the nodes with Antidote and Basho Bench
provisionNodes () {
  provisionAntidote
  provisionBench
}

setupTests () {
  echo "[SETUP_TESTS]: Starting..."

  local total_antidote_nodes=$((sites_size * ANTIDOTE_NODES))
  local antidote_nodes="${ANTIDOTE_NODES_FILE}"

  echo -e "[CHANGE_RING_SIZE]: Starting..."
  # Change the ring size of riak depending on the number of nodes
  ./change-partition-size.sh "${total_antidote_nodes}" "${antidote_nodes}"
  echo -e "[CHANGE_RING_SIZE]: Done"

  # Distribute Antidote IPs to benchmark nodes
  echo -e "[DISTRIBUTE_IPS]: Starting..."
  ./distribute-ips.sh "${BENCH_NODES_FILE}" "${total_antidote_nodes}"
  echo -e "[DISTRIBUTE_IPS]: Done"

  echo -e "[SETUP_TESTS]: Done"

  # Start Antidote and form a cluster
  ./prepare-clusters.sh "${total_antidote_nodes}"

  # Load the database with some initial data and
  # distribute key information to all bench nodes
  local antidote_head=$(head -n 1 "${ANTIDOTE_IPS_FILE}")
  echo "[LOAD_DATABASE]: Starting... (This may take a while)"

  # The load size only applies to the rubis benchmark
  if [[ "${BENCH_TYPE}" == "rubis" ]]; then
    ./bootstrap-load.sh "${antidote_head}" "${BENCH_TYPE}" "${RUBIS_LOAD_SIZE}"
  else
    ./bootstrap-load.sh "${antidote_head}" "${BENCH_TYPE}"
  fi
  echo "[LOAD_DATABASE]: Done"
}

runTests () {
  echo "[RUNNING_TEST]: Starting..."
  ./run-benchmark.sh >> "${LOGDIR}/bench-execution.log" 2>&1
  echo "[RUNNING_TEST]: Done"
}

collectResults () {
  echo "[COLLECTING_RESULTS]: Starting..."

  [[ -d "${RESULTSDIR}" ]] && rm -r "${RESULTSDIR}"
  mkdir -p "${RESULTSDIR}"

  while read bench_node; do
    scp -i "${EXPERIMENT_PRIVATE_KEY}" root@"${bench_node}":/root/*.tar "${RESULTSDIR}"
  done < "${BENCH_NODES_FILE}"

  echo "[COLLECTING_RESULTS]: Done"

  echo "[MERGING_RESULTS]: Starting..."
  ./merge-results.sh "${RESULTSDIR}" "${BENCH_TYPE}"
  echo "[MERGING_RESULTS]: Done"

  pushd "${SCRATCHFOLDER}" > /dev/null 2>&1
  local result_folder_name=$(basename "${RESULTSDIR}")
  local tar_name="${result_folder_name}-${GRID_JOB_ID}.tar"
  tar -zcf "${tar_name}" "${RESULTSDIR}"
  mv "${tar_name}" "${HOMEFOLDER}"
  popd > /dev/null 2>&1
}


# Prepare the experiment, create the output folder,
# logs and key pairs.
setupScript () {
  echo "[SETUP_KEYS]: Starting..."

  mkdir -p "${SCRATCHFOLDER}"
  mkdir -p "${LOGDIR}"
  mkdir -p "${DBLOADDIR}"
  cp "${PRKFILE}" "${EXPERIMENT_PRIVATE_KEY}"
  cp "${PBKFILE}" "${EXPERIMENT_PUBLIC_KEY}"

  echo "[SETUP_KEYS]: Done"
}


# Gather information about all the deployed machines, like
# node names and IPs, and split them into antidote and basho_bench
# nodes. If selected, it will also go ahead and deploy the k3 image
# into the nodes.
setupCluster () {
  gatherMachines
  if [[ "${DEPLOY_IMAGE}" == "true" ]]; then
    echo "[DEPLOY_IMAGE]: Starting..."
    kadeployNodes
    echo "[DEPLOY_IMAGE]: Done"
  else
    echo "[DEPLOY_IMAGE]: Skipping"
  fi
}

# Provision the nodes with the appropiate versions of antidote and
# basho_bench.
# Also create and distribute the erlang cookies to all nodes.
configCluster () {
  if [[ "${PROVISION_IMAGES}" == "true" ]]; then
    echo "[PROVISION_NODES]: Starting..."
    provisionNodes
    echo "[PROVISION_NODES]: Done"
  else
    echo "[PROVISION_NODES]: Skipping"
    echo "[CLEAN_RUN]: Starting..."
    rebuildAntidote
    echo "[CLEAN_RUN]: Done"
  fi
}


run () {
  setupScript

  setupCluster
  configCluster

  setupTests
  runTests
  collectResults

  echo "Benchmark done and packaged. All done! :^)"
}

run
