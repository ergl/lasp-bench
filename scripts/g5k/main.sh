#!/usr/bin/env bash

set -eo pipefail

IFS=$'\r\n' GLOBIGNORE='*' :;

SELF=$(readlink $0 || true)
if [[ -z ${SELF} ]]; then
  SELF=$0
fi

cd $(dirname "$SELF")

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

SCRATCHFOLDER="/home/$(whoami)/grid-benchmark-${GRID_JOB_ID}"
export LOGDIR=${SCRATCHFOLDER}/logs
RESULTSDIR=${SCRATCHFOLDER}/results
export DBLOADDIR=${SCRATCHFOLDER}/dbload

export EXPERIMENT_PRIVATE_KEY=${SCRATCHFOLDER}/key
EXPERIMENT_PUBLIC_KEY=${SCRATCHFOLDER}/exp_key.pub

export ALL_NODES=${SCRATCHFOLDER}/.all_nodes
export BENCH_NODEF=${SCRATCHFOLDER}/.bench_nodes
export ANT_NODES=${SCRATCHFOLDER}/.antidote_nodes

export ALL_IPS=${SCRATCHFOLDER}/.all_ips
BENCH_IPS=${SCRATCHFOLDER}/.bench_ips
export ANT_IPS=${SCRATCHFOLDER}/.antidote_ips

# For each node / ip in a file (one each line),
# ssh into it and run the given command
doForNodesIn () {
  ./execute-in-nodes.sh "$(cat "$1")" "$2"
}


# Node Name -> IP
getIPs () {
  [[ -f ${ALL_IPS} ]] && rm ${ALL_IPS}
  [[ -f ${BENCH_IPS} ]] && rm ${BENCH_IPS}
  [[ -f ${ANT_IPS} ]] && rm ${ANT_IPS}

  while read n; do dig +short "${n}"; done < ${ANT_NODES} > ${ANT_IPS}
  while read n; do dig +short "${n}"; done < ${BENCH_NODEF} > ${BENCH_IPS}
  while read n; do dig +short "${n}"; done < ${ALL_NODES} > ${ALL_IPS}
}


# Get all nodes in reservation, split them into
# antidote and lasp bench nodes.
gatherMachines () {
  echo "[GATHER_MACHINES]: Starting..."

  local antidote_nodes_per_site="${ANTIDOTE_NODES}"
  local benchmark_nodes_per_site="${BENCH_NODES}"

  [[ -f ${ALL_NODES} ]] && rm ${ALL_NODES}
  [[ -f ${ANT_NODES} ]] && rm ${ANT_NODES}
  [[ -f ${BENCH_NODEF} ]] && rm ${BENCH_NODEF}

  # Remove all blank lines and repeats
  # and add those to the full machine list
  oargridstat -w -l ${GRID_JOB_ID} | sed '/^$/d' \
    | awk '!seen[$0]++' > ${ALL_NODES}

  # For each site, get the list of nodes and slice
  # them into antidote and basho bench lists, depending on
  # the configuration given.
  for site in "${sites[@]}"; do
    awk < ${ALL_NODES} "/${site}/ {print $1}" \
      | tee >(head -n "${antidote_nodes_per_site}" >> ${ANT_NODES}) \
      | sed "1,${antidote_nodes_per_site}d" \
      | head -n "${benchmark_nodes_per_site}" >> ${BENCH_NODEF}
  done

  # Override the full node list, in case we didn't pick all the nodes
  cat ${BENCH_NODEF} ${ANT_NODES} > ${ALL_NODES}

  getIPs

  echo "[GATHER_MACHINES]: Done"
}

# Use kadeploy to provision all the machines
kadeployNodes () {
  for site in "${sites[@]}"; do
    echo -e "\t[SYNC_IMAGE_${site}]: Starting..."

    local image_dir="$(dirname "${K3_IMAGE}")"

    # Place the K3 environment in every site
    # rsync can only create dirs up to two levels deep, so we create it just in case
    ssh -o StrictHostKeyChecking=no ${site} "mkdir -p ${image_dir}"
    rsync "${image_dir}"/* ${site}:"${image_dir}"

    # Create the results folder in every site
    rsync -r "${SCRATCHFOLDER}"/* ${site}:"${SCRATCHFOLDER}"

    echo -e "\t[SYNC_IMAGE_${site}]: Done"

    echo -e "\t[DEPLOY_IMAGE_${site}]: Starting..."

    # Deploy the environment in every node in this site
    local command="\
      oargridstat -w -l ${GRID_JOB_ID} \
        | sed '/^$/d' \
        | awk '/${site}/ {print $1}' > ~/.todeploy && \
      kadeploy3 -f ~/.todeploy -a ${K3_IMAGE} -k ${EXPERIMENT_PUBLIC_KEY}
    "

    $(
      ssh -t -o StrictHostKeyChecking=no ${site} "${command}" \
        > ${LOGDIR}/${site}-kadeploy-${GLOBAL_TIMESTART} 2>&1
    ) &

    echo -e "\t[DEPLOY_IMAGE_${site}]: In progress"
  done
  echo "[DEPLOY_IMAGE]: Waiting. (This may take a while)"
  wait
}


provisionBench () {
  echo -e "\t[PROVISION_BENCH_NODES]: Starting..."

  local command="\
      rm -rf lasp-bench && \
      git clone ${BENCH_URL} --branch ${BENCH_BRANCH} --single-branch lasp-bench && \
      cd lasp-bench && \
      make all
    "

  doForNodesIn ${BENCH_NODEF} "${command}" >> "${LOGDIR}/lasp-bench-compile-job-${GLOBAL_TIMESTART}" 2>&1

  echo -e "\t[PROVISION_BENCH_NODES]: Done"
}


provisionAntidote () {
  echo -e "\t[PROVISION_ANTIDOTE_NODES]: Starting... (This may take a while)"

  local command="\
    rm -rf antidote && \
    git clone ${ANTIDOTE_URL} --branch ${ANTIDOTE_BRANCH} --single-branch antidote && \
    cd antidote && \
    make relgrid
  "
  # We need antidote in all nodes even if we don't use it
  # lasp_bench will need the sources to start
  doForNodesIn ${ALL_NODES} "${command}" \
    >> "${LOGDIR}/antidote-compile-and-config-job-${GLOBAL_TIMESTART}" 2>&1

  echo -e "\t[PROVISION_ANTIDOTE_NODES]: Done"
}


rebuildAntidote () {
  echo -e "\t[REBUILD_ANTIDOTE]: Starting..."
  local command="\
    cd antidote; \
    pkill beam; \
    rm -rf deps; mkdir deps; \
    make clean; make relclean; make relgrid
  "
  # We use the IPs here so that we can change the default (127.0.0.1)
  doForNodesIn ${ANT_IPS} "${command}" \
    >> "${LOGDIR}/config-antidote-${GLOBAL_TIMESTART}" 2>&1

  echo -e "\t[REBUILD_ANTIDOTE]: Done"
}

cleanAntidote () {
  echo -e "\t[CLEAN_ANTIDOTE]: Starting..."

  local command="\
    cd antidote; \
    pkill beam; \
    make clean; \
    make relcleanl \
    make relgrid
  "
  doForNodesIn ${ANT_IPS} "${command}" \
    >> ${LOGDIR}/clean-antidote-${GLOBAL_TIMESTART} 2>&1

  echo -e "\t[CLEAN_ANTIDOTE]: Done"
}


# Provision all the nodes with Antidote and Basho Bench
provisionNodes () {
  provisionAntidote
  provisionBench
}

setupTests () {
  echo "[SETUP_TESTS]: Starting..."

  local total_antidote_nodes=$((sites_size * ANTIDOTE_NODES))
  local antidote_nodes="${ANT_NODES}"

  echo -e "\t[CHANGE_RING_SIZE]: Starting..."
  # Change the ring size of riak depending on the number of nodes
  ./change-partition-size.sh "${total_antidote_nodes}" "${antidote_nodes}"
  echo -e "\t[CHANGE_RING_SIZE]: Done"

  # Distribute Antidote IPs to benchmark nodes
  echo -e "\t[DISTRIBUTE_IPS]: Starting..."
  ./distribute-ips.sh "${BENCH_NODEF}"
  echo -e "\t[DISTRIBUTE_IPS]: Done"

  echo -e "[SETUP_TESTS]: Done"

  # Start Antidote and form a cluster
  ./prepare-clusters.sh "${total_antidote_nodes}"

  # Load the database with some initial data and
  # distribute key information to all bench nodes
  local antidote_head=$(head -n 1 "${ANT_IPS}")
  local load_size="${LOAD_SIZE}"
  echo "[LOAD_DATABASE]: Starting... (This may take a while)"
  ./bootstrap-load.sh "${antidote_head}" "${load_size}"
  echo "[LOAD_DATABASE]: Done"
}

runTests () {
  echo "[RUNNING_TEST]: Starting..."
  ./run-benchmark.sh >> ${LOGDIR}/basho-bench-execution-${GLOBAL_TIMESTART} 2>&1
  echo "[RUNNING_TEST]: Done"
}

collectResults () {
  echo "[COLLECTING_RESULTS]: Starting..."
  [[ -d "${RESULTSDIR}" ]] && rm -r "${RESULTSDIR}"
  mkdir -p "${RESULTSDIR}"
  local bench_nodes=( $(< ${BENCH_NODEF}) )
  for node in "${bench_nodes[@]}"; do
    scp -i ${EXPERIMENT_PRIVATE_KEY} root@${node}:/root/test* "${RESULTSDIR}"
  done
  echo "[COLLECTING_RESULTS]: Done"

  echo "[MERGING_RESULTS]: Starting..."
  ./merge-results.sh "${RESULTSDIR}"
  echo "[MERGING_RESULTS]: Done"

  pushd "${RESULTSDIR}" > /dev/null 2>&1
  local tar_name=$(basename "${RESULTSDIR}")
  tar -czf ../"${tar_name}".tar .
  popd > /dev/null 2>&1
}


# Prepare the experiment, create the output folder,
# logs and key pairs.
setupScript () {
  echo "[SETUP_KEYS]: Starting..."

  mkdir -p ${SCRATCHFOLDER}
  mkdir -p ${LOGDIR}
  mkdir -p ${DBLOADDIR}
  cp ${PRKFILE} ${EXPERIMENT_PRIVATE_KEY}
  cp ${PBKFILE} ${EXPERIMENT_PUBLIC_KEY}

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
  fi

  if [[ "${CLEAN_RUN}" == "true" ]]; then
    echo "[CLEAN_RUN]: Starting..."
    rebuildAntidote
    echo "[CLEAN_RUN]: Done"
  else
    cleanAntidote
  fi
}


run () {
  setupScript

  setupCluster
  configCluster

  setupTests
  # runTests
}

run
