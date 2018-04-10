#!/usr/bin/env bash

# The private / public key pair used for this experiment
# Recommended to use password-less keys
PRKFILE=~/.ssh/benchmark
PBKFILE=~/.ssh/benchmark.pub

# The url of the k3 env to deploy on the nodes
K3_IMAGE=/home/$(whoami)/public/antidote-images/latest/antidote.env

# Different g5k sites to run the benchmark
SITES=( "nancy" )

# Reserve sites and nodes through oargridsub
RESERVE_SITES=true

# Boot the machines and load the os image.
DEPLOY_IMAGE=true

# Provision the nodes with Antidote / lasp_bench
PROVISION_IMAGES=true

# Download and compile antidote and lasp_bench from scratch
CLEAN_RUN=true

# Number of nodes running Antidote per site
# Saying SITES=( "nancy" "rennes" )
# and ANTIDOTE_NODES=2 would create 4 nodes,
# two in nancy, two in rennes
ANTIDOTE_NODES=1

# Number of nodes running lasp_bench per site
BENCH_NODES=1

# Number of instances of lasp_bench to run per node
BENCH_INSTANCES=1

# git repository of the antidote code (useful to test forks)
ANTIDOTE_URL="https://github.com/ergl/antidote.git"

# git branch of antidote to run the experiment on
ANTIDOTE_BRANCH="pvc-rubis-g5k"

# git repository of the basho_bench code (useful to test forks)
BENCH_URL="https://github.com/ergl/lasp_bench.git"

# git branch of Basho Bench to use
BENCH_BRANCH="antidote-rubis"

# Name of the benchmark configuration file to use
BENCH_FILE="rubis_default.config"
