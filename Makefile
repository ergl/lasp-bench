REPO            ?= lasp_bench

PKG_REVISION    ?= $(shell git describe --tags)
PKG_VERSION     ?= $(shell git describe --tags | tr - .)
PKG_ID           = basho-bench-$(PKG_VERSION)
PKG_BUILD        = 1
BASE_DIR         = $(shell pwd)
ERLANG_BIN       = $(shell dirname $(shell which erl))
REBAR           ?= $(BASE_DIR)/rebar3
OVERLAY_VARS    ?=

RUBIS_IP ?= "127.0.0.1"
RUBIS_PORT ?= "7878"

RUBIS_TABLE_CONFIG ?= "examples/rubis.config"

REBAR := ./rebar3

all: compile
	${REBAR} escriptize

compile:
	(${REBAR} compile)

clean:
	${REBAR} clean

load: compile
	./rubis-load.sh $(RUBIS_IP) $(RUBIS_PORT)

generate:
	./rubis-generate.sh $(RUBIS_IP) $(RUBIS_PORT) $(RUBIS_TABLE_CONFIG)

bench:
	./_build/default/bin/lasp_bench $(RUBIS_TABLE_CONFIG)

results:
	Rscript --vanilla priv/summary.r -x 2600 -y 2000 -i tests/current


TARGETS := $(shell ls tests/ | grep -v current)
JOBS := $(addprefix job,${TARGETS})
.PHONY: all_results ${JOBS}

all_results: ${JOBS} ; echo "$@ successfully generated."
${JOBS}: job%: ; Rscript --vanilla priv/summary.r -i tests/$*
