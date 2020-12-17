REPO            ?= lasp_bench
BASEDIR = $(shell pwd)
PKG_REVISION    ?= $(shell git describe --tags)
PKG_VERSION     ?= $(shell git describe --tags | tr - .)
PKG_ID           = basho-bench-$(PKG_VERSION)
PKG_BUILD        = 1
BASE_DIR         = $(shell pwd)
ERLANG_BIN       = $(shell dirname $(shell which erl))
REBAR           ?= $(BASE_DIR)/rebar3
OVERLAY_VARS    ?=
BASIC_PROFILE = default

REBAR = $(BASEDIR)/rebar3

all: compile
	$(REBAR) as $(BASIC_PROFILE) escriptize

compile:
	$(REBAR) as $(BASIC_PROFILE) compile

clean:
	$(REBAR) clean

results:
	Rscript --vanilla priv/summary.r -x 1500 -y 2500 -i tests/current

clean_results:
	rm -rf tests/*
	touch tests/.keepme

TARGETS := $(shell ls tests/ | grep -v current)
JOBS := $(addprefix job,${TARGETS})
.PHONY: all_results ${JOBS}

all_results: ${JOBS} ; echo "$@ successfully generated."
${JOBS}: job%: ; Rscript --vanilla priv/summary.r -i tests/$*
