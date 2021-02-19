-module(lasp_bench_driver_null).
-include("lasp_bench.hrl").

-ignore_xref([new/1, run/4, terminate/2]).
-export([new/1, run/4, terminate/2]).

new(_) -> {ok, ok}.
run(_, _, _, ok) -> {ok, ok}.
terminate(_, ok) -> ok.
