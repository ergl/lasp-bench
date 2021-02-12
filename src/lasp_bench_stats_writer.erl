-module(lasp_bench_stats_writer).

-callback new(Ops :: [{term(), term()}], Measurements :: [{term(), term()}]) -> State :: term().

-callback process_summary(State :: term(),
                          ElapsedUs :: non_neg_integer(), Window :: non_neg_integer(),
                          Oks :: non_neg_integer(), Errors :: non_neg_integer()) -> ok.

-callback report_latency(State :: term(), ElapsedUs :: non_neg_integer(), Window :: non_neg_integer(),
                         Op :: {term(), term()}, Stats :: proplists:proplist(), Errors :: non_neg_integer(),
                         Units :: non_neg_integer()) -> ok.

-callback report_error(State :: term(), term(), integer() | float()) -> ok.

-callback terminate(State :: term()) -> ok.
