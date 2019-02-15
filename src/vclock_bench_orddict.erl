-module(vclock_bench_orddict).

%% Called in Module:function fashion
-ignore_xref([new/0,
              from_list/1,
              get_time/2,
              set_time/3,
              max/2]).

-export([new/0,
         from_list/1,
         get_time/2,
         set_time/3,
         max/2]).

new() -> orddict:new().

get_time(Key, VectorClock) ->
    case orddict:find(Key, VectorClock) of
        {ok, Value} -> Value;
        error -> 0
    end.

set_time(Key, Value, VectorClock) ->
    orddict:store(Key, Value, VectorClock).

from_list(List) ->
    orddict:from_list(List).

max(Left, Right) ->
    orddict:merge(fun(_Key, V1, V2) -> erlang:max(V1, V2) end, Left, Right).
