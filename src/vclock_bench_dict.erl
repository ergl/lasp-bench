-module(vclock_bench_dict).

-export([new/0,
         from_list/1,
         get_time/2,
         set_time/3,
         max/2]).

new() -> dict:new().

get_time(Key, VectorClock) ->
    case dict:find(Key, VectorClock) of
        {ok, Value} -> Value;
        error -> 0
    end.

set_time(Key, Value, VectorClock) ->
    dict:store(Key, Value, VectorClock).

from_list(List) ->
    dict:from_list(List).

max(Left, Right) ->
    dict:merge(fun(_Key, V1, V2) -> erlang:max(V1, V2) end, Left, Right).
