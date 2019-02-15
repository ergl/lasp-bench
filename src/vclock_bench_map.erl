-module(vclock_bench_map).

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

new() -> maps:new().

get_time(Key, VectorClock) ->
    maps:get(Key, VectorClock, 0).

set_time(Key, Value, VectorClock) ->
    maps:put(Key, Value, VectorClock).

from_list(List) ->
    maps:from_list(List).

max(Left, Right) when map_size(Left) == 0 -> Right;
max(Left, Right) when map_size(Right) == 0 -> Left;
max(Left, Right) ->
    maps:merge(Left, maps:map(fun(Key, Value) ->
        case maps:find(Key, Left) of
            {ok, V} -> erlang:max(V, Value);
            error -> Value
        end
   end, Right)).
