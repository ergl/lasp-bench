-module(vclock_bench_list).

-export([new/0,
         from_list/1,
         get_time/2,
         set_time/3,
         max/2]).

new() -> [].

get_time(Key, VectorClock) ->
    case lists:keyfind(Key, 1, VectorClock) of
        false -> 0;
        {Key, V} -> V
    end.

set_time(Key, Value, VectorClock) ->
    lists:keystore(Key, 1, VectorClock, {Key, Value}).

from_list(List) ->
    List.

max(First, []) -> First;
max([], Other) -> Other;
max(Left, Right) ->
    merge(Left, lists:keysort(1, Right), []).

merge([], [], Acc) -> Acc;
merge([], Right, Acc) -> lists:reverse(Acc, Right);
merge(Left, [], Acc) -> lists:reverse(Acc, Left);
merge(Left=[{Partition1,V1}=Entry1|Rest1], Right=[{Partition2,V2}=Entry2 | Rest2], Acc) ->
    if
        Partition1 < Partition2 ->
            merge(Rest1, Right,[Entry1 | Acc]);
        Partition1 > Partition2 ->
            merge(Left, Rest2, [Entry2 | Acc]);
        true ->
            merge(Rest1, Rest2, [{Partition1, erlang:max(V1, V2)} | Acc])
    end.