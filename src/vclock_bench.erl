
-module(vclock_bench).

-export([new/1,
         run/4]).

-include("lasp_bench.hrl").

-record(state, {
    target :: module(),
    size :: non_neg_integer(),
    partitions :: [non_neg_integer()],
    full :: term(),
    half :: term(),
    empty :: term()
}).

new(_Id) ->
    Module = choose_module(lasp_bench_config:get(impl)),
    Size = lasp_bench_config:get(partitions),
    io:format("Chose module ~p with size ~p~n", [Module, Size]),
    Partitions = gen_partitions(Size),
    {ok, #state{target=Module,
                size=Size,
                partitions=Partitions,
                full=generate(Module, Partitions),
                half=generate(Module, Partitions, Size div 2),
                empty=Module:new()}}.

choose_module(dict) -> vclock_bench_dict;
choose_module(orddict) -> vclock_bench_orddict;
choose_module(list) -> vclock_bench_list;
choose_module(map) -> vclock_bench_map.

generate(Module, Partitions) ->
    Module:from_list(to_entries(Partitions)).

generate(Module, Partitions, Size) ->
    Module:from_list(to_entries(Partitions, Size)).

to_entries(Partitions) ->
    lists:map(fun(Entry) -> {Entry, rand:uniform(499) + 1} end, Partitions).

to_entries(Partitions, Size) ->
    {First, _} = lists:split(Size, Partitions),
    to_entries(First).

gen_partitions(Size) ->
    lists:map(fun(K) ->
        <<PartitionId:160/big-unsigned-integer>> = crypto:hash(sha, integer_to_binary(K,36)),
        PartitionId
    end, lists:seq(1, Size)).

random_partition(Partitions, Size) ->
    Nth = rand:uniform(Size),
    lists:nth(Nth, Partitions).

run(lookup_full, _, _, State = #state{partitions=Partitions,size=Size,full=Full,target=Module}) ->
    _V = Module:get_time(random_partition(Partitions, Size), Full),
    {ok, State};

run(lookup_half, _, _, State = #state{partitions=Partitions,size=Size,half=Half,target=Module}) ->
    _V = Module:get_time(random_partition(Partitions, Size), Half),
    {ok, State};

run(lookup_empty, _, _, State = #state{partitions=Partitions,size=Size,empty=Empty,target=Module}) ->
    _V = Module:get_time(random_partition(Partitions, Size), Empty),
    {ok, State};

run(set_full, K, _, State = #state{partitions=Partitions,size=Size,full=Full,target=Module}) ->
    _V = Module:set_time(random_partition(Partitions, Size), K(), Full),
    {ok, State};

run(set_half, K, _, State = #state{partitions=Partitions,size=Size,half=Half,target=Module}) ->
    _V = Module:set_time(random_partition(Partitions, Size), K(), Half),
    {ok, State};

run(set_empty, K, _, State = #state{partitions=Partitions,size=Size,empty=Empty,target=Module}) ->
    _V = Module:set_time(random_partition(Partitions, Size), K(), Empty),
    {ok, State};

run(max_f, _, _, State = #state{full=Full,target=Module}) ->
    _ = Module:max(Full, Full),
    {ok, State};

run(max_h, _, _, State = #state{half=Half,target=Module}) ->
    _ = Module:max(Half, Half),
    {ok, State};

run(max_e, _, _, State = #state{empty=Empty,target=Module}) ->
    _ = Module:max(Empty, Empty),
    {ok, State};

run(max_fh, _, _, State = #state{full=Full,half=Half,target=Module}) ->
    _ = Module:max(Full, Half),
    {ok, State};

run(max_fe, _, _, State = #state{full=Full,empty=Empty,target=Module}) ->
    _ = Module:max(Full, Empty),
    {ok, State};

run(max_he, _, _, State = #state{half=Half,empty=Empty,target=Module}) ->
    _ = Module:max(Half, Empty),
    {ok, State}.
