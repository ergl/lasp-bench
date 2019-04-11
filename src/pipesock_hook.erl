-module(pipesock_hook).

%% API
-export([start/1,
         stop/0]).

start(KeyList) ->
    lager:info("Running pre hook, loading pipesock with args ~p", [KeyList]),
    ok = application:ensure_started(pipesock),
    _ = ets:new(prehook_ets, [set, named_table, protected]),

    {value, {pool_size, PoolSize}, ConnOpts} = lists:keytake(pool_size, 1, KeyList),
    Port = lasp_bench_config:get(ranch_port),
    IPs = lasp_bench_config:get(ranch_ips),
    ConnMap = maps:from_list(ConnOpts),

    %% Create a pool of `PoolSize` connections for each server node in the cluster
    %% These pools will be partitioned uniformly among worker threads
    %%
    %% Store in the ETS a mapping Ip -> {PoolSize, Conns=[connection()]}
    %% A worker will, for each Ip, pick Conns[random(0, PoolSize)]

    ok = lists:foreach(fun(Ip) ->
        Connections = spawn_pool(Ip, Port, ConnMap, PoolSize),
        true = ets:insert(prehook_ets, {Ip, PoolSize, Connections}),
        ok
    end, IPs).

spawn_pool(Ip, Port, Options, Size) ->
    spawn_pool(Ip, Port, Options, Size, []).

spawn_pool(_, _, _Options, 0, Acc) ->
    Acc;

spawn_pool(Ip, Port, Options, N, Acc) ->
    {ok, Connection} = pipesock_conn:open(Ip, Port, Options),
    spawn_pool(Ip, Port, Options, N - 1, [Connection | Acc]).

stop() ->
    lager:info("Running post hook, unloading pipesock"),
    IPs = lasp_bench_config:get(ranch_ips),
    lists:foreach(fun(Ip) ->
        ok = teardown_pool(prehook_ets, Ip)
    end, IPs),
    true = ets:delete(prehook_ets),
    application:stop(pipesock),
    ok.

teardown_pool(ETS, Ip) ->
    [{Ip, _, Conns}] = ets:take(ETS, Ip),
    lists:foreach(fun pipesock_conn:close/1, Conns).
