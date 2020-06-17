-module(hook_pipesock).

-ignore_xref([start/1, stop/0]).

%% API
-export([start/1,
         stop/0]).

start(HookOpts) ->
    logger:info("Running pre hook, loading pipesock with args ~p", [HookOpts]),
    ok = application:ensure_started(pipesock),
    _ = ets:new(prehook_ets, [set, named_table, protected]),

    {pool_size, PoolSize} = lists:keyfind(pool_size, 1, HookOpts),
    Port = lasp_bench_config:get(ranch_port),
    IPs = lasp_bench_config:get(ranch_ips),

    %% Create a pool of `PoolSize` connections for each server node in the cluster
    %% These pools will be partitioned uniformly among worker threads
    %%
    %% Store in the ETS a mapping Ip -> {PoolSize, Conns=[connection()]}
    %% A worker will, for each Ip, pick Conns[random(0, PoolSize)]

    ok = lists:foreach(fun(Ip) ->
        Connections = spawn_pool(Ip, Port, PoolSize),
        true = ets:insert(prehook_ets, {Ip, PoolSize, Connections}),
        ok
    end, IPs).

spawn_pool(Ip, Port, Size) ->
    spawn_pool(Ip, Port, Size, []).

spawn_pool(_, _, 0, Acc) ->
    Acc;

spawn_pool(Ip, Port, N, Acc) ->
    {ok, Connection} = pipesock_conn:open(Ip, Port),
    spawn_pool(Ip, Port, N - 1, [Connection | Acc]).

stop() ->
    logger:info("Running post hook, unloading pipesock"),
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
