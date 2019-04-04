-module(pipesock_hook).

%% API
-export([start/1,
         stop/1]).

start(PoolSize) ->
    lager:info("Running pre hook, loading pipesock with args ~p", [PoolSize]),
    ok = application:ensure_started(pipesock),
    _ = ets:new(prehook_ets, [set, named_table, protected]),

    Port = lasp_bench_config:get(ranch_port),
    IPs = lasp_bench_config:get(ranch_ips),

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
    PID = case pipesock_worker_sup:start_connection(Ip, Port, N) of
      {ok, ServerPid} ->
          ServerPid;
      {error, {already_started, ServerPid}} ->
          ServerPid
    end,
    spawn_pool(Ip, Port, N - 1, [PID | Acc]).

stop([]) ->
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
    lists:foreach(fun pipesock_worker:close/1, Conns).
