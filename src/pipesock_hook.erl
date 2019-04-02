-module(pipesock_hook).

%% API
-export([start/0,
         stop/0]).

start() ->
    lager:info("Running pre hook, loading pipesock"),
    ok = application:ensure_started(pipesock),
    _ = ets:new(prehook_ets, [set, named_table, protected]),

    Port = lasp_bench_config:get(ranch_port),
    IPs = lasp_bench_config:get(ranch_ips),
    %% TODO(borja): Either spawn multiple connections or create pool
    lists:foreach(fun(IP) ->
        {ok, ServerPid} = pipesock_worker_sup:start_connection(IP, Port),
        true = ets:insert(prehook_ets, {IP, ServerPid})
    end, IPs),
    ok.

stop() ->
    lager:info("Running post hook, unloading pipesock"),
    IPs = lasp_bench_config:get(ranch_ips),
    lists:foreach(fun(IP) ->
        [{IP, ConnectionPID}] = ets:take(prehook_ets, IP),
        ok = pipesock_worker:close(ConnectionPID)
    end, IPs),
    true = ets:delete(prehook_ets),
    application:stop(pipesock),
    ok.
