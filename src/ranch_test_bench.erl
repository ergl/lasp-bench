-module(ranch_test_bench).

%% Ignore xref for all lasp bench drivers
-ignore_xref([new/1, run/4]).

-export([new/1,
         run/4]).

-include("lasp_bench.hrl").

-record(state, { worker_id, connections }).

new(Id) ->
    Connections = lists:map(fun(IP) ->
        [{IP, PoolSize, Conns}] = ets:lookup(prehook_ets, IP),
        lists:nth((Id rem PoolSize) + 1, Conns)
    end, lasp_bench_config:get(ranch_ips)),
    {ok, #state{worker_id = Id, connections = Connections}}.

%% Ping a node at random
run(ping, _, ValueGen, State = #state{worker_id=Id, connections=Connections}) ->
    Connection = lists:nth(rand:uniform(length(Connections)), Connections),
    %% FIXME(borja): Take into account id exhaustion
    Payload = <<Id:8, (ValueGen())/binary>>,
    case pipesock_worker:send_sync(Connection, Payload, 5000) of
        {ok, Payload} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.
