-module(ranch_test_bench).

%% Ignore xref for all lasp bench drivers
-ignore_xref([new/1, run/4]).

-export([new/1,
         run/4]).

-include("lasp_bench.hrl").

-record(state, { worker_id, connections }).

new(Id) ->
    Connections = [begin
         [{Ip, PoolSize, Conns}] = ets:lookup(prehook_ets, Ip),
         {Ip, lists:nth((Id rem PoolSize) + 1, Conns)}
     end || Ip <- lasp_bench_config:get(ranch_ips)],
    {ok, #state{worker_id = Id, connections = Connections}}.

%% Ping a node at random
run(ping, _, ValueGen, State = #state{worker_id=Id, connections=Connections}) ->
    {_Ip, Connection} = lists:nth(rand:uniform(length(Connections)), Connections),
    Payload = <<Id:16, (ValueGen())/binary>>,
    case pipesock_conn:send_sync(Connection, Payload, 5000) of
        {ok, Payload} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.
