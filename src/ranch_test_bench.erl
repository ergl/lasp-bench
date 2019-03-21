-module(ranch_test_bench).

%% Ignore xref for all lasp bench drivers
-ignore_xref([new/1, run/4, terminate/2]).

-export([new/1,
         run/4,
         terminate/2]).

-include("lasp_bench.hrl").

-define(CONN_OPTIONS, [binary, {active, false}, {packet, 2}, {nodelay, true}]).

-record(state, { sockets }).

new(_Id) ->
    Port = lasp_bench_config:get(ranch_port, 7878),
    Sockets = lists:map(fun(IP) ->
        {ok, Socket} = gen_tcp:connect(IP, Port, ?CONN_OPTIONS),
        Socket
    end, lasp_bench_config:get(ranch_ips)),
    {ok, #state{sockets = Sockets}}.

terminate(_Reason, #state{sockets=Sockets}) ->
    lists:foreach(fun gen_tcp:close/1, Sockets).


%% Ping a node at random
run(ping, _, ValueGen, State = #state{sockets = Sockets}) ->
    Socket = lists:nth(rand:uniform(length(Sockets)), Sockets),

    ok = gen_tcp:send(Socket, Payload = ValueGen()),
    case gen_tcp:recv(Socket, 0) of
        {ok, Payload} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.
