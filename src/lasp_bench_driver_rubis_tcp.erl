-module(lasp_bench_driver_rubis_tcp).

-export([new/1,
         run/4]).

-include("lasp_bench.hrl").

-record(state, {
    %% Antidote connection to the remote node
    worker_id,
    target_ip,
    target_port,
    socket
}).

new(Id) ->
    Ip = lasp_bench_config:get(pb_ip),
    Port = lasp_bench_config:get(pb_port),
    Options = [binary, {active, false}, {packet, 2}],
    {ok, Sock} = gen_tcp:connect(Ip, Port, Options),
    {ok, #state{worker_id=Id,
                target_ip=Ip,
                socket=Sock,
                target_port=Port}}.

run(send, _, ValueGen, State = #state{socket=Sock}) ->
    NewVal = ValueGen(),
    gen_tcp:send(Sock, NewVal),
    RecVal = gen_tcp:recv(Sock, 0),
    case RecVal of
        {ok, NewVal} ->
            {ok, State};
        {ok, _Other} ->
            {error, no_match, State};
        {error, Reason} ->
            {error, Reason, State}
    end.
