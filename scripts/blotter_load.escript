#!/usr/bin/env escript
%%! -pa ../_build/default/lib/pvc_proto/ebin -Wall

-define(CONN_OPTS, [binary, {active, false}, {packet, 4}]).
-define(VAL_SIZE, 1024).

main([BinHost, BinPort]) ->
    {Host, Port} = parse_args(BinHost, BinPort),
    case gen_tcp:connect(Host, Port, ?CONN_OPTS) of
        {error, Reason} ->
            io:fwrite(standard_error, "connect error: ~p~n", [Reason]);
        {ok, Socket} ->
            ok = load_internal(Socket),
            gen_tcp:close(Socket)
    end;

main(_) ->
    io:fwrite("blotter_load.escript <host> <port>~n"),
    halt(1).

parse_args(BinHost, BinPort) ->
    Host = list_to_atom(BinHost),
    Port = (catch list_to_integer(BinPort)),
    case is_integer(Port) of
        false ->
            io:fwrite(standard_error, "bad port: ~p~n", [BinPort]),
            halt(1);
        true ->
            {Host, Port}
    end.

load_internal(Socket) ->
    ok = gen_tcp:send(Socket, <<0:16, (ppb_simple_driver:load(?VAL_SIZE))/binary>>),
    case gen_tcp:recv(Socket, 0) of
        {error, Reason} ->
            {error, Reason};
        {ok, <<0:16, RawReply/binary>>} ->
            ok = pvc_proto:decode_serv_reply(RawReply)
    end.
