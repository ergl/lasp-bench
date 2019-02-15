#!/usr/bin/env escript
%%! -pa ../_build/default/lib/pvc_proto/ebin -Wall

-define(NUM_KEYS, 100000).
-define(VAL_SIZE, 1024).

main([BinHost, BinPort]) ->
    Host = list_to_atom(BinHost),
    Port = (catch list_to_integer(BinPort)),
    case is_integer(Port) of
        false ->
            io:fwrite(standard_error, "bad port: ~p~n", [BinPort]),
            halt(1);
        true ->
            {ok, Sock} = gen_tcp:connect(Host, Port, [binary, {active, false}, {packet, 2}]),
            Msg = ppb_simple_driver:load(?NUM_KEYS, ?VAL_SIZE),
            ok = gen_tcp:send(Sock, Msg),
            {ok, BinReply} = gen_tcp:recv(Sock, 0),
            ok = pvc_proto:decode_serv_reply(BinReply),
            gen_tcp:close(Sock)
    end;

main(_) ->
    io:fwrite("blotter_load.escript <host> <port>~n"),
    halt(1).
