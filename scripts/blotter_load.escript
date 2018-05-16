#!/usr/bin/env escript
%%! -pa ../_build/default/lib/jsx/ebin ../_build/default/lib/rubis_proto/ebin -Wall

-define(NUM_KEYS, 10000).
-define(VAL_SIZE, 256).

main([BinHost, BinPort]) ->
    Host = list_to_atom(BinHost),
    Port = (catch list_to_integer(BinPort)),
    case is_integer(Port) of
        false ->
            io:fwrite(standard_error, "bad port: ~p~n", [BinPort]),
            halt(1);
        true ->
            {ok, Sock} = gen_tcp:connect(Host, Port, [binary, {active, false}, {packet, 2}]),
            Msg = rpb_simple_driver:load(?NUM_KEYS, ?VAL_SIZE),
            ok = gen_tcp:send(Sock, Msg),
            {ok, BinReply} = gen_tcp:recv(Sock, 0),
            ok = rubis_proto:decode_serv_reply(BinReply),
            gen_tcp:close(Sock)
    end;

main(_) ->
    io:fwrite("blotter_load.escript <host> <port>~n"),
    halt(1).
