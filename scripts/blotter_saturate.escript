#!/usr/bin/env escript
%%! -pa ../_build/default/lib/pvc_proto/ebin -Wall

-define(NUM_KEYS, 100).
-define(VAL_SIZE, 256).
-define(TRIES, 50).

main([BinHost, BinPort, BinVersions]) ->
    Host = list_to_atom(BinHost),
    Port = (catch list_to_integer(BinPort)),
    Versions = (catch list_to_integer(BinVersions)),
    case is_integer(Port) of
        false ->
            io:fwrite(standard_error, "bad port: ~p~n", [BinPort]),
            halt(1);
        true ->
            {ok, Sock} = gen_tcp:connect(Host, Port, [binary, {active, false}, {packet, 2}]),
            Value = crypto:strong_rand_bytes(?VAL_SIZE),
            ok = saturate(Sock, ?NUM_KEYS, Value, Versions),
            gen_tcp:close(Sock)
    end;

main(_) ->
    io:fwrite("blotter_load.escript <host> <port> <versions>~n"),
    halt(1).

saturate(_, 0, _, _) ->
    ok;

saturate(Sock, N, Val, Versions) ->
    Key = integer_to_binary(N, 36),
    ok = saturate_key(Sock, Key, Val, Versions),
    saturate(Sock, N - 1, Val, Versions).

saturate_key(_, _, _, 0) ->
    ok;

saturate_key(Sock, Key, Value, N) ->
    Msg = ppb_simple_driver:read_write([Key], [{Key, Value}]),
    ok = perform_write(Sock, Msg, ?TRIES),
    saturate_key(Sock, Key, Value, N - 1).

perform_write(_, _, 0) ->
    {error, too_many_retries};

perform_write(Sock, Msg, Tries) ->
    ok = gen_tcp:send(Sock, Msg),
    {ok, BinReply} = gen_tcp:recv(Sock, 0),

    Resp = pvc_proto:decode_serv_reply(BinReply),
    case Resp of
        {error, _} ->
            perform_write(Sock, Msg, Tries - 1);
        ok ->
            ok
    end.
