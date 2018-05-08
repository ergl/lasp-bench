#!/usr/bin/env escript
%%! -pa ../_build/default/lib/jsx/ebin ../_build/default/lib/rubis_proto/ebin -Wall

-define(NUM_KEYS, 10000000).
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
            ok = do_load(Sock),
            gen_tcp:close(Sock)
    end;

main(_) ->
    io:fwrite("blotter_load.escript <host> <port>~n"),
    halt(1).

do_load(Socket) ->
    pmap(fun(N) ->
        do_load(Socket, N)
    end, lists:seq(1, ?NUM_KEYS)).

do_load(Socket, N) ->
    Key = integer_to_binary(N, 36),
    Value = crypto:strong_rand_bytes(?VAL_SIZE),
    Msg = rpb_simple_driver:read_write([], [{Key, Value}]),
    ok = gen_tcp:send(Socket, Msg),
    {ok, BinReply} = gen_tcp:recv(Socket, 0),
    rubis_proto:decode_serv_reply(BinReply).

pmap(_, []) ->
    [];

pmap(Fun, List) when is_function(Fun, 1) andalso is_list(List) ->
    S = self(),
    Ref = make_ref(),
    Pids = lists:map(fun(Elt) ->
        spawn(fun() -> do_map(S, Ref, Fun, Elt) end)
    end, List),
    gather(Pids, Ref).

do_map(Caller, TaskID, Fun, Elt) ->
    Caller ! {self(), TaskID, catch(Fun(Elt))}.

gather([W | R], TaskID) ->
    receive
        {W, TaskID, Val}->
            [Val | gather(R, TaskID)]
    end;

gather([], _) ->
    [].
