#!/usr/bin/env escript
%%! ../_build/default/lib/pvc_proto/ebin -Wall

-mode(compile).
-export([main/1]).

main([Host, Port, ConfigFile]) ->
    case file:consult(ConfigFile) of
        {error, Reason} ->
            io:fwrite(standard_error, "config file: ~p~n", [Reason]),
            halt(1);
        {ok, Terms} ->
            Properties = proplists:get_value(rubis_config, Terms),
            ok = do_load(Host, Port, Properties)
    end;

main(_) ->
    io:fwrite("rubis_load.escript <host> <port> <config-file>~n"),
    halt(1).

do_load(BinHost, BinPort, Properties) ->
    Host = list_to_atom(BinHost),
    Port = (catch list_to_integer(BinPort)),
    case is_integer(Port) of
        false ->
            {error, bad_port};
        true ->
            case gen_tcp:connect(Host, Port, [binary, {active, false}, {packet, 4}, {nodelay, true}]) of
                {error, Reason} ->
                    {error, Reason};
                {ok, Socket} ->
                    Msg = ppb_rubis_driver:preload(Properties),
                    ok = gen_tcp:send(Socket, <<0:16, Msg/binary>>),
                    Reply = case gen_tcp:recv(Socket, 0) of
                        {error, Reason} ->
                            {error, Reason};
                        {ok, <<0:16, RawReply/binary>>} ->
                            ok = pvc_proto:decode_serv_reply(RawReply)
                    end,
                    ok = gen_tcp:close(Socket),
                    Reply
            end
    end.
