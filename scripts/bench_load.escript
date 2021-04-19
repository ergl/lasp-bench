#!/usr/bin/env escript
%%! -pa ../_build/default/lib/pvc_proto/ebin -Wall

-mode(compile).
-export([main/1]).

-define(COMMANDS, [
    {grb, true},
    {rubis, true}
]).

usage() ->
    Name = filename:basename(escript:script_name()),
    Commands = lists:foldl(
        fun({Command, NeedsArg}, Acc) ->
            CommandStr =
                case NeedsArg of
                    true -> io_lib:format("~s=arg", [Command]);
                    false -> io_lib:format("~s", [Command])
                end,
            case Acc of
                "" -> io_lib:format("< ~s", [CommandStr]);
                _ -> io_lib:format("~s | ~s", [Acc, CommandStr])
            end
        end,
        "",
        ?COMMANDS
    ),
    ok = io:fwrite(
        standard_error,
        "Usage: [-dv] ~s -f <config-file> -a <target-ip> -p <target-port> -c ~s~n",
        [Name, Commands ++ " >"]
    ).

main(Args) ->
    case parse_args(Args) of
        error ->
            usage(),
            halt(1);

        {ok, Opts} ->
            #{
                config := ConfigFile,
                command := Command,
                host := Host,
                port := Port
            } = Opts,
            {ok, Config} = file:consult(ConfigFile),
            case maps:get(command_arg, Opts, undefined) of
                undefined -> execute_command(Command, Host, Port, Config);
                Arg -> execute_command({Command, Arg}, Host, Port, Config)
            end
    end.

execute_command({grb, PropFile}, Host, Port, Config) ->
    {ok, PropConfig} = file:consult(PropFile),
    Properties = proplists:get_value(grb_config, PropConfig),
    IDLen = proplists:get_value(tcp_id_len_bits, Config),
    ok = do_load(Host, Port, ppb_grb_driver, Properties, IDLen);
execute_command({rubis, PropFile}, Host, Port, Config) ->
    {ok, PropConfig} = file:consult(PropFile),
    Properties = proplists:get_value(rubis_config, PropConfig),
    IDLen = proplists:get_value(tcp_id_len_bits, Config),
    ok = do_load(Host, Port, ppb_rubis_driver, Properties, IDLen).

do_load(Host, Port, DriverModule, Properties, IDLen) ->
    case
        gen_tcp:connect(Host, Port, [binary, {active, false}, {packet, 4}, {nodelay, true}])
    of
        {error, Reason} ->
            {error, Reason};
        {ok, Socket} ->
            Msg = DriverModule:preload(Properties),
            ok = gen_tcp:send(Socket, <<0:IDLen, Msg/binary>>),
            Reply = case gen_tcp:recv(Socket, 0) of
                {error, Reason} ->
                    {error, Reason};
                {ok, <<0:IDLen, RawReply/binary>>} ->
                    ok = pvc_proto:decode_serv_reply(RawReply)
            end,
            ok = gen_tcp:close(Socket),
            Reply
    end.

parse_args([]) ->
    error;
parse_args(Args) ->
    case parse_args(Args, #{}) of
        {ok, Opts} -> required(Opts);
        Err -> Err
    end.

parse_args([], Acc) ->
    {ok, Acc};
parse_args([[$- | Flag] | Args], Acc) ->
    case Flag of
        [$a] ->
            parse_flag(Args, fun(Arg) -> Acc#{host => list_to_atom(Arg)} end);
        [$p] ->
            parse_flag(Args, fun(Arg) -> Acc#{port => list_to_integer(Arg)} end);
        [$f] ->
            parse_flag(Args, fun(Arg) -> Acc#{config => Arg} end);
        [$c] ->
            parse_flag(Args, fun(Arg) -> parse_command(Arg, Acc) end);
        [$h] ->
            usage(),
            halt(0);
        _ ->
            error
    end;
parse_args(_, _) ->
    error.

parse_flag(Args, Fun) ->
    case Args of
        [FlagArg | Rest] -> parse_args(Rest, Fun(FlagArg));
        _ -> error
    end.

parse_command(Arg, Acc) ->
    case string:str(Arg, "=") of
        0 ->
            Acc#{command => list_to_atom(Arg)};
        _ ->
            % crash on malformed command for now
            [Command, CommandArg | _Ignore] = string:tokens(Arg, "="),
            Acc#{command_arg => CommandArg, command => list_to_atom(Command)}
    end.

required(Opts) ->
    Required = [config, command, host, port],
    Valid = lists:all(fun(F) -> maps:is_key(F, Opts) end, Required),
    case Valid of
        false ->
            error;
        true ->
            case maps:is_key(command, Opts) of
                true -> check_command(Opts);
                false -> {ok, Opts}
            end
    end.

check_command(Opts = #{command := Command}) ->
    case lists:member({Command, maps:is_key(command_arg, Opts)}, ?COMMANDS) of
        true ->
            {ok, Opts};
        false ->
            error
    end.
