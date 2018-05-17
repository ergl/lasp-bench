#!/usr/bin/env escript
%%! -pa ../_build/default/lib/jsx/ebin -Wall

-define(CONFIG_MODE, max).
-define(CONFIG_DURATION, 5).
-define(CONFIG_INTERVAL_SECONDS, 5).
-define(CONFIG_CLIENTS, 50).

-define(CONFIG_HEADER,
    fun() ->
        io_lib:fwrite(
            "{mode,~p}.~n{duration,~p}.~n{report_interval,~p}.~n{concurrent,~p}.~n",
            [?CONFIG_MODE, ?CONFIG_DURATION, ?CONFIG_INTERVAL_SECONDS, ?CONFIG_CLIENTS]
        )
    end).

-define(TRANSITION_TABLE,
    fun() ->
        {ok, Path} = file:get_cwd(),
        Path ++ "/tables/default_transitions.txt"
    end).

-define(CONFIG_DRIVER, "{driver,rubis_bench}.~n").

main([Host, Port, LoadOutput]) ->
    TablePath = ?TRANSITION_TABLE(),
    {ok, LoadInfo} = load_from_json(LoadOutput),
    write_config_file({table, TablePath}, Host, Port, LoadInfo);

main([Host, Port, LoadOutput, DefaultOps]) ->
    {ok, LoadInfo} = load_from_json(LoadOutput),
    {ok, Ops} = load_ops(DefaultOps),
    write_config_file({ops, Ops}, Host, Port, LoadInfo);

main(_) ->
    io:fwrite("rubis_generate_bench_config.escript <rubis-ip> <rubis-port> <load-file>~n"),
    halt(1).

load_from_json(JsonFile) ->
    load_from_json(JsonFile, [{labels, atom}, return_maps]).

load_from_json(JsonFile, Opts) ->
    case file:read_file(JsonFile) of
        {error, Reason} ->
            {error, Reason};

        {ok, Contents} ->
            case jsx:is_json(Contents) of
                false ->
                    {error, no_json};
                true ->
                    Json = jsx:decode(Contents, Opts),
                    {ok, Json}
            end
    end.

load_ops(DefaultOps) ->
    case load_from_json(DefaultOps) of
        {error, Reason} ->
            {error, Reason};

        {ok, Ops} ->
            {ok, lists:map(fun(T) -> binary_to_atom(T, utf8) end, Ops)}
    end.

write_config_file(Mode, Host, Port, LoadInfo) ->
    io:fwrite(?CONFIG_HEADER(), []),
    io:fwrite(?CONFIG_DRIVER),
    case Mode of
        {table, TablePath} ->
            io:fwrite("{bench_mode,{table,~p}}.~n", [TablePath]);
        {ops, Ops} ->
            io:fwrite("{bench_mode,{transition,~p}}.~n", [Ops])
    end,
    io:fwrite("{operations,[{perform_operation, 1}]}.~n"),
    io:fwrite("{rubis_ip,~p}.~n", [list_to_atom(Host)]),
    io:fwrite("{rubis_port,~p}.~n", [list_to_integer(Port)]),
    io:fwrite("{region_ids, ~n~p}.~n",[maps:get(region_ids, LoadInfo)]),
    io:fwrite("{category_ids, ~n~p}.~n",[maps:get(category_ids, LoadInfo)]),
    io:fwrite("{user_ids, ~n~p}.~n",[maps:get(user_ids, LoadInfo)]),
    io:fwrite("{user_names, ~n~p}.~n",[maps:get(user_names, LoadInfo)]),
    io:fwrite("{item_ids, ~n~p}.~n",[maps:get(item_ids, LoadInfo)]),
    ok.
