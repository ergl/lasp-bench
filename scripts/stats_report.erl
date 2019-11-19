#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable -name stats_report@127.0.0.1 -setcookie antidote

-mode(compile).

-export([main/1]).

main([NodeNameListConfig]) ->
    validate(parse_node_config(NodeNameListConfig));
main(_) ->
    usage(),
    halt(1).

%% @doc Parse node names from config file
%%
%% The config file is the same as the cluster definition.
-spec parse_node_config(ConfigFilePath :: string()) -> {ok, [atom()]} | error.
parse_node_config(ConfigFilePath) ->
    case file:consult(ConfigFilePath) of
        {ok, Terms} ->
            {clusters, ClusterMap} = lists:keyfind(clusters, 1, Terms),
            NodeNames = lists:usort(lists:flatten([N || #{servers := N} <- maps:values(ClusterMap)])),
            {ok, build_erlang_node_names(NodeNames)};
        _ ->
            error
    end.

-spec build_erlang_node_names([atom()]) -> [atom()].
build_erlang_node_names(NodeNames) ->
    [begin
         {ok, Addr} = inet:getaddr(Node, inet),
         IPString = inet:ntoa(Addr),
         list_to_atom("antidote@" ++ IPString)
     end || Node <- NodeNames].


%% @doc Validate parsing, then proceed
validate(error) ->
    usage();
validate({ok, Nodes}) when is_list(Nodes) ->
    [ erlang:set_cookie(N, antidote) || N <- Nodes],
    {Results, BadNodes} = rpc:multicall(Nodes, antidote_stats_collector, report_stats, [], infinity),

    BadNodes =:= [] orelse begin
        io:fwrite(standard_error, "report_version_misses failed on ~p~n", [BadNodes]),
        true
    end,

    Averages = calc(lists:flatten(Results)),
    io:fwrite(standard_io, "~p~n", [Averages]),
    halt(0).

calc([]) -> #{clog => 0, vlog => 0, tries => 0, fix_vlog => 0};
calc(Results) ->
    {Len, CT, VT, TT, VCT} = lists:foldl(fun(Map, {N, AccClog, AccVlog, AccTries, AccVC}) ->
        #{clog_misses := C, vlog_misses := V, not_ready_tries := T} = Map,
        VC = maps:get(fix_vc_misses, Map, 0),
        {N + 1, AccClog + C, AccVlog + V, T + AccTries, VC + AccVC}
    end, {0, 0, 0, 0, 0}, Results),
    #{clog => CT / Len, vlog => VT / Len, tries => TT / Len, fix_vlog => VCT / Len}.

-spec usage() -> no_return().
usage() ->
    Name = filename:basename(escript:script_name()),
    io:fwrite(standard_error, "~s <config_file>~n", [Name]),
    halt(1).
