#!/usr/bin/env escript

-define(IFACE, enp1s0).
-define(ROOT_CMD_STR, "sudo tc qdisc add dev ~p root handle 1: prio bands ~b priomap ~s").
-define(NETEM_CMD_STR, "sudo tc qdisc add dev ~p parent 1:~b handle ~b: netem delay ~bms").
-define(FILTER_CMD_STR, "sudo tc filter add dev ~p parent 1:0 protocol ip prio 1 u32 match ip dst ~s/32 flowid 1:~b").

%% TODO(borja): Make sure to remove the current cluster definition from the config
%% Config file is something like:
%% {latencies, [{<latency>, [<cluster_name>. ....]}, ...]}
%% {clusters, {<cluster_name>, [<node_name>, ...]}}
%%
%% Example configuration file:
%% ```cluster.config:
%% {latencies, [
%%      {10, [cluster_a]},
%%      {30, [cluster_c]},
%%      {50, [cluster_d]},
%%      {200, [cluster_b]}
%% ]}.
%%
%% {clusters, [
%%      {cluster_a, ['apollo-1-1.imdea', 'apollo-2-1.imdea']},
%%      {cluster_b, ['apollo-1-2.imdea', 'apollo-2-2.imdea']},
%%      {cluster_c, ['apollo-1-3.imdea', 'apollo-2-3.imdea']},
%%      {cluster_d, ['apollo-1-4.imdea', 'apollo-2-4.imdea']}
%% ]}.
%% ```

main([ConfigFile]) ->
    ok = run(ConfigFile);

main(["-d", ConfigFile]) ->
    erlang:put(dry_run, true),
    ok = run(ConfigFile);

main(_) ->
    usage(),
    halt(1).

run(ConfigFile) ->
    {ok, Config} = file:consult(ConfigFile),
    true = reset_tc_rules(),
    ok = build_tc_rules(Config).

-spec usage() -> ok.
usage() ->
    Name = filename:basename(escript:script_name()),
    ok = io:fwrite(standard_error, "~s <config_file>~n", [Name]).

-spec reset_tc_rules() -> boolean().
reset_tc_rules() ->
    already_default() orelse begin
        Cmd = io_lib:format("sudo tc qdisc del dev ~p root", [?IFACE]),
        _ = safe_cmd(Cmd),
        true
    end.

-spec already_default() -> boolean().
already_default() ->
    TCRules = binary:split(list_to_binary(safe_cmd("tc qdisc ls")), [<<"\n">>, <<" \n">>], [global, trim_all]),
    DefaultRule = list_to_binary(io_lib:format("qdisc mq 0: dev ~p root", [?IFACE])),
    lists:member(DefaultRule, TCRules).

build_tc_rules(Config) ->
    {ok, Latencies} = config_get(latencies, Config),
    Lanes = length(Latencies),
    ok = setup_tc_root(Lanes),

    {ok, Clusters} = config_get(clusters, Config),
    ok = setup_tc_qdiscs(Latencies, Clusters),
    ok.

setup_tc_qdiscs(Latencies, Clusters) ->
    %% Each qdisc must have the parent number, and a handle
    %% Handles must be globally unique, so pick a multiple of the parent minor number
    %% Parent 1:1 -> handle 10:
    %% Parent 1:2 -> handle 20:
    %% We made sure beforehand that there are enough parent bands to do this
    _ = lists:foldl(fun({Latency, ClusterMems}, ParentMinor) ->
        ok = build_tc_rules_for_cluster(ParentMinor, Latency, ClusterMems, Clusters),
        ParentMinor + 1
    end, 1, Latencies),
    ok.

build_tc_rules_for_cluster(ParentId, Latency, ClusterNames, ClustersDefs) ->
    HandleId = ParentId * 10,
    RuleCmd = io_lib:format(?NETEM_CMD_STR, [?IFACE, ParentId, HandleId, Latency]),
    _ = safe_cmd(RuleCmd),
    NodeIPs = lists:flatten([get_cluster_ips(Name, ClustersDefs) || Name <- ClusterNames]),
    lists:foreach(fun(NodeIP) ->
        FilterCmd = io_lib:format(?FILTER_CMD_STR, [?IFACE, NodeIP, ParentId]),
        _ = safe_cmd(FilterCmd)
    end, NodeIPs),
    ok.

get_cluster_ips(ClusterName, Defs) ->
    {ok, Nodes} = config_get(ClusterName, Defs),
    lists:map(fun(N) -> {ok, Addr} = inet:getaddr(N, inet), list_to_atom(inet:ntoa(Addr)) end, Nodes).

%% Set up root qdisc, of type prio, with N+1 bands, and a priomap redirecting all
%% traffic to the highes band (lowest priority). This way, only packets matching
%% filters will go through netem qdiscs.
-spec setup_tc_root(non_neg_integer()) -> ok.
setup_tc_root(Lanes) ->
    RootCmd = io_lib:format(?ROOT_CMD_STR, [?IFACE, Lanes + 1, priomap(Lanes)]),
    _ = safe_cmd(RootCmd),
    ok.

-spec priomap(non_neg_integer()) -> string().
priomap(Lanes) when is_integer(Lanes) ->
    lists:join(" ", lists:duplicate(16, integer_to_list(Lanes))).

%% @doc Wrapper over lists:keyfind/3
-spec config_get(atom(), [tuple(), ...]) -> {ok, any()} | error.
config_get(Key, Config) ->
    case lists:keyfind(Key, 1, Config) of
        false -> error;
        {Key, Value} -> {ok, Value}
    end.

safe_cmd(Cmd) ->
    case get_default(dry_run, false) of
        true ->
            ok = io:format("~s~n", [Cmd]),
            "";
        false ->
            os:cmd(Cmd)
    end.

get_default(Key, Default) ->
    case erlang:get(Key) of
        undefined ->
            Default;
        Val ->
            Val
    end.
