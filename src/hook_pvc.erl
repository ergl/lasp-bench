-module(hook_pvc).

-ignore_xref([start/1, stop/0]).

%% API
-export([start/1,
         stop/0]).

%% Worker API
-export([conns_for_worker/1]).

%% Get ring information from Antidote,
%% and spawn all the necessary connections
start(HookOpts) ->
    lager:info("~p:~p(~p)", [?MODULE, ?FUNCTION_NAME, HookOpts]),
    ok = pvc:start(),

    %% ETS table to share data with workers
    _ = ets:new(?MODULE, [set, named_table, protected]),

    NodeNameOpt = lists:keyfind(bootstrap_node, 1, HookOpts),
    NodeIPOpt = lists:keyfind(bootstrap_node_ip, 1, HookOpts),
    NodeClusterOpt = lists:keyfind(bootstrap_cluster, 1, HookOpts),

    {bootstrap_port, Port} = lists:keyfind(bootstrap_port, 1, HookOpts),
    {conn_pool_size, PoolSize} = lists:keyfind(conn_pool_size, 1, HookOpts),
    {connection_port, ConnectionPort} = lists:keyfind(connection_port, 1, HookOpts),

    BootstrapIp = get_bootstrap_ip(NodeNameOpt, NodeClusterOpt, NodeIPOpt),
    lager:info("Given bootstrap IP ~p~n", [BootstrapIp]),

    ConnectionOpts0 = case lists:keyfind(connection_cork_ms, 1, HookOpts) of
        {connection_cork_ms, CorkMs} -> #{cork_len => CorkMs};
        false -> #{}
    end,

    ConnectionOpts1 = case lists:keyfind(connection_buff_wm, 1, HookOpts) of
        {connection_buff_wm, BuffWatermark} ->
            maps:put(buf_watermark, BuffWatermark, ConnectionOpts0);
        false ->
            ConnectionOpts0
    end,

    {ok, Ring, UniqueNodes} = pvc_ring:partition_info(BootstrapIp, Port),
    ets:insert(?MODULE, {ring, Ring}),
    ets:insert(?MODULE, {nodes, UniqueNodes}),
    ok = lists:foreach(fun(NodeIp) ->
        Connections = spawn_pool(NodeIp, ConnectionPort, ConnectionOpts1, PoolSize),
        ets:insert(?MODULE, {NodeIp, PoolSize, Connections}),
        ok
    end, UniqueNodes).

stop() ->
    lager:info("Unloading ~p", [?MODULE]),
    [{nodes, UniqueNodes}] = ets:take(?MODULE, nodes),
    lists:foreach(fun(NodeIp) ->
        teardown_pool(NodeIp)
    end, UniqueNodes),
    ets:delete(?MODULE),
    ok = pvc:stop(),
    ok.

-spec conns_for_worker(non_neg_integer()) -> pvc:cluster_conns().
conns_for_worker(WorkerId) ->
    Nodes = ets:lookup_element(?MODULE, nodes, 2),
    [begin
         [{Node, PoolSize, Connections}] = ets:lookup(?MODULE, Node),
         {Node, lists:nth((WorkerId rem PoolSize) + 1, Connections)}
     end || Node <- Nodes].

%% If Node IP option is present, override anything else
-spec get_bootstrap_ip(tuple() | false, tuple() | false, tuple() | false) -> atom().
get_bootstrap_ip(_, _, {bootstrap_node_ip, NodeIp}) ->
    NodeIp;

%% If node name is given, and we're on Emulab, parse /etc/hosts to get node IP
get_bootstrap_ip({bootstrap_node, NodeName}, {bootstrap_cluster, emulab}, _) ->
    NameB = atom_to_binary(NodeName, latin1),
    {ok, Raw} = file:read_file("/etc/hosts"),
    Lines = binary:split(Raw, <<"\n">>, [global, trim_all]),
    [BootstrapIp] = [begin
        [IP | _] = binary:split(L, <<"\t">>),
        binary_to_atom(IP, latin1)
    end || L <- Lines, binary:match(L, NameB) =/= nomatch],
    BootstrapIp;

%% If we're on Apollo, just use inet
get_bootstrap_ip({bootstrap_node, NodeName}, {bootstrap_cluster, apollo}, _) ->
    {ok, Addr} = inet:getaddr(NodeName, inet),
    list_to_atom(inet:ntoa(Addr)).

%% Create a pool of connections to `NodeIp:ConnectionPort` of size `PoolSize`
spawn_pool(NodeIp, ConnectionPort, ConnectionOpts, PoolSize) ->
    spawn_pool(NodeIp, ConnectionPort, ConnectionOpts, PoolSize, []).

spawn_pool(_, _, _, 0, Acc) ->
    Acc;

spawn_pool(Ip, Port, Opts, N, Acc) ->
    {ok, Connection} = pvc_connection:new(Ip, Port, Opts),
    spawn_pool(Ip, Port, Opts, N - 1, [Connection | Acc]).

%% Close all the pvc connections
teardown_pool(NodeIp) ->
    [{NodeIp, _, Connections}] = ets:take(?MODULE, NodeIp),
    lists:foreach(fun pvc_connection:close/1, Connections).
