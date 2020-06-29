-module(hook_grb).

-ignore_xref([start/1, stop/0]).

-define(BOOTSTRAP_NODE, "BOOTSTRAP_NODE").
-define(BOOTSTRAP_PORT, "BOOTSTRAP_PORT").

%% API
-export([start/1,
         stop/0]).

%% Worker API
-export([conns_for_worker/1]).

%% Get ring information from Antidote,
%% and spawn all the necessary connections
start(HookOpts) ->
    logger:info("~p:~p(~p)", [?MODULE, ?FUNCTION_NAME, HookOpts]),
    ok = pvc:start(),

    %% ETS table to share data with workers
    _ = ets:new(?MODULE, [set, named_table, protected]),

    BootstrapNode = os:getenv(?BOOTSTRAP_NODE, "127.0.0.1"),
    BootstrapPort = list_to_integer(os:getenv(?BOOTSTRAP_PORT, "7878")),
    logger:info("Given bootstrap node ~p~n", [BootstrapNode]),

    {conn_pool_size, PoolSize} = lists:keyfind(conn_pool_size, 1, HookOpts),
    {connection_port, ConnectionPort} = lists:keyfind(connection_port, 1, HookOpts),

    BootstrapIp = get_bootstrap_ip(BootstrapNode),
    logger:info("Bootstraping from ~p:~p~n", [BootstrapIp, BootstrapNode]),

    {conection_opts, ConnectionOpts} = lists:keyfind(conection_opts, 1, HookOpts),
    {connection_transport, ConnTransport} = lists:keyfind(connection_transport, 1, HookOpts),

    {ok, ReplicaID, Ring, UniqueNodes} = pvc_ring:grb_replica_info(BootstrapIp, BootstrapPort),
    true = ets:insert(?MODULE, {replica_id, ReplicaID}),
    true = ets:insert(?MODULE, {ring, Ring}),
    true = ets:insert(?MODULE, {nodes, UniqueNodes}),
    true = ets:insert(?MODULE, {transport, ConnTransport}),
    ok = lists:foreach(fun(NodeIp) ->
        Connections = spawn_pool(NodeIp, ConnectionPort, ConnTransport, ConnectionOpts, PoolSize),
        ets:insert(?MODULE, {NodeIp, PoolSize, Connections}),
        ok
    end, UniqueNodes).

stop() ->
    logger:info("Unloading ~p", [?MODULE]),
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

%% If node name is given, and we're on Emulab, parse /etc/hosts to get node IP
get_bootstrap_ip(NodeName) ->
    {ok, Addr} = inet:getaddr(NodeName, inet),
    list_to_atom(inet:ntoa(Addr)).

%% Create a pool of connections to `NodeIp:ConnectionPort` of size `PoolSize`
spawn_pool(NodeIp, ConnectionPort, Transport, ConnectionOpts, PoolSize) ->
    spawn_pool(NodeIp, ConnectionPort, Transport, ConnectionOpts, PoolSize, []).

spawn_pool(_, _, _, _, 0, Acc) ->
    Acc;

spawn_pool(Ip, Port, Transport, Opts, N, Acc) ->
    {ok, Connection} = Transport:new(Ip, Port, Opts),
    spawn_pool(Ip, Port, Transport, Opts, N - 1, [Connection | Acc]).

%% Close all the pvc connections
teardown_pool(NodeIp) ->
    [{NodeIp, _, Connections}] = ets:take(?MODULE, NodeIp),
    lists:foreach(fun pvc_connection:close/1, Connections).
