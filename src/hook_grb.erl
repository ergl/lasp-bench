-module(hook_grb).

-ignore_xref([start/1, stop/0]).

-define(BOOTSTRAP_NODE, "BOOTSTRAP_NODE").
-define(BOOTSTRAP_PORT, "BOOTSTRAP_PORT").

%% API
-export([start/1,
         stop/0]).

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
    {conection_opts, ConnectionOpts} = lists:keyfind(conection_opts, 1, HookOpts),

    BootstrapIp = get_bootstrap_ip(BootstrapNode),
    logger:info("Bootstraping from ~p:~p~n", [BootstrapIp, BootstrapNode]),

    IdLen = maps:get(id_len, ConnectionOpts, 16),
    {ok, LocalIP, ReplicaID, Ring, UniqueNodes} = pvc_ring:grb_replica_info(BootstrapIp, BootstrapPort, IdLen),
    true = ets:insert(?MODULE, {local_ip, LocalIP}),
    true = ets:insert(?MODULE, {replica_id, ReplicaID}),
    true = ets:insert(?MODULE, {ring, Ring}),
    true = ets:insert(?MODULE, {nodes, UniqueNodes}),

    {Pools, RedConns} = lists:foldl(fun(NodeIp, {PoolAcc, RedConAcc}) ->
        PoolName = pool_name(NodeIp),
        shackle_pool:start(PoolName, pvc_shackle_transport,
                          [{address, NodeIp}, {port, ConnectionPort}, {reconnect, false},
                           {socket_options, [{packet, 4}, binary]},
                           {init_options, ConnectionOpts}],
                          [{pool_size, PoolSize}]),
        {ok, RedHandler} = pvc_red_connection:start_connection(NodeIp, ConnectionPort, IdLen),
        {
            PoolAcc#{NodeIp => PoolName},
            RedConAcc#{NodeIp => RedHandler}
        }
    end, {#{}, #{}}, UniqueNodes),
    true = ets:insert(?MODULE, {shackle_pools, Pools}),
    true = ets:insert(?MODULE, {red_conns, RedConns}),
    ok.

pool_name(NodeIp) ->
    list_to_atom(atom_to_list(NodeIp) ++ "_shackle_pool").

stop() ->
    logger:info("Unloading ~p", [?MODULE]),
    [{shackle_pools, Pools}] = ets:take(?MODULE, shackle_pools),
    [ shackle_pool:stop(Pool) || {_, Pool} <- maps:to_list(Pools)],
    ets:delete(?MODULE),
    ok = pvc:stop(),
    ok.

%% If node name is given, and we're on Emulab, parse /etc/hosts to get node IP
get_bootstrap_ip(NodeName) ->
    {ok, Addr} = inet:getaddr(NodeName, inet),
    list_to_atom(inet:ntoa(Addr)).
