-module(hook_grb).

-ignore_xref([start/1, stop/0]).

-define(BOOTSTRAP_NODE, "BOOTSTRAP_NODE").
-define(BOOTSTRAP_PORT, "BOOTSTRAP_PORT").

%% API
-export([start/1,
         stop/0]).

-export([get_config/1,
         constant_partition_generator/2,
         exclude_partition_generator/2,
         worker_generator/1,
         make_coordinator/1]).

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
    {conection_opts, ConnModuleOpts} = lists:keyfind(conection_opts, 1, HookOpts),

    BootstrapIp = get_bootstrap_ip(BootstrapNode),
    logger:info("Bootstraping from ~p:~p~n", [BootstrapIp, BootstrapNode]),

    IdLen = maps:get(id_len, ConnModuleOpts, 16),
    {ok, LocalIP, ReplicaID, Ring, UniqueNodes} = pvc_ring:grb_replica_info(BootstrapIp, BootstrapPort, IdLen),
    true = ets:insert(?MODULE, {local_ip, LocalIP}),
    true = ets:insert(?MODULE, {replica_id, ReplicaID}),
    true = ets:insert(?MODULE, {ring, Ring}),
    true = ets:insert(?MODULE, {nodes, UniqueNodes}),

    {Pools, RedConns} = lists:foldl(fun(NodeIp, {PoolAcc, RedConAcc}) ->
        PoolName = pool_name(NodeIp),
        %% todo(borja): Add config for socket opts?
        shackle_pool:start(PoolName, pvc_shackle_transport,
                          [{address, NodeIp}, {port, ConnectionPort}, {reconnect, false},
                           {socket_options, [{packet, 4}, binary, {nodelay, true}]},
                           {init_options, ConnModuleOpts}],
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

get_config(Key) ->
    ets:lookup_element(?MODULE, Key, 2).

-spec make_coordinator(non_neg_integer()) -> {ok, grb_client:coord()}.
make_coordinator(WorkerId) ->
    ReplicaId = get_config(replica_id),
    RingInfo = get_config(ring),
    LocalIP = get_config(local_ip),
    ConnPools = get_config(shackle_pools),
    RedConnections = get_config(red_conns),
    grb_client:new(ReplicaId, LocalIP, WorkerId, RingInfo, ConnPools, RedConnections).

%% Use as {key_generator, {function, hook_grb, worker_generator, []}}
-spec worker_generator(Id :: non_neg_integer()) -> fun(() -> binary()).
worker_generator(Id) ->
    fun() -> make_worker_key(get_config(local_ip), Id) end.

%% @doc Always land on the first partition.
%%
%%      Use as {key_generator, {function, hook_grb, constant_partition_generator, [#{ ... ]}}
%%
%%      For integer keys, grb maps to partitions by doing (Key rem Partitions).
%%      If our key is a multiple of Partitions, Key rem Partitions will aways yield 0,
%%      which maps to the first partition in the ring.
%%
-spec constant_partition_generator(
    Id :: non_neg_integer(),
    Opts :: #{ring_size := non_neg_integer(), n_keys => non_neg_integer()}
) -> Gen :: fun(() -> term()).

constant_partition_generator(_Id, Opts = #{ring_size := RingSize}) ->
    Keys = maps:get(n_keys, Opts, 1000000),
    fun() -> (rand:uniform(Keys) * RingSize) end.

%% @doc Always exclude the first partition.
%%
%%      Use as {key_generator, {function, hook_grb, exclude_partition_generator, [#{ ... ]}}
%%
%%      If the zero-partition is located at (Key rem Partition) = 0, anything that doesn't map
%%      to this partition is enough, so we exclude any number N s.t. (N rem Partitions) = 0.
%%
-spec exclude_partition_generator(
    Id :: non_neg_integer(),
    Opts :: #{ring_size := non_neg_integer(), n_keys => non_neg_integer()}
) -> Gen :: fun(() -> term()).

exclude_partition_generator(_Id, Opts = #{ring_size := RingSize}) ->
    Keys = maps:get(n_keys, Opts, 1000000),
    fun() -> loop_rand_until_not_div(Keys, RingSize) end.

-spec loop_rand_until_not_div(non_neg_integer(), non_neg_integer()) -> non_neg_integer().
loop_rand_until_not_div(Limit, K) ->
    loop_rand_until_not_div(rand:uniform(Limit), K, Limit).

-spec loop_rand_until_not_div(non_neg_integer(), non_neg_integer(), non_neg_integer()) -> non_neg_integer().
loop_rand_until_not_div(N, K, _Limit)
    when (N rem K) =/= 0 ->
        N;
loop_rand_until_not_div(_, K, Limit) ->
    loop_rand_until_not_div(rand:uniform(Limit), K, Limit).

-spec make_worker_key(inet:ip_address(), non_neg_integer()) -> binary().
make_worker_key(IP, WorkerId) ->
    <<(list_to_binary(inet:ntoa(IP)))/binary, "_",  (integer_to_binary(WorkerId, 36))/binary>>.
