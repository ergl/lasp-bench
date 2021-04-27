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
         make_coordinator/1,
         next_transaction_id/1]).

-export([trace_msg/2]).

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

    CausalPoolSize = proplists:get_value(conn_pool_size, HookOpts),
    StrongPoolSize = proplists:get_value(red_conn_pool_size, HookOpts, 1),
    ShackleBacklog = proplists:get_value(shackle_backlog, HookOpts, infinity),
    ShackleRetries = proplists:get_value(shackle_retries, HookOpts, infinity),
    ConnectionPort = proplists:get_value(connection_port, HookOpts),
    ConnModuleOpts = proplists:get_value(conection_opts, HookOpts),

    BootstrapIp = get_bootstrap_ip(BootstrapNode),
    logger:info("Bootstraping from ~p:~p~n", [BootstrapIp, BootstrapNode]),

    IdLen = maps:get(id_len, ConnModuleOpts, 16),
    {ok, LocalIP, ReplicaID, Ring, UniqueNodes} = pvc_ring:grb_replica_info(BootstrapIp, BootstrapPort, IdLen),
    true = ets:insert(?MODULE, {local_ip, LocalIP}),
    true = ets:insert(?MODULE, {replica_id, ReplicaID}),
    true = ets:insert(?MODULE, {ring, Ring}),
    true = ets:insert(?MODULE, {nodes, UniqueNodes}),

    CausalPoolOpts = [{pool_size, CausalPoolSize}, {backlog_size, ShackleBacklog}, {max_retries, ShackleRetries}],
    StrongPoolOpts = [{pool_size, StrongPoolSize}, {backlog_size, ShackleBacklog}, {max_retries, ShackleRetries}],
    {Pools, RedPools} = lists:foldl(fun(NodeIp, {PoolAcc, RedConAcc}) ->
        {
            PoolAcc#{
                NodeIp => make_shackle_pool(pool_name(NodeIp), NodeIp, ConnectionPort,
                                            pvc_shackle_transport, ConnModuleOpts, CausalPoolOpts)
            },
            RedConAcc#{
                NodeIp => make_shackle_pool(red_pool_name(NodeIp), NodeIp, ConnectionPort,
                                            pvc_red_connection, ConnModuleOpts, StrongPoolOpts)
            }
        }
    end, {#{}, #{}}, UniqueNodes),
    true = ets:insert(?MODULE, {shackle_pools, Pools}),
    true = ets:insert(?MODULE, {red_shackle_pools, RedPools}),

    ok = case proplists:get_value(tx_trace_log_path, HookOpts, undefined) of
        undefined -> ok;
        TraceLogPath -> ok = create_trace_log_file(TraceLogPath)
    end,

    ok.

pool_name(NodeIp) ->
    list_to_atom(atom_to_list(NodeIp) ++ "_shackle_pool").

red_pool_name(NodeIp) ->
    list_to_atom(atom_to_list(NodeIp) ++ "_shackle_red_pool").

make_shackle_pool(Name, Ip, Port, Module, ModuleOpts, PoolOpts) ->
    ok = shackle_pool:start(Name, Module,
        [
            {address, Ip}, {port, Port}, {reconnect, false},
            %% todo(borja): Add config for socket opts?
            {socket_options, [{packet, 4}, binary, {nodelay, true}]},
            {init_options, ModuleOpts}
        ],
        PoolOpts),
    Name.

stop() ->
    logger:info("Unloading ~p", [?MODULE]),

    [{shackle_pools, Pools}] = ets:take(?MODULE, shackle_pools),
    [ shackle_pool:stop(Pool) || {_, Pool} <- maps:to_list(Pools)],

    [{red_shackle_pools, RedPools}] = ets:take(?MODULE, red_shackle_pools),
    [ shackle_pool:stop(RPool) || {_, RPool} <- maps:to_list(RedPools)],

    ets:delete(?MODULE),
    ok = pvc:stop(),
    ok = close_trace_log_file(),
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
    RedConnPools = get_config(red_shackle_pools),
    grb_client:new(ReplicaId, LocalIP, WorkerId, RingInfo, ConnPools, RedConnPools).

-spec next_transaction_id(non_neg_integer()) -> non_neg_integer().
next_transaction_id(WorkerId) ->
    case erlang:get({?MODULE, WorkerId}) of
        undefined ->
            erlang:put({?MODULE, WorkerId}, 1),
            0;
        N ->
            erlang:put({?MODULE, WorkerId}, N+1)
    end.

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

-spec trace_msg(Fmt :: string(), Args :: [term()]) -> ok.
trace_msg(Fmt, Args) ->
    case get_trace_log_pid() of
        {error, no_log} ->
            ok;
        {ok, Pid} ->
            Pid ! {trace_msg, append_time_ts(io_lib:format(Fmt, Args))}
    end,
    ok.

-spec create_trace_log_file(Path :: string()) -> ok | {error, term()}.
create_trace_log_file(TraceLogPath) ->
    Self = self(),
    Ref = erlang:make_ref(),
    Pid = erlang:spawn(fun() -> init_file_loop(Self, Ref, TraceLogPath) end),
    receive
        {ok, Ref} ->
            persistent_term:put({?MODULE, log_file_pid}, Pid);
        {error, Ref, Reason} ->
            {error, Reason}
    end.

-spec get_trace_log_pid() -> {ok, pid()} | {error, no_log}.
get_trace_log_pid() ->
    case persistent_term:get({?MODULE, log_file_pid}, undefined) of
        undefined ->
            {error, no_log};
        Pid ->
            {ok, Pid}
    end.

-spec close_trace_log_file() -> ok.
close_trace_log_file() ->
    case get_trace_log_pid() of
        {error, no_log} ->
            ok;

        {ok, Pid} ->
            Ref = make_ref(),
            Pid ! {close_file, Ref, self()},
            receive {ok, Ref} -> ok end
    end.

init_file_loop(ParentPid, ParentRef, Path) ->
    case file:open(Path, [write, raw, delayed_write]) of
        {ok, IODev} ->
            ParentPid ! {ok, ParentRef},
            file_pid_loop(IODev);
        {error, Reason} ->
            ParentPid ! {error, ParentRef, Reason}
    end.

file_pid_loop(IODev) ->
    receive
        {trace_msg, Bin} ->
            ok = file:write(IODev, Bin),
            file_pid_loop(IODev);

        {close_file, Ref, From} ->
            ok = file:sync(IODev),
            ok = file:close(IODev),
            From ! {ok, Ref};

       _ ->
           file_pid_loop(IODev)
    end.

append_time_ts(Str) ->
    Ts = {_, _, Micros} = os:timestamp(),
    {_, {Hour, Min, Sec}} = calendar:now_to_datetime(Ts),
    io_lib:format("~2w:~2..0w:~2..0w.~6..0w ~s", [Hour, Min, Sec, Micros, Str]).
