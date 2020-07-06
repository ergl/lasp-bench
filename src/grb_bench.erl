-module(grb_bench).

%% Ignore xref for all lasp bench drivers
-ignore_xref([new/1, run/4]).

-export([new/1,
         run/4,
         terminate/2]).

-include("lasp_bench.hrl").

-type tx_id() :: {non_neg_integer(), non_neg_integer()}.
-type key_gen(T) :: fun(() -> T).

-record(state, {
    %% Id of this worker thread
    worker_id :: non_neg_integer(),
    %% Auto incremented counter to create unique transaction ids
    transaction_count :: non_neg_integer(),
    %% Client coordinator state
    coord_state :: pvc:coord_state(),
    readonly_ops :: non_neg_integer(),
    writeonly_ops :: non_neg_integer(),
    mixed_ops_ration :: {non_neg_integer(), non_neg_integer()},
    keep_cvc :: boolean(),
    last_cvc :: pvc_vclock:vc(),
    retry_until_commit :: boolean(),

    sockets = undefined :: #{inet:ip_address() => inet:socket()} | undefined,
    ring = undefined :: pvc_ring:ring() | undefined
}).

new(Id) ->
    Transport = ets:lookup_element(hook_grb, transport, 2),
    new(Id, Transport).

new(Id, socket) ->
    RingInfo = ets:lookup_element(hook_grb, ring, 2),
    Nodes = ets:lookup_element(hook_grb, nodes, 2),
    Port = ets:lookup_element(hook_grb, port, 2),
    Sockets = lists:foldl(fun(Node, Acc) ->
        {ok, Socket} = gen_tcp:connect(Node, Port, [{active, false}, {packet, 4}, binary]),
        Acc#{Node => Socket}
    end, #{}, Nodes),
    State = #state{worker_id=Id,
                   ring=RingInfo,
                   sockets=Sockets},
    {ok, State};

new(Id, Transport) ->
    RonlyOps = lasp_bench_config:get(readonly_ops),
    WonlyOps = lasp_bench_config:get(writeonly_ops),
    MixedOpsRatio = lasp_bench_config:get(ratio),
    KeepCVC = lasp_bench_config:get(keep_cvc),
    RetryUntilCommit = lasp_bench_config:get(retry_aborts, true),

    ReplicaId = ets:lookup_element(hook_grb, replica_id, 2),
    RingInfo = ets:lookup_element(hook_grb, ring, 2),
    Connections = hook_grb:conns_for_worker(Id),

    {ok, CoordState} = pvc:grb_new(#{ring => RingInfo,
                                     transport => Transport,
                                     connections => Connections,
                                     coord_id => Id,
                                     replica_id => ReplicaId}),

    State = #state{worker_id=Id,
                   transaction_count=0,
                   coord_state=CoordState,
                   readonly_ops=RonlyOps,
                   writeonly_ops=WonlyOps,
                   mixed_ops_ration=MixedOpsRatio,
                   keep_cvc=KeepCVC,
                   last_cvc=pvc_vclock:new(),
                   retry_until_commit=RetryUntilCommit},

    {ok, State}.

run(ping, KeyGen, _, State = #state{worker_id=Id, ring=RingInfo, sockets=Socks}) ->
    Key = integer_to_binary(KeyGen(), 36),
    {Partition, Node} = pvc_ring:get_key_indexnode(RingInfo, Key),
    Socket = maps:get(Node, Socks),
    Msg = <<0:16, (ppb_grb_driver:uniform_barrier(Partition, #{}))/binary>>,
    ok = gen_tcp:send(Socket, Msg),
    case gen_tcp:recv(Socket, 0) of
        {error, Reason} ->
            logger:error("Worker ~p bad recv ~p", [Id, Reason]),
            {error, Reason, State};
        {ok, <<0:16, RawReply/binary>>} ->
            ok = pvc_proto:decode_serv_reply(RawReply),
            {ok, State}
    end;

run(readonly_blue, KeyGen, _, State = #state{readonly_ops=N}) ->
    Keys = gen_keys(N, KeyGen),
    perform_readonly_blue(Keys, State);

run(writeonly_blue, KeyGen, ValueGen, State = #state{writeonly_ops=N}) ->
    Keys = gen_keys(N, KeyGen),
    Ops = [ {K, ValueGen()} || K <- Keys],
    perform_writeonly_blue(Ops, State);

run(read_write_blue, KeyGen, ValueGen, State = #state{mixed_ops_ration={RN, WN}}) ->
    ReadKeys = gen_keys(RN, KeyGen),
    WriteKeys = gen_keys(WN, KeyGen),
    Updates = [ {K, ValueGen()} || K <- WriteKeys],
    perform_read_write_blue(ReadKeys, Updates, State).

terminate(_Reason, #state{sockets=Socks}) when is_map(Socks) ->
    _ = [ gen_tcp:close(S) || {_, S} <- maps:to_list(Socks)],
    ok;

terminate(_Reason, _State) ->
    ok.

perform_readonly_blue(Keys, State=#state{coord_state=CoordState}) ->
    {ok, Tx} = maybe_start_with_clock(State),
    %% todo(borja): Implement multi-key read
    Tx1 = lists:foldl(fun(K, AccTx) ->
        {ok, _, NextTx} = pvc:grb_ronly_op(CoordState, AccTx, K),
        NextTx
    end, Tx, Keys),
    CVC = pvc:grb_blue_commit(CoordState, Tx1),
    {ok, maybe_keep_clock(State, CVC)}.

perform_writeonly_blue(Updates, State=#state{coord_state=CoordState}) ->
    {ok, Tx} = maybe_start_with_clock(State),
    %% todo(borja): Implement multi-key update
    Tx1 = lists:foldl(fun({K, V}, AccTx) ->
        {ok, _, NextTx} = pvc:grb_op(CoordState, AccTx, K, V),
        NextTx
    end, Tx, Updates),
    CVC = pvc:grb_blue_commit(CoordState, Tx1),
    {ok, maybe_keep_clock(State, CVC)}.

perform_read_write_blue(Keys, Updates, State=#state{coord_state=CoordState}) ->
    {ok, Tx} = maybe_start_with_clock(State),
    %% todo(borja): Implement multi-key read and update
    Tx1 = lists:foldl(fun(K, AccTx) ->
        {ok, _, NextTx} = pvc:grb_ronly_op(CoordState, AccTx, K),
        NextTx
    end, Tx, Keys),

    Tx2 = lists:foldl(fun({K, V}, AccTx) ->
        {ok, _, NextTx} = pvc:grb_op(CoordState, AccTx, K, V),
        NextTx
    end, Tx1, Updates),
    CVC = pvc:grb_blue_commit(CoordState, Tx2),
    {ok, maybe_keep_clock(State, CVC)}.

%%====================================================================
%% Util functions
%%====================================================================

maybe_start_with_clock(S=#state{coord_state=CoordState, keep_cvc=true, last_cvc=VC}) ->
    pvc:grb_start_tx(CoordState, next_tx_id(S), VC);
maybe_start_with_clock(S=#state{coord_state=CoordState, keep_cvc=false}) ->
    pvc:grb_start_tx(CoordState, next_tx_id(S)).

maybe_keep_clock(S=#state{keep_cvc=true}, CVC) ->
    incr_tx_id(S#state{last_cvc=CVC});
maybe_keep_clock(S=#state{keep_cvc=false}, _) ->
    incr_tx_id(S).

%% @doc Generate N random keys
-spec gen_keys(non_neg_integer(), key_gen(non_neg_integer())) -> binary() | [binary()].
gen_keys(N, K) ->
    gen_keys(N, K, []).

gen_keys(0, _, Acc) ->
    Acc;

gen_keys(N, K, Acc) ->
    gen_keys(N - 1, K, [integer_to_binary(K(), 36) | Acc]).

-spec next_tx_id(#state{}) -> tx_id().
next_tx_id(#state{worker_id=Id, transaction_count=N}) ->
    {Id, N}.

-spec incr_tx_id(#state{}) -> #state{}.
incr_tx_id(State=#state{transaction_count=N}) ->
    State#state{transaction_count = N + 1}.
