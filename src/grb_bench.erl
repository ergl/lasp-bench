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

    readonly_ops :: non_neg_integer(),
    writeonly_ops :: non_neg_integer(),
    mixed_ops_ration :: {non_neg_integer(), non_neg_integer()},
    reuse_cvc :: boolean(),
    last_cvc :: pvc_vclock:vc(),
    retry_until_commit :: boolean(),

    %% Client coordinator state (pvc/grb_client, depending on mode)
    coord_state :: pvc:coord_state() | grb_client:coord()
}).

new(Id) ->
    RonlyOps = lasp_bench_config:get(readonly_ops),
    WonlyOps = lasp_bench_config:get(writeonly_ops),
    MixedOpsRatio = lasp_bench_config:get(ratio),
    ReuseCVC = lasp_bench_config:get(reuse_cvc),
    RetryUntilCommit = lasp_bench_config:get(retry_aborts, true),

    ReplicaId = ets:lookup_element(hook_grb, replica_id, 2),
    RingInfo = ets:lookup_element(hook_grb, ring, 2),

    LocalIP = ets:lookup_element(hook_grb, local_ip, 2),
    ConnPools = ets:lookup_element(hook_grb, shackle_pools, 2),
    {ok, CoordState} = grb_client:new(ReplicaId, LocalIP, Id, RingInfo, ConnPools),

    State = #state{worker_id=Id,
                   transaction_count=0,
                   readonly_ops=RonlyOps,
                   writeonly_ops=WonlyOps,
                   mixed_ops_ration=MixedOpsRatio,
                   reuse_cvc=ReuseCVC,
                   last_cvc=pvc_vclock:new(),
                   retry_until_commit=RetryUntilCommit,
                   coord_state=CoordState},

    {ok, State}.

run(ping, _, _, State = #state{coord_state=Coord}) ->
    ok = grb_client:uniform_barrier(Coord, pvc_vclock:new()),
    {ok, State};

run(uniform_barrier, _, _, State = #state{coord_state=Coord, last_cvc=CVC}) ->
    ok = grb_client:uniform_barrier(Coord, CVC),
    {ok, State};

run(readonly_blue, KeyGen, _, State = #state{readonly_ops=N, coord_state=Coord}) ->
    Keys = gen_keys(N, KeyGen),
    {ok, Tx} = maybe_start_with_clock(State),
    CVC = perform_readonly_blue(Coord, Tx, Keys),
    {ok, incr_tx_id(State#state{last_cvc=CVC})};

run(writeonly_blue, KeyGen, ValueGen, State = #state{writeonly_ops=N, coord_state=Coord}) ->
    Keys = gen_keys(N, KeyGen),
    Ops = [ {K, ValueGen()} || K <- Keys],
    {ok, Tx} = maybe_start_with_clock(State),
    CVC = perform_writeonly_blue(Coord, Tx, Ops),
    {ok, incr_tx_id(State#state{last_cvc=CVC})};

run(read_write_blue, KeyGen, ValueGen, State = #state{mixed_ops_ration={RN, WN}, coord_state=Coord}) ->
    ReadKeys = gen_keys(RN, KeyGen),
    WriteKeys = gen_keys(WN, KeyGen),
    Updates = [ {K, ValueGen()} || K <- WriteKeys],
    {ok, Tx} = maybe_start_with_clock(State),
    CVC = perform_read_write_blue(Coord, Tx, ReadKeys, Updates),
    {ok, incr_tx_id(State#state{last_cvc=CVC})};

run(readonly_blue_barrier, KeyGen, _, State = #state{readonly_ops=N, coord_state=Coord}) ->
    Keys = gen_keys(N, KeyGen),
    {ok, Tx} = maybe_start_with_clock(State),
    CVC = perform_readonly_blue(Coord, Tx, Keys),
    ok = perform_uniform_barrier(Coord, CVC),
    {ok, incr_tx_id(State#state{last_cvc=CVC})};

run(writeonly_blue_barrier, KeyGen, ValueGen, State = #state{writeonly_ops=N, coord_state=Coord}) ->
    Keys = gen_keys(N, KeyGen),
    Ops = [ {K, ValueGen()} || K <- Keys],
    {ok, Tx} = maybe_start_with_clock(State),
    CVC = perform_writeonly_blue(Coord, Tx, Ops),
    ok = perform_uniform_barrier(Coord, CVC),
    {ok, incr_tx_id(State#state{last_cvc=CVC})};

run(read_write_blue_barrier, KeyGen, ValueGen, State = #state{mixed_ops_ration={RN, WN}, coord_state=Coord}) ->
    ReadKeys = gen_keys(RN, KeyGen),
    WriteKeys = gen_keys(WN, KeyGen),
    Updates = [ {K, ValueGen()} || K <- WriteKeys],
    {ok, Tx} = maybe_start_with_clock(State),
    CVC = perform_read_write_blue(Coord, Tx, ReadKeys, Updates),
    ok = perform_uniform_barrier(Coord, CVC),
    {ok, incr_tx_id(State#state{last_cvc=CVC})}.

terminate(_Reason, _State) ->
    hook_grb:stop(),
    ok.

perform_uniform_barrier(CoordState, CVC) ->
    ok = grb_client:uniform_barrier(CoordState, CVC).

%% todo(borja): Implement multi-key read
perform_readonly_blue(CoordState, Tx, Keys) ->
    Tx1 = lists:foldl(fun(K, AccTx) ->
        {ok, _, NextTx} = grb_client:read_op(CoordState, AccTx, K),
        NextTx
    end, Tx, Keys),
    grb_client:commit(CoordState, Tx1).

%% todo(borja): Implement multi-key update
perform_writeonly_blue(CoordState, Tx, Updates) ->
    Tx1 = lists:foldl(fun({K, V}, AccTx) ->
        {ok, _, NextTx} = grb_client:update_op(CoordState, AccTx, K, V),
        NextTx
    end, Tx, Updates),
    grb_client:commit(CoordState, Tx1).

%% todo(borja): Implement multi-key read and update
perform_read_write_blue(CoordState, Tx, Keys, Updates) ->
    Tx1 = lists:foldl(fun(K, AccTx) ->
        {ok, _, NextTx} = grb_client:read_op(CoordState, AccTx, K),
        NextTx
    end, Tx, Keys),

    Tx2 = lists:foldl(fun({K, V}, AccTx) ->
        {ok, _, NextTx} = grb_client:update_op(CoordState, AccTx, K, V),
        NextTx
    end, Tx1, Updates),

    grb_client:commit(CoordState, Tx2).

%%====================================================================
%% Util functions
%%====================================================================

-spec maybe_start_with_clock(#state{}) -> {ok, grb_client:tx() | pvc:grb_transaction()}.
maybe_start_with_clock(S=#state{coord_state=CoordState, reuse_cvc=true, last_cvc=VC}) ->
    grb_client:start_transaction(CoordState, next_tx_id(S), VC);

maybe_start_with_clock(S=#state{coord_state=CoordState, reuse_cvc=false}) ->
    grb_client:start_transaction(CoordState, next_tx_id(S)).

%% @doc Generate N random keys
-spec gen_keys(non_neg_integer(), key_gen(non_neg_integer())) -> binary() | [binary()].
gen_keys(N, K) ->
    gen_keys(N, K, []).

gen_keys(0, _, Acc) ->
    Acc;

gen_keys(N, K, Acc) ->
    gen_keys(N - 1, K, [integer_to_binary(K(), 36) | Acc]).

-spec next_tx_id(#state{}) -> tx_id().
next_tx_id(#state{transaction_count=N}) -> N.

-spec incr_tx_id(#state{}) -> #state{}.
incr_tx_id(State=#state{transaction_count=N}) ->
    State#state{transaction_count = N + 1}.
