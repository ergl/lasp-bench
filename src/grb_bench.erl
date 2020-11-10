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

    %% Read / Update ops to use in mixed workloads
    rw_reads :: non_neg_integer(),
    rw_updates :: non_neg_integer(),

    reuse_cvc :: boolean(),
    last_cvc :: pvc_vclock:vc(),
    retry_until_commit :: boolean(),

    %% Client coordinator state
    coord_state :: grb_client:coord()
}).

new(WorkerId) ->
    {ok, CoordState} = hook_grb:make_coordinator(WorkerId),

    RonlyOps = lasp_bench_config:get(readonly_ops),
    WonlyOps = lasp_bench_config:get(writeonly_ops),
    {RWReads, RWUpdates} = lasp_bench_config:get(mixed_read_write),

    ReuseCVC = lasp_bench_config:get(reuse_cvc),
    RetryUntilCommit = lasp_bench_config:get(retry_aborts, true),

    State = #state{worker_id=WorkerId,
                   transaction_count=0,

                   readonly_ops=RonlyOps,
                   writeonly_ops=WonlyOps,

                   rw_reads=RWReads,
                   rw_updates=RWUpdates,

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

%%====================================================================
%% Blue operations
%%====================================================================

run(readonly_blue, KeyGen, _, State = #state{readonly_ops=N}) ->
    CVC = perform_readonly_blue(State, gen_keys(N, KeyGen)),
    {ok, incr_tx_id(State#state{last_cvc=CVC})};

run(readonly_blue_bypass, KeyGen, _, State = #state{readonly_ops=N}) ->
    CVC = perform_readonly_blue_bypass(State, gen_keys(N, KeyGen)),
    {ok, incr_tx_id(State#state{last_cvc=CVC})};

run(writeonly_blue, KeyGen, ValueGen, State = #state{writeonly_ops=N}) ->
    CVC = perform_writeonly_blue(State, gen_updates(N, KeyGen, ValueGen)),
    {ok, incr_tx_id(State#state{last_cvc=CVC})};

run(writeonly_blue_bypass, KeyGen, ValueGen, State = #state{writeonly_ops=N}) ->
    CVC = perform_writeonly_blue_bypass(State, gen_updates(N, KeyGen, ValueGen)),
    {ok, incr_tx_id(State#state{last_cvc=CVC})};

run(read_write_blue, KeyGen, ValueGen, State = #state{rw_reads=RN, rw_updates=WN}) ->
    CVC = perform_read_write_blue(State, gen_keys(RN, KeyGen), gen_updates(WN, KeyGen, ValueGen)),
    {ok, incr_tx_id(State#state{last_cvc=CVC})};

run(read_write_blue_track, KeyGen, ValueGen, State = #state{coord_state=Coord, rw_reads=RN, rw_updates=WN}) ->
    Keys = gen_keys(RN, KeyGen),
    Updates = gen_updates(WN, KeyGen, ValueGen),
    {StartTook, {ok, Tx}} = timer:tc(fun maybe_start_with_clock/1, [State]),

    {Tx1, ReadTook} = lists:foldl(fun(K, {AccTx, AccTook}) ->
        {Took, {ok, _, NextTx}} = timer:tc(grb_client, read_op, [Coord, AccTx, K]),
        {NextTx, AccTook + Took}
    end, {Tx, 0}, Keys),

    {Tx2, UpdateTook} = lists:foldl(fun({K, V}, {AccTx, AccTook}) ->
        {Took, {ok, _, NextTx}} = timer:tc(grb_client, update_op, [Coord, AccTx, K, V]),
        {NextTx, Took + AccTook}
    end, {Tx1, 0}, Updates),

    {CommitTook, CVC} = timer:tc(grb_client, commit, [Coord, Tx2]),
    {ok, {track_mixed_blue, incr_tx_id(State#state{last_cvc=CVC}), StartTook, {RN, ReadTook}, {WN, UpdateTook}, CommitTook}};

run(readonly_blue_barrier, KeyGen, _, State = #state{readonly_ops=N, coord_state=Coord}) ->
    CVC = perform_readonly_blue(State, gen_keys(N, KeyGen)),
    ok = perform_uniform_barrier(Coord, CVC),
    {ok, incr_tx_id(State#state{last_cvc=CVC})};

run(writeonly_blue_barrier, KeyGen, ValueGen, State = #state{writeonly_ops=N, coord_state=Coord}) ->
    CVC = perform_writeonly_blue(State, gen_updates(N, KeyGen, ValueGen)),
    ok = perform_uniform_barrier(Coord, CVC),
    {ok, incr_tx_id(State#state{last_cvc=CVC})};

run(read_write_blue_barrier, KeyGen, ValueGen, State = #state{rw_reads=RN, rw_updates=WN, coord_state=Coord}) ->
    CVC = perform_read_write_blue(State, gen_keys(RN, KeyGen), gen_updates(WN, KeyGen, ValueGen)),
    ok = perform_uniform_barrier(Coord, CVC),
    {ok, incr_tx_id(State#state{last_cvc=CVC})};

%%====================================================================
%% Red operations
%%====================================================================

run(readonly_red_track, KeyGen, _, State = #state{coord_state=CoordState}) ->
    {StartTook, {ok, Tx}} = timer:tc(fun maybe_start_with_clock/1, [State]),
    {ReadTook, {ok, _, NextTx}} = timer:tc(grb_client, read_op, [CoordState, Tx, KeyGen()]),
    {CommitTook, Result} = timer:tc(grb_client, commit_red, [CoordState, NextTx]),
    case Result of
        {abort, _}=Err ->
            {error, Err, incr_tx_id(State)};
        {ok, {CVC, CommitTimings}} ->
            {ok, {track_red_commit, incr_tx_id(State#state{last_cvc=CVC}),
                  StartTook, ReadTook, CommitTook, CommitTimings}}
    end;

run(readonly_red, KeyGen, _, State = #state{readonly_ops=N}) ->
    case perform_readonly_red(State, gen_keys(N, KeyGen)) of
        {ok, CVC} -> {ok, incr_tx_id(State#state{last_cvc=CVC})};
        Err -> {error, Err, incr_tx_id(State)}
    end;

run(readonly_red_bypass, KeyGen, _, State = #state{readonly_ops=N}) ->
    case perform_readonly_red_bypass(State, gen_keys(N, KeyGen)) of
        {ok, CVC} -> {ok, incr_tx_id(State#state{last_cvc=CVC})};
        Err -> {error, Err, incr_tx_id(State)}
    end;

run(read_start_red, KeyGen, _, State = #state{readonly_ops=N}) ->
    case perform_start_readonly_red(State, gen_keys(N, KeyGen)) of
        {ok, CVC} -> {ok, incr_tx_id(State#state{last_cvc=CVC})};
        Err -> {error, Err, incr_tx_id(State)}
    end;

run(writeonly_red, KeyGen, ValueGen, State = #state{writeonly_ops=N}) ->
    case perform_writeonly_red(State, gen_updates(N, KeyGen, ValueGen)) of
        {ok, CVC} -> {ok, incr_tx_id(State#state{last_cvc=CVC})};
        Err -> {error, Err, incr_tx_id(State)}
    end;

run(read_write_red, KeyGen, ValueGen, State = #state{rw_reads=RN, rw_updates=WN}) ->
    case perform_read_write_red(State, gen_keys(RN, KeyGen), gen_updates(WN, KeyGen, ValueGen)) of
        {ok, CVC} -> {ok, incr_tx_id(State#state{last_cvc=CVC})};
        Err -> {error, Err, incr_tx_id(State)}
    end;

run(readonly_red_barrier, KeyGen, _, State = #state{readonly_ops=N, coord_state=Coord}) ->
    case perform_readonly_red(State, gen_keys(N, KeyGen)) of
        {ok, CVC} ->
            ok = perform_uniform_barrier(Coord, CVC),
            {ok, incr_tx_id(State#state{last_cvc=CVC})};
        Err -> {error, Err, State}
    end;

run(writeonly_red_barrier, KeyGen, ValueGen, State = #state{writeonly_ops=N, coord_state=Coord}) ->
    case perform_writeonly_red(State, gen_updates(N, KeyGen, ValueGen)) of
        {ok, CVC} ->
            ok = perform_uniform_barrier(Coord, CVC),
            {ok, incr_tx_id(State#state{last_cvc=CVC})};
        Err -> {error, Err, incr_tx_id(State)}
    end;

run(read_write_red_barrier, KeyGen, ValueGen, State = #state{rw_reads=RN, rw_updates=WN, coord_state=Coord}) ->
    case perform_read_write_red(State, gen_keys(RN, KeyGen), gen_updates(WN, KeyGen, ValueGen)) of
        {ok, CVC} ->
            ok = perform_uniform_barrier(Coord, CVC),
            {ok, incr_tx_id(State#state{last_cvc=CVC})};
        Err -> {error, Err, incr_tx_id(State)}
    end.

terminate(_Reason, _State) ->
    hook_grb:stop(),
    ok.

perform_uniform_barrier(CoordState, CVC) ->
    ok = grb_client:uniform_barrier(CoordState, CVC).

%%====================================================================
%% Blue operations
%%====================================================================

%% todo(borja): Implement multi-key read
perform_readonly_blue(S=#state{coord_state=CoordState}, Keys) ->
    {ok, Tx} = maybe_start_with_clock(S),
    Tx1 = lists:foldl(fun(K, AccTx) ->
        {ok, _, NextTx} = grb_client:read_op(CoordState, AccTx, K),
        NextTx
    end, Tx, Keys),
    grb_client:commit(CoordState, Tx1).

perform_readonly_blue_bypass(S=#state{coord_state=CoordState}, Keys) ->
    {ok, Tx} = maybe_start_with_clock(S),
    Tx1 = lists:foldl(fun(K, AccTx) ->
        {ok, _, NextTx} = grb_client:read_bypass(CoordState, AccTx, K),
        NextTx
    end, Tx, Keys),
    grb_client:commit(CoordState, Tx1).

%% todo(borja): Implement multi-key update
perform_writeonly_blue(S=#state{coord_state=CoordState}, Updates) ->
    {ok, Tx} = maybe_start_with_clock(S),
    Tx1 = lists:foldl(fun({K, V}, AccTx) ->
        {ok, _, NextTx} = grb_client:update_op(CoordState, AccTx, K, V),
        NextTx
    end, Tx, Updates),
    grb_client:commit(CoordState, Tx1).

perform_writeonly_blue_bypass(S=#state{coord_state=CoordState}, Updates) ->
    {ok, Tx} = maybe_start_with_clock(S),
    Tx1 = lists:foldl(fun({K, V}, AccTx) ->
        {ok, _, NextTx} = grb_client:update_bypass(CoordState, AccTx, K, V),
        NextTx
    end, Tx, Updates),
    grb_client:commit(CoordState, Tx1).

%% todo(borja): Implement multi-key read and update
perform_read_write_blue(S=#state{coord_state=CoordState}, Keys, Updates) ->
    {ok, Tx} = maybe_start_with_clock(S),

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
%% Red operations
%%====================================================================

perform_readonly_red(S=#state{coord_state=CoordState}, Keys) ->
    {ok, Tx} = maybe_start_with_clock(S),
    Tx1 = lists:foldl(fun(K, AccTx) ->
        {ok, _, NextTx} = grb_client:read_op(CoordState, AccTx, K),
        NextTx
    end, Tx, Keys),
    case grb_client:commit_red(CoordState, Tx1) of
        {abort, _}=Err -> maybe_retry_readonly(S, Keys, Err);
        Other -> Other
    end.

perform_start_readonly_red(S=#state{coord_state=CoordState}, Keys=[HdKey | Rest]) ->
    io:format("Reading keys ~p~n", [Keys]),
    {ok, _, Tx} = maybe_start_read_with_clock(HdKey, S),
    Tx1 = lists:foldl(fun(K, AccTx) ->
        {ok, _, NextTx} = grb_client:read_op(CoordState, AccTx, K),
        NextTx
    end, Tx, Rest),
    case grb_client:commit_red(CoordState, Tx1) of
        {abort, _}=Err -> Err;
        Other -> Other
    end.

perform_readonly_red_bypass(S=#state{coord_state=CoordState}, Keys) ->
    {ok, Tx} = maybe_start_with_clock(S),
    Tx1 = lists:foldl(fun(K, AccTx) ->
        {ok, _, NextTx} = grb_client:read_bypass(CoordState, AccTx, K),
        NextTx
    end, Tx, Keys),
    case grb_client:commit_red(CoordState, Tx1) of
        {abort, _}=Err -> Err;
        Other -> Other
    end.

perform_writeonly_red(S=#state{coord_state=CoordState}, Updates) ->
    {ok, Tx} = maybe_start_with_clock(S),
    Tx1 = lists:foldl(fun({K, V}, AccTx) ->
        {ok, _, NextTx} = grb_client:update_op(CoordState, AccTx, K, V),
        NextTx
    end, Tx, Updates),
    case grb_client:commit_red(CoordState, Tx1) of
        {abort, _}=Err -> maybe_retry_writeonly(S, Updates, Err);
        Other -> Other
    end.

perform_read_write_red(S=#state{coord_state=CoordState}, Keys, Updates) ->
    {ok, Tx} = maybe_start_with_clock(S),

    Tx1 = lists:foldl(fun(K, AccTx) ->
        {ok, _, NextTx} = grb_client:read_op(CoordState, AccTx, K),
        NextTx
    end, Tx, Keys),

    Tx2 = lists:foldl(fun({K, V}, AccTx) ->
        {ok, _, NextTx} = grb_client:update_op(CoordState, AccTx, K, V),
        NextTx
    end, Tx1, Updates),

    case grb_client:commit_red(CoordState, Tx2) of
        {abort, _}=Err -> maybe_retry_read_write(S, Keys, Updates, Err);
        Other -> Other
    end.

%%====================================================================
%% Util functions
%%====================================================================

-spec maybe_start_with_clock(#state{}) -> {ok, grb_client:tx()}.
maybe_start_with_clock(S=#state{coord_state=CoordState, reuse_cvc=true, last_cvc=VC}) ->
    grb_client:start_transaction(CoordState, next_tx_id(S), VC);

maybe_start_with_clock(S=#state{coord_state=CoordState, reuse_cvc=false}) ->
    grb_client:start_transaction(CoordState, next_tx_id(S)).

-spec maybe_start_read_with_clock(binary(), #state{}) -> {ok, binary(), grb_client:tx()}.
maybe_start_read_with_clock(Key, S=#state{coord_state=CoordState, reuse_cvc=true, last_cvc=VC}) ->
    grb_client:start_read(CoordState, next_tx_id(S), Key, VC);

maybe_start_read_with_clock(Key, S=#state{coord_state=CoordState, reuse_cvc=false}) ->
    grb_client:start_read(CoordState, next_tx_id(S), Key).

maybe_retry_readonly(#state{retry_until_commit=false}, _, Err) -> Err;
maybe_retry_readonly(S, Keys, _) -> perform_readonly_red(incr_tx_id(S), Keys).

maybe_retry_writeonly(#state{retry_until_commit=false}, _, Err) -> Err;
maybe_retry_writeonly(S, Updates, _) -> perform_writeonly_red(incr_tx_id(S), Updates).

maybe_retry_read_write(#state{retry_until_commit=false}, _, _, Err) -> Err;
maybe_retry_read_write(S, Keys, Updates, _) -> perform_read_write_red(incr_tx_id(S), Keys, Updates).

%% @doc Generate N random keys
-spec gen_keys(non_neg_integer(), key_gen(binary())) -> [binary()].
gen_keys(N, K) -> gen_keys(N, K, []).

gen_keys(0, _, Acc) -> Acc;
gen_keys(N, K, Acc) -> gen_keys(N - 1, K, [K() | Acc]).

-spec gen_updates(non_neg_integer(), key_gen(binary()), key_gen(binary())) -> [{binary(), binary()}].
gen_updates(N, K, V) -> gen_updates(N, K, V, []).
gen_updates(0, _, _, Acc) -> Acc;
gen_updates(N, K, V, Acc) -> gen_updates(N - 1, K, V, [{K(), V()} | Acc]).

-spec next_tx_id(#state{}) -> tx_id().
next_tx_id(#state{transaction_count=N}) -> N.

-spec incr_tx_id(#state{}) -> #state{}.
incr_tx_id(State=#state{transaction_count=N}) ->
    State#state{transaction_count = N + 1}.
