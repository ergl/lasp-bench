-module(grb_bench_v2).

%% Ignore xref for all lasp bench drivers
-ignore_xref([new/1, run/4, terminate/2]).

-export([new/1,
         run/4,
         terminate/2]).

-export([handle_continue/4]).

-include("lasp_bench.hrl").

-type key_gen(T) :: fun(() -> T).

-record(state, {
    worker_id :: non_neg_integer(),
    coord_state :: grb_client:coord(),

    readonly_ops :: non_neg_integer(),
    writeonly_ops :: non_neg_integer(),

    %% Read / Update ops to use in mixed workloads
    rw_reads :: non_neg_integer(),
    rw_updates :: non_neg_integer(),

    crdt_type :: atom(),

    %% Generator for contention
    strong_generator :: key_gen(non_neg_integer()),

    reuse_cvc :: boolean(),
    last_cvc :: pvc_vclock:vc(),
    retry_until_commit :: boolean()
}).

new(WorkerId) ->
    {ok, CoordState} = hook_grb:make_coordinator(WorkerId),

    RonlyOps = lasp_bench_config:get(readonly_ops),
    WonlyOps = lasp_bench_config:get(writeonly_ops),
    {RWReads, RWUpdates} = lasp_bench_config:get(mixed_read_write),

    ReuseCVC = lasp_bench_config:get(reuse_cvc),
    RetryUntilCommit = lasp_bench_config:get(retry_aborts, true),

    Spec = lasp_bench_config:get(strong_key_generator, undefined),

    CRDTType = lasp_bench_config:get(crdt_type, grb_lww),
    State = #state{worker_id=WorkerId,
                   coord_state=CoordState,

                   readonly_ops=RonlyOps,
                   writeonly_ops=WonlyOps,

                   rw_reads=RWReads,
                   rw_updates=RWUpdates,

                   crdt_type=CRDTType,

                   %% No matter, we will overwrite this in handle_continue/4
                   strong_generator=undefined,

                   reuse_cvc=ReuseCVC,
                   last_cvc=pvc_vclock:new(),
                   retry_until_commit=RetryUntilCommit},

    {ok, State, {continue, Spec}}.

handle_continue(undefined, KeyGen, _, State) ->
    {ok, State#state{strong_generator=KeyGen}};

handle_continue(StrongKeyGenSpec, _, _, State = #state{worker_id=WorkerId}) ->
    ContentionGen = lasp_bench_keygen:new(StrongKeyGenSpec, WorkerId),
    {ok, State#state{strong_generator=ContentionGen}}.

run(uniform_barrier, _, _, State = #state{coord_state=Coord, last_cvc=CVC}) ->
    ok = grb_client:uniform_barrier(Coord, CVC),
    {ok, State};

%%====================================================================
%% Blue operations
%%====================================================================

run(readonly_blue, KeyGen, _, State = #state{readonly_ops=N}) ->
    CVC = perform_readonly_blue(State, gen_keys(N, KeyGen)),
    {ok, State#state{last_cvc=CVC}};

run(writeonly_blue, KeyGen, ValueGen, State = #state{writeonly_ops=N}) ->
    CVC = perform_writeonly_blue(State, gen_updates(N, KeyGen, ValueGen)),
    {ok, State#state{last_cvc=CVC}};

run(read_write_blue, KeyGen, ValueGen, State = #state{rw_reads=RN, rw_updates=WN}) ->
    CVC = perform_read_write_blue(State, gen_keys(RN, KeyGen), gen_updates(WN, KeyGen, ValueGen)),
    {ok, State#state{last_cvc=CVC}};

run(readonly_blue_barrier, KeyGen, _, State = #state{readonly_ops=N, coord_state=Coord}) ->
    CVC = perform_readonly_blue(State, gen_keys(N, KeyGen)),
    ok = perform_uniform_barrier(Coord, CVC),
    {ok, State#state{last_cvc=CVC}};

run(writeonly_blue_barrier, KeyGen, ValueGen, State = #state{writeonly_ops=N, coord_state=Coord}) ->
    CVC = perform_writeonly_blue(State, gen_updates(N, KeyGen, ValueGen)),
    ok = perform_uniform_barrier(Coord, CVC),
    {ok, State#state{last_cvc=CVC}};

run(read_write_blue_barrier, KeyGen, ValueGen, State = #state{rw_reads=RN, rw_updates=WN, coord_state=Coord}) ->
    CVC = perform_read_write_blue(State, gen_keys(RN, KeyGen), gen_updates(WN, KeyGen, ValueGen)),
    ok = perform_uniform_barrier(Coord, CVC),
    {ok, State#state{last_cvc=CVC}};

%%====================================================================
%% Red operations
%%====================================================================

run(readonly_red, _, _, State = #state{readonly_ops=N, strong_generator=KeyGen}) ->
    case perform_readonly_red(State, gen_keys(N, KeyGen), 0) of
        {ok, CVC, Retries} -> {ok, ignore, Retries - 1, State#state{last_cvc=CVC}};
        Err -> {error, Err, State}
    end;

run(writeonly_red, _, ValueGen, State = #state{writeonly_ops=N, strong_generator=KeyGen}) ->
    case perform_writeonly_red(State, gen_updates(N, KeyGen, ValueGen), 0) of
        {ok, CVC, Retries} -> {ok, ignore, Retries - 1, State#state{last_cvc=CVC}};
        Err -> {error, Err, State}
    end;

run(read_write_red, _, ValueGen, State = #state{rw_reads=RN, rw_updates=WN, strong_generator=KeyGen}) ->
    case perform_read_write_red(State, gen_keys(RN, KeyGen), gen_updates(WN, KeyGen, ValueGen), 0) of
        {ok, CVC, Retries} -> {ok, ignore, Retries - 1, State#state{last_cvc=CVC}};
        Err -> {error, Err, State}
    end;

run(readonly_red_barrier, _, _, State = #state{readonly_ops=N, coord_state=Coord, strong_generator=KeyGen}) ->
    case perform_readonly_red(State, gen_keys(N, KeyGen), 0) of
        {ok, CVC, Retries} ->
            ok = perform_uniform_barrier(Coord, CVC),
            {ok, ignore, Retries - 1, State#state{last_cvc=CVC}};
        Err -> {error, Err, State}
    end;

run(writeonly_red_barrier, _, ValueGen, State = #state{writeonly_ops=N, coord_state=Coord, strong_generator=KeyGen}) ->
    case perform_writeonly_red(State, gen_updates(N, KeyGen, ValueGen), 0) of
        {ok, CVC, Retries} ->
            ok = perform_uniform_barrier(Coord, CVC),
            {ok, ignore, Retries - 1, State#state{last_cvc=CVC}};
        Err -> {error, Err, State}
    end;

run(read_write_red_barrier, _, ValueGen, State = #state{rw_reads=RN, rw_updates=WN, coord_state=Coord, strong_generator=KeyGen}) ->
    case perform_read_write_red(State, gen_keys(RN, KeyGen), gen_updates(WN, KeyGen, ValueGen), 0) of
        {ok, CVC, Retries} ->
            ok = perform_uniform_barrier(Coord, CVC),
            {ok, ignore, Retries - 1, State#state{last_cvc=CVC}};
        Err -> {error, Err, State}
    end.

terminate(_Reason, _State) ->
    hook_grb:stop(),
    ok.

perform_uniform_barrier(CoordState, CVC) ->
    ok = grb_client:uniform_barrier(CoordState, CVC).

%%====================================================================
%% Blue operations
%%====================================================================

perform_readonly_blue(S=#state{coord_state=CoordState, crdt_type=Type}, Keys) ->
    {ok, Tx} = start_transaction(S),
    Tx1 = sequential_read(CoordState, Tx, Keys, Type),
    grb_client:commit(CoordState, Tx1).

perform_writeonly_blue(S=#state{coord_state=CoordState, crdt_type=Type}, Updates) ->
    {ok, Tx} = start_transaction(S),
    Tx1 = sequential_update(CoordState, Tx, Updates, Type),
    grb_client:commit(CoordState, Tx1).

perform_read_write_blue(S=#state{coord_state=CoordState, crdt_type=Type}, Keys, Updates) ->
    {ok, Tx} = start_transaction(S),
    Tx1 = sequential_read(CoordState, Tx, Keys, Type),
    Tx2 = sequential_update(CoordState, Tx1, Updates, Type),
    grb_client:commit(CoordState, Tx2).

%%====================================================================
%% Red operations
%%====================================================================

perform_readonly_red(S=#state{coord_state=CoordState, crdt_type=Type}, Keys, Attempts) ->
    {ok, Tx} = start_transaction_with_indexnode(S, Keys),
    Tx1 = sequential_read(CoordState, Tx, Keys, Type),
    case grb_client:commit_red(CoordState, Tx1) of
        {abort, _}=Err -> maybe_retry_readonly(S, Keys, Err, Attempts + 1);
        {ok, CVC} -> {ok, CVC, Attempts + 1}
    end.

perform_writeonly_red(S=#state{coord_state=CoordState, crdt_type=Type}, Updates, Attempts) ->
    {ok, Tx} = start_transaction_with_indexnode(S, Updates),
    Tx1 = sequential_update(CoordState, Tx, Updates, Type),
    case grb_client:commit_red(CoordState, Tx1) of
        {abort, _}=Err -> maybe_retry_writeonly(S, Updates, Err, Attempts + 1);
        {ok, CVC} -> {ok, CVC, Attempts + 1}
    end.

perform_read_write_red(S=#state{coord_state=CoordState, crdt_type=Type}, Keys, Updates, Attempts) ->
    {ok, Tx} = start_transaction_with_indexnode(S, Keys),
    Tx1 = sequential_read(CoordState, Tx, Keys, Type),
    Tx2 = sequential_update(CoordState, Tx1, Updates, Type),
    case grb_client:commit_red(CoordState, Tx2) of
        {abort, _}=Err -> maybe_retry_read_write(S, Keys, Updates, Err, Attempts + 1);
        {ok, CVC} -> {ok, CVC, Attempts + 1}
    end.

%%====================================================================
%% Util functions
%%====================================================================

-spec sequential_read(grb_client:coord(), grb_client:tx(), [term()], atom()) -> grb_client:tx().
sequential_read(Coord, Tx, Keys, Type) ->
    lists:foldl(
        fun(Key, Acc) ->
            {ok, _, Next} = grb_client:read_key_snapshot(Coord, Acc, Key, Type),
            Next
        end,
        Tx,
        Keys
    ).

-spec sequential_update(grb_client:coord(), grb_client:tx(), [{term(), term()}], atom()) -> grb_client:tx().
sequential_update(Coord, Tx, Updates, Type) ->
    lists:foldl(
        fun({K, V}, Acc) ->
            {ok, _, Next} = grb_client:update_operation(Coord, Acc, K, grb_crdt:make_op(Type, V)),
            Next
        end,
        Tx,
        Updates
    ).

-spec start_transaction(#state{}) -> {ok, grb_client:tx()}.
start_transaction(S=#state{coord_state=CoordState, worker_id=WorkerId}) ->
    grb_client:start_transaction(CoordState,
                                 hook_grb:next_transaction_id(WorkerId),
                                 get_start_clock(S)).

-spec start_transaction(#state{}, grb_client:index_node()) -> {ok, grb_client:tx()}.
start_transaction(S=#state{coord_state=CoordState, worker_id=WorkerId}, IndexNode) ->
    grb_client:start_transaction(CoordState,
                                 hook_grb:next_transaction_id(WorkerId),
                                 get_start_clock(S), IndexNode).

-spec start_transaction_with_indexnode(#state{}, [term() | {term(), term()}]) -> {ok, grb_client:tx()}.
start_transaction_with_indexnode(S=#state{coord_state=CoordState}, [{Key, _} | _]) ->
    start_transaction(S, grb_client:key_location(CoordState, Key));

start_transaction_with_indexnode(S=#state{coord_state=CoordState}, [Key | _]) ->
    start_transaction(S, grb_client:key_location(CoordState, Key)).

-spec get_start_clock(#state{}) -> grb_vclock:vc(grb_client:replica_id()).
get_start_clock(#state{reuse_cvc=true, last_cvc=VC}) -> VC;
get_start_clock(#state{reuse_cvc=false}) -> grb_vclock:new().

maybe_retry_readonly(#state{retry_until_commit=false}, _, Err, _At) -> Err;
maybe_retry_readonly(S, Keys, _, Attempts) ->
    lasp_bench_stats:op_complete({readonly_red, readonly_red}, {error, abort}, ignore),
    perform_readonly_red(S, Keys, Attempts).

maybe_retry_writeonly(#state{retry_until_commit=false}, _, Err, _At) -> Err;
maybe_retry_writeonly(S, Updates, _, Attempts) ->
    lasp_bench_stats:op_complete({writeonly_red, writeonly_red}, {error, abort}, ignore),
    perform_writeonly_red(S, Updates, Attempts).

maybe_retry_read_write(#state{retry_until_commit=false}, _, _, Err, _At) -> Err;
maybe_retry_read_write(S, Keys, Updates, _, Attempts) ->
    lasp_bench_stats:op_complete({read_write_red, read_write_red}, {error, abort}, ignore),
    perform_read_write_red(S, Keys, Updates, Attempts).

%% @doc Generate N random keys
-spec gen_keys(non_neg_integer(), key_gen(binary())) -> [binary()].
gen_keys(N, K) -> gen_keys(N, K, []).

gen_keys(0, _, Acc) -> Acc;
gen_keys(N, K, Acc) -> gen_keys(N - 1, K, [K() | Acc]).

-spec gen_updates(non_neg_integer(), key_gen(binary()), key_gen(binary())) -> [{binary(), binary()}].
gen_updates(N, K, V) -> gen_updates(N, K, V, []).
gen_updates(0, _, _, Acc) -> Acc;
gen_updates(N, K, V, Acc) -> gen_updates(N - 1, K, V, [{K(), V()} | Acc]).
