-module(rubis_bench).

%% Ignore xref for all lasp bench drivers
-ignore_xref([new/1, run/4]).

-export([new/1,
         run/4,
         terminate/2]).

-include("lasp_bench.hrl").

-define(global_indices, <<"global_index">>).

-record(state, {
    worker_id :: non_neg_integer(),
    transaction_count :: non_neg_integer(),
    last_cvc :: pvc_vclock:vc(),
    retry_until_commit :: boolean(),
    coord_state :: grb_client:coord(),
    rubis_state :: rubis_bench_utils:t()
}).

-type tx_id() :: {non_neg_integer(), non_neg_integer()}.

new(WorkerId) ->
    {ok, CoordState} = hook_rubis:make_coordinator(WorkerId),

    RubisState = rubis_bench_utils:new(),
    RetryUntilCommit = lasp_bench_config:get(retry_aborts, true),

    RandSeed = lasp_bench_config:get(worker_seed, os:timestamp()),
    _ = rand:seed(exsss, RandSeed),

    State = #state{worker_id=WorkerId,
                   transaction_count=0,
                   last_cvc=pvc_vclock:new(),
                   retry_until_commit=RetryUntilCommit,
                   coord_state=CoordState,
                   rubis_state=RubisState},

    {ok, State}.

run(register_user, _, _, S) -> {ok, S};

run(browse_categories, _, _, S=#state{coord_state=Coord}) ->
    {ok, Tx} = start_transaction(S),
    {ok, _, Tx1} = grb_client:read_key_snapshot(Coord, Tx, {?global_indices, all_categories}, grb_gset),
    CVC = grb_client:commit(Coord, Tx1),
    {ok, S#state{last_cvc=CVC}};

run(search_items_in_category, _, _, S=#state{coord_state=Coord}) ->
    Category = random_category(),
    %% todo(borja): Figure out a way to work in page size
    {ok, Tx} = start_transaction(S),
    {ok, Regions, Tx1} = grb_client:read_key_snapshot(Coord, Tx, {?global_indices, all_regions}, grb_gset),

    IndexKeys = maps:fold(fun(Region, _, Acc) ->
        [ { {Region, items_region_in_category, Category}, grb_gset } | Acc]
    end, [], Regions),

    %% fixme(borja): This might return a huge number of items
    %% todo(borja): Fetch items, filter on open auction
    {ok, _, Tx2} = grb_client:read_key_snapshots(Coord, Tx1, IndexKeys),
    CVC = grb_client:commit(Coord, Tx2),
    {ok, S#state{last_cvc=CVC}};

run(browse_regions, _, _, S=#state{coord_state=Coord}) ->
    {ok, Tx} = start_transaction(S),
    {ok, _, Tx1} = grb_client:read_key_snapshot(Coord, Tx, {?global_indices, all_regions}, grb_gset),
    CVC = grb_client:commit(Coord, Tx1),
    {ok, S#state{last_cvc=CVC}};

run(browse_categories_in_region, _, _, S=#state{coord_state=Coord}) ->
    %% same as browse_categories
    {ok, Tx} = start_transaction(S),
    {ok, _, Tx1} = grb_client:read_key_snapshot(Coord, Tx, {?global_indices, all_categories}, grb_gset),
    CVC = grb_client:commit(Coord, Tx1),
    {ok, S#state{last_cvc=CVC}};

run(search_items_in_region, _, _, S=#state{coord_state=Coord}) ->
    Region = random_region(),
    Category = random_category(),
    {ok, Tx} = start_transaction(S),
    %% fixme(borja): This might return a huge number of items
    %% todo(borja): Fetch items, filter on open auction
    {ok, _, Tx1} = grb_client:read_key_snapshot(Coord, Tx, {Region, items_region_category, Category}, grb_gset),
    CVC = grb_client:commit(Coord, Tx1),
    {ok, S#state{last_cvc=CVC}};

run(view_item, _, _, S) -> {ok, S};
run(view_user_info, _, _, S) -> {ok, S};
run(view_bid_history, _, _, S) -> {ok, S};
run(buy_now, _, _, S) -> {ok, S};
run(store_buy_now, _, _, S) -> {ok, S};
run(put_bid, _, _, S) -> {ok, S};
run(store_bid, _, _, S) -> {ok, S};
run(put_comment, _, _, S) -> {ok, S};
run(store_comment, _, _, S) -> {ok, S};
run(select_category_to_sell_item, _, _, S) -> {ok, S};
run(register_item, _, _, S) -> {ok, S};
run(about_me, _, _, S) -> {ok, S};
run(get_auctions_ready_for_close, _, _, S) -> {ok, S};
run(close_auction, _, _, S) -> {ok, S}.

terminate(_Reason, _State) ->
    hook_rubis:stop(),
    ok.

%% Util

-spec start_transaction(#state{}) -> {ok, grb_client:tx()}.
start_transaction(S=#state{coord_state=CoordState, last_cvc=VC}) ->
    grb_client:start_transaction(CoordState, next_tx_id(S), VC).

-spec next_tx_id(#state{}) -> tx_id().
next_tx_id(#state{transaction_count=N}) -> N.

-spec incr_tx_id(#state{}) -> #state{}.
incr_tx_id(State=#state{transaction_count=N}) ->
    State#state{transaction_count = N + 1}.

-spec random_region() -> binary().
random_region() ->
    erlang:element(
        rand:uniform(hook_rubis:get_rubis_prop(regions_size)),
        hook_rubis:get_rubis_prop(regions)
    ).

-spec random_category() -> binary().
random_category() ->
    erlang:element(1,
        erlang:element(
            rand:uniform(hook_rubis:get_rubis_prop(categories_size)),
            hook_rubis:get_rubis_prop(categories)
        )
    ).
