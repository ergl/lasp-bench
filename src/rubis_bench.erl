-module(rubis_bench).

%% Ignore xref for all lasp bench drivers
-ignore_xref([new/1, run/4]).

-export([new/1,
         run/4,
         terminate/2]).

-include("lasp_bench.hrl").

-record(state, {
    worker_id :: non_neg_integer(),
    transaction_count :: non_neg_integer(),
    last_cvc :: pvc_vclock:vc(),
    retry_until_commit :: boolean(),
    coord_state :: grb_client:coord(),
    rubis_state :: rubis_bench_utils:t()
}).

new(WorkerId) ->
    {ok, CoordState} = hook_grb:make_coordinator(WorkerId),

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
run(browse_categories, _, _, S) -> {ok, S};
run(search_items_in_category, _, _, S) -> {ok, S};
run(browse_regions, _, _, S) -> {ok, S};
run(browse_categories_in_region, _, _, S) -> {ok, S};
run(search_items_in_region, _, _, S) -> {ok, S};
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
    hook_grb:stop(),
    ok.
