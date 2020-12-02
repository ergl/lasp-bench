-module(rubis_bench).

%% Ignore xref for all lasp bench drivers
-ignore_xref([new/1, run/4]).

-export([new/1,
         run/4,
         terminate/2]).

-include("lasp_bench.hrl").

-define(global_indices, <<"global_index">>).
-define(register_user_label, <<"rubis/registerUser">>).
-define(store_buy_now_label, <<"rubis/storeBuyNow">>).
-define(place_bid_label, <<"rubis/placeBid">>).
-define(close_auction_label, <<"rubis/closeAuction">>).

-record(state, {
    worker_id :: non_neg_integer(),
    local_ip_str :: list(),
    transaction_count :: non_neg_integer(),
    last_cvc :: pvc_vclock:vc(),
    retry_until_commit :: boolean(),
    coord_state :: grb_client:coord(),

    %% Benchmark state
    user_count = 1 :: pos_integer(),
    item_count = 1 :: pos_integer(),

    last_generated_user = undefined :: {Region :: binary(), Name :: binary()} | undefined,
    last_generated_item = undefined :: {Region :: binary(), Id :: binary()} | undefined
}).
-type state() :: #state{}.
-type tx_id() :: {non_neg_integer(), non_neg_integer()}.

new(WorkerId) ->
    {ok, CoordState} = hook_rubis:make_coordinator(WorkerId),
    LocalIp = hook_rubis:get_config(local_ip),

    RetryUntilCommit = lasp_bench_config:get(retry_aborts, true),

    RandSeed = lasp_bench_config:get(worker_seed, os:timestamp()),
    _ = rand:seed(exsss, RandSeed),

    State = #state{worker_id=WorkerId,
                   local_ip_str=inet:ntoa(LocalIp),
                   transaction_count=0,
                   last_cvc=pvc_vclock:new(),
                   retry_until_commit=RetryUntilCommit,
                   coord_state=CoordState},

    {ok, State}.

run(register_user, _, _, S0=#state{coord_state=Coord}) ->
    Region = random_region(),
    {Nickname, S1} = gen_new_nickname(S0),
    Updates = [{
        {{Region, users, Nickname}, grb_crdt:make_op(grb_lww, Nickname)},
        {{Region, users, Nickname, name}, grb_crdt:make_op(grb_lww, Nickname)},
        {{Region, users, Nickname, lastname}, grb_crdt:make_op(grb_lww, Nickname)},
        {{Region, users, Nickname, password}, grb_crdt:make_op(grb_lww, Nickname)},
        {{Region, users, Nickname, email}, grb_crdt:make_op(grb_lww, Nickname)},
        {{Region, users, Nickname, rating}, grb_crdt:make_op(grb_gcounter, 0)}
    }],
    {ok, Tx} = start_transaction(S1),
    {ok, Index, Tx1} = grb_client:read_key_snapshot(Coord, Tx, Nickname, grb_lww),
    S2 = case Index of
        <<>> ->
            %% username not claimed, send our operations
            {ok, Tx2} = grb_client:send_key_operations(Coord, Tx1, Updates),
            case grb_client:commit_red(Coord, Tx2, ?register_user_label) of
                {ok, CVC} ->
                    S1#state{last_cvc=CVC, last_generated_user={Region, Nickname}};
                {abort, _Reason} ->
                    %% todo(borja): What to do?
                    S1
            end;
        _ ->
            %% no need to abort at server, we didn't store anything there
            S1
    end,
    %% todo(borja): Store this user nickname?
    {ok, incr_tx_id(S2)};

run(browse_categories, _, _, S=#state{coord_state=Coord}) ->
    {ok, Tx} = start_transaction(S),
    {ok, _, Tx1} = grb_client:read_key_snapshot(Coord, Tx, {?global_indices, all_categories}, grb_gset),
    CVC = grb_client:commit(Coord, Tx1),
    {ok, incr_tx_id(S#state{last_cvc=CVC})};

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
    {ok, incr_tx_id(S#state{last_cvc=CVC})};

run(browse_regions, _, _, S=#state{coord_state=Coord}) ->
    {ok, Tx} = start_transaction(S),
    {ok, _, Tx1} = grb_client:read_key_snapshot(Coord, Tx, {?global_indices, all_regions}, grb_gset),
    CVC = grb_client:commit(Coord, Tx1),
    {ok, incr_tx_id(S#state{last_cvc=CVC})};

run(browse_categories_in_region, _, _, S=#state{coord_state=Coord}) ->
    %% same as browse_categories
    {ok, Tx} = start_transaction(S),
    {ok, _, Tx1} = grb_client:read_key_snapshot(Coord, Tx, {?global_indices, all_categories}, grb_gset),
    CVC = grb_client:commit(Coord, Tx1),
    {ok, incr_tx_id(S#state{last_cvc=CVC})};

run(search_items_in_region, _, _, S=#state{coord_state=Coord}) ->
    Region = random_region(),
    Category = random_category(),
    {ok, Tx} = start_transaction(S),
    %% fixme(borja): This might return a huge number of items
    %% todo(borja): Fetch items, filter on open auction
    {ok, _, Tx1} = grb_client:read_key_snapshot(Coord, Tx, {Region, items_region_category, Category}, grb_gset),
    CVC = grb_client:commit(Coord, Tx1),
    {ok, incr_tx_id(S#state{last_cvc=CVC})};

run(view_item, _, _, S0=#state{coord_state=Coord}) ->
    {Region, ItemId} = random_item(S0),
    {ok, Tx} = start_transaction(S0),
    {ok, _, Tx1} = grb_client:read_key_snapshots(Coord, Tx, [{
        {{Region, items, ItemId, seller}, grb_lww},
        {{Region, items, ItemId, category}, grb_lww},
        {{Region, items, ItemId, initial_price}, grb_lww},
        {{Region, items, ItemId, quantity}, grb_lww},
        {{Region, items, ItemId, reserve_price}, grb_lww},
        {{Region, items, ItemId, buy_now}, grb_lww},
        {{Region, items, ItemId, closed}, grb_lww}
    }]),
    {ok, CVC} = grb_client:commit(Coord, Tx1),
    {ok, incr_tx_id(S0#state{last_cvc=CVC})};

run(view_user_info, _, _, S0=#state{coord_state=Coord}) ->
    {Region, NickName} = random_user(S0),
    CommentIndexKey = {Region, comments_to_user, {Region, users, NickName}},
    UserKeys = [{
        %% User object
        {{Region, users, NickName, name}, grb_lww},
        {{Region, users, NickName, lastname}, grb_lww},
        {{Region, users, NickName, rating}, grb_gcounter},
        %% Comments to this user
        %% fixme(borja): limit?
        {CommentIndexKey, grb_gset}
    }],
    {ok, Tx} = start_transaction(S0),
    {ok, #{ CommentIndexKey := CommentIds }, Tx1} = grb_client:read_key_snapshots(Coord, Tx, UserKeys),
    CommentKeys = maps:fold(fun({CommentRegion, comments, CommentId}, _, Acc) ->
        [
            {{CommentRegion, comments, CommentId, from}, grb_lww},
            {{CommentRegion, comments, CommentId, rating}, grb_lww},
            {{CommentRegion, comments, CommentId, text}, grb_lww}
            | Acc
        ]
    end, [], CommentIds),
    {ok, _, Tx2} = grb_client:read_key_snapshots(Coord, Tx1, CommentKeys),
    {ok, CVC} = grb_client:commit(Coord, Tx2),
    {ok, incr_tx_id(S0#state{last_cvc=CVC})};

run(view_bid_history, _, _, S) -> {ok, S};
run(buy_now, _, _, S) -> {ok, S};
run(store_buy_now, _, _, S) -> {ok, S};
run(put_bid, _, _, S) -> {ok, S};
run(store_bid, _, _, S) -> {ok, S};
run(put_comment, _, _, S) -> {ok, S};
run(store_comment, _, _, S) -> {ok, S};
run(select_category_to_sell_item, _, _, S) -> {ok, S};

run(register_item, _, _, S0=#state{coord_state=Coord}) ->
    Category = random_category(),
    {Region, SellerNickname} = random_user(S0),
    Seller = {Region, users, SellerNickname},
    {ItemId, S1} = gen_new_itemid(S0),
    ItemKey = {Region, items, ItemId},
    InitialPrice = rand:uniform(100),
    Quantity = safe_uniform(hook_rubis:get_rubis_prop(item_max_quantity)),
    ReservePrice = case (rand:uniform(100) =< hook_rubis:get_rubis_prop(item_reserve_percentage)) of
        true -> InitialPrice + rand:uniform(10);
        false -> 0
    end,
    BuyNow = case (rand:uniform(100) =< hook_rubis:get_rubis_prop(item_buy_now_percentage)) of
        true -> InitialPrice + ReservePrice + rand:uniform(10);
        false -> 0
    end,
    Updates = [{
        %% Item Updates
        {{Region, items, ItemId, seller}, grb_crdt:make_op(grb_lww, {Region, users, Seller})},
        {{Region, items, ItemId, category}, grb_crdt:make_op(grb_lww, Category)},
        {{Region, items, ItemId, initial_price}, grb_crdt:make_op(grb_lww, InitialPrice)},
        {{Region, items, ItemId, quantity}, grb_crdt:make_op(grb_lww, Quantity)},
        {{Region, items, ItemId, reserve_price}, grb_crdt:make_op(grb_lww, ReservePrice)},
        {{Region, items, ItemId, buy_now}, grb_crdt:make_op(grb_lww, BuyNow)},
        {{Region, items, ItemId, closed}, grb_crdt:make_op(grb_lww, false)},
        %% append to ITEMS_seller index
        {{Region, items_seller, Seller}, grb_crdt:make_op(grb_set, ItemKey)},
        %% append to ITEMS_region_category index
        {{Region, items_region_category, Category}, grb_crdt:make_op(grb_set, ItemKey)}
    }],
    {ok, Tx} = start_transaction(S1),
    %% Claim the item key, read/write so we don't do any blind updates, should get back the same value
    {ok, ItemKey, Tx1} = grb_client:update_operation(Coord, Tx, ItemKey, grb_crdt:make_op(grb_lww, ItemKey)),
    {ok, Tx2} = grb_client:send_key_operations(Coord, Tx1, Updates),
    {ok, CVC} = grb_client:commit(Coord, Tx2),
    {ok, incr_tx_id(S1#state{last_cvc=CVC, last_generated_item={Region, ItemId}})};

run(about_me, _, _, S) -> {ok, S};
run(get_auctions_ready_for_close, _, _, S) -> {ok, S};
run(close_auction, _, _, S) -> {ok, S}.

terminate(_Reason, _State) ->
    hook_rubis:stop(),
    ok.

%%%===================================================================
%%% Transaction Utils
%%%===================================================================

-spec start_transaction(#state{}) -> {ok, grb_client:tx()}.
start_transaction(S=#state{coord_state=CoordState, last_cvc=VC}) ->
    grb_client:start_transaction(CoordState, next_tx_id(S), VC).

-spec next_tx_id(#state{}) -> tx_id().
next_tx_id(#state{transaction_count=N}) -> N.

-spec incr_tx_id(#state{}) -> #state{}.
incr_tx_id(State=#state{transaction_count=N}) ->
    State#state{transaction_count = N + 1}.

%%%===================================================================
%%% Random Utils
%%%===================================================================

-spec random_region() -> binary().
random_region() ->
    {NReg, Reg} = hook_rubis:get_rubis_prop(regions),
    erlang:element(rand:uniform(NReg), Reg).

-spec random_category() -> binary().
random_category() ->
    {NCat, Cat} = hook_rubis:get_rubis_prop(categories),
    element(1, erlang:element(rand:uniform(NCat), Cat)).

-spec random_category_items() -> {binary(), pos_integer()}.
random_category_items() ->
    {NCat, Cat} = hook_rubis:get_rubis_prop(categories),
    erlang:element(rand:uniform(NCat), Cat).

-spec random_user(state()) -> {Region :: binary(), Nickname :: binary()}.
random_user(#state{last_generated_user={Region, NickName}}) ->
    {Region, NickName};

random_user(#state{last_generated_user=undefined}) ->
    Region = random_region(),
    Id = rand:uniform(hook_rubis:get_rubis_prop(user_per_region)),
    {Region, list_to_binary(io_lib:format("~s/user/preload_~b", [Region, Id]))}.

random_item(#state{last_generated_item={Region, ItemId}}) ->
    {Region, ItemId};

random_item(#state{last_generated_item=undefined}) ->
    {NReg, Reg} = hook_rubis:get_rubis_prop(regions),
    {Category, ItemsInCat} = random_category_items(),
    ItemIdNumeric = rand:uniform(ItemsInCat),
    Region = erlang:element((ItemIdNumeric rem NReg), Reg),
    ItemId = list_to_binary(io_lib:format("~s/items/preload_~b", [Category, ItemIdNumeric])),
    {Region, ItemId}.

-spec gen_new_nickname(state()) -> {binary(), state()}.
gen_new_nickname(S=#state{local_ip_str=IPStr, worker_id=Id, user_count=N}) ->
    {
        list_to_binary(IPStr ++ integer_to_list(Id) ++ integer_to_list(N)),
        S#state{user_count=N+1}
    }.

-spec gen_new_itemid(state()) -> {binary(), state()}.
gen_new_itemid(S=#state{local_ip_str=IPStr, worker_id=Id, item_count=N}) ->
    {
        list_to_binary(IPStr ++ integer_to_list(Id) ++ integer_to_list(N)),
        S#state{item_count=N+1}
    }.

-spec safe_uniform(pos_integer()) -> pos_integer().
safe_uniform(0) -> 0;
safe_uniform(X) when X >= 1 -> rand:uniform(X).

-spec random_string(Size :: non_neg_integer()) -> string().
random_string(N) ->
    Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890",
    lists:foldl(fun(_, Acc) ->
        [lists:nth(rand:uniform(length(Chars)), Chars)] ++ Acc
    end, [], lists:seq(1, N)).
