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

-define(gset_limit_op(__S), grb_crdt:wrap_op(grb_gset, grb_gset:limit_op(__S))).

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

run(register_user, _, _, State) ->
    {ok, register_user_loop(State)};

run(browse_categories, _, _, S=#state{coord_state=Coord}) ->
    {ok, Tx} = start_transaction(S),
    {ok, _, Tx1} = grb_client:read_key_snapshot(Coord, Tx, {?global_indices, all_categories}, grb_gset),
    CVC = grb_client:commit(Coord, Tx1),
    {ok, incr_tx_id(S#state{last_cvc=CVC})};

run(search_items_in_category, _, _, S=#state{coord_state=Coord}) ->
    %% mostly the same as search_items_in_region
    {ok, Tx} = start_transaction(S),
    CVC = search_items_in_region_category(Coord, Tx, random_region(), random_category(), random_page_size()),
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
    {ok, Tx} = start_transaction(S),
    CVC = search_items_in_region_category(Coord, Tx, random_region(), random_category(), random_page_size()),
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
        {{Region, users, NickName, rating}, grb_gcounter}
    }],
    {ok, Tx} = start_transaction(S0),

    %% Read up to limit from the list of comments to this user, and while it completes read the user object
    {ok, Req} = grb_client:send_read_operation(Coord, Tx, CommentIndexKey, ?gset_limit_op(random_page_size())),
    {ok, _, Tx1} = grb_client:read_key_snapshots(Coord, Tx, UserKeys),
    {ok, CommentIds, Tx2} = grb_client:receive_read_operation(Coord, Tx1, CommentIndexKey, Req),

    %% Comments written to this user are stored in the same region
    CommentKeys = lists:foldl(
        fun({CommentRegion, comments, CommentId}, Acc)
            when CommentRegion =:= Region ->
                [
                    {{CommentRegion, comments, CommentId, from}, grb_lww},
                    {{CommentRegion, comments, CommentId, rating}, grb_lww},
                    {{CommentRegion, comments, CommentId, text}, grb_lww}
                    | Acc
                ]
        end,
        [],
        CommentIds
    ),
    {ok, _, Tx3} = grb_client:read_key_snapshots(Coord, Tx2, CommentKeys),
    {ok, CVC} = grb_client:commit(Coord, Tx3),
    {ok, incr_tx_id(S0#state{last_cvc=CVC})};

run(view_bid_history, _, _, S0=#state{coord_state=Coord}) ->
    {Region, ItemId} = random_item(S0),
    ItemNameKey = {Region, items, ItemId, name},
    BidIndexKey = {Region, bids_item, {Region, items, ItemId}},
    {ok, Tx} = start_transaction(S0),
    {ok, Req} = grb_client:send_read_operation(Coord, Tx, BidIndexKey, ?gset_limit_op(random_page_size())),
    {ok, _, Tx1} = grb_client:read_key_snapshot(Coord, Tx, ItemNameKey, grb_lww),
    {ok, BidIds, Tx2} = grb_client:receive_read_operation(Coord, Tx1, BidIndexKey, Req),
    BidKeys = lists:foldl(
        fun({{BidRegion, bids, BidId}, _BidderKey}, Acc)
            when BidRegion =:= Region->
                [
                    {{BidRegion, bids, BidId, amount}, grb_lww},
                    {{BidRegion, bids, BidId, quantity}, grb_lww}
                    | Acc
                ]
        end,
        [],
        BidIds
    ),
    {ok, _, Tx3} = grb_client:read_key_snapshots(Coord, Tx2, BidKeys),
    {ok, CVC} = grb_client:commit(Coord, Tx3),
    {ok, incr_tx_id(S0#state{last_cvc=CVC})};

run(buy_now, _, _, S0=#state{coord_state=Coord}) ->
    {ItemRegion, ItemId} = random_item(S0),
    {UserRegion, Nickname} = random_user(S0),
    ItemKeys = [{
        {{ItemRegion, items, ItemId, name}, grb_lww},
        {{ItemRegion, items, ItemId, seller}, grb_lww},
        {{ItemRegion, items, ItemId, buy_now}, grb_lww}
    }],
    {ok, Tx} = start_transaction(S0),
    S1 = case try_auth(Coord, Tx, UserRegion, Nickname, Nickname) of
        {error, _} ->
            %% bail out, no need to clean up anything
            S0;
         {ok, Tx1} ->
             {ok, _, Tx2} = grb_client:read_key_snapshots(Coord, Tx1, ItemKeys),
             {ok, CVC} = grb_client:commit(Coord, Tx2),
             S0#state{last_cvc=CVC}
    end,
    {ok, incr_tx_id(S1)};

run(store_buy_now, _, _, S) ->
    %% fixme(borja): Implement
    {ok, S};

run(put_bid, _, _, S0=#state{coord_state=Coord}) ->
    {ItemRegion, ItemId} = random_item(S0),
    {UserRegion, Nickname} = random_user(S0),
    ItemKeys = [{
        {{ItemRegion, items, ItemId, name}, grb_lww},
        {{ItemRegion, items, ItemId, seller}, grb_lww},
        {{ItemRegion, items, ItemId, max_bid}, grb_maxtuple},
        {{ItemRegion, items, ItemId, bids_number}, grb_gcounter},
        {{ItemRegion, items, ItemId, initial_price}, grb_lww}
    }],
    {ok, Tx} = start_transaction(S0),
    S1 = case try_auth(Coord, Tx, UserRegion, Nickname, Nickname) of
        {error, _} ->
            %% bail out, no need to clean up anything
            S0;
         {ok, Tx1} ->
             {ok, _, Tx2} = grb_client:read_key_snapshots(Coord, Tx1, ItemKeys),
             {ok, CVC} = grb_client:commit(Coord, Tx2),
             S0#state{last_cvc=CVC}
    end,
    {ok, incr_tx_id(S1)};

run(store_bid, _, _, S) ->
    %% fixme(borja): Implement
    {ok, S};

run(put_comment, _, _, S0=#state{coord_state=Coord}) ->
    {FromRegion, FromNickname} = random_user(S0),
    {_, ToNickname} = random_different_user(FromRegion, FromNickname, S0),
    {ItemRegion, ItemId} = random_item(S0),
    Keys = [{
        {ToNickname, grb_lww},
        {{ItemRegion, items, ItemId, name}, grb_lww}
    }],
    {ok, Tx} = start_transaction(S0),
    S1 = case try_auth(Coord, Tx, FromRegion, FromNickname, FromNickname) of
        {error, _} ->
            %% bail out, no need to clean up anything
            S0;
         {ok, Tx1} ->
             {ok, _, Tx2} = grb_client:read_key_snapshots(Coord, Tx1, Keys),
             {ok, CVC} = grb_client:commit(Coord, Tx2),
             S0#state{last_cvc=CVC}
    end,
    {ok, incr_tx_id(S1)};

run(store_comment, _, _, S) ->
    %% fixme(borja): Implement
    {ok, S};

run(select_category_to_sell_item, _, _, S0=#state{coord_state=Coord}) ->
    %% same as browse_categories, but with auth step
    {UserRegion, Nickname} = random_user(S0),
    {ok, Tx} = start_transaction(S0),
    S1 = case try_auth(Coord, Tx, UserRegion, Nickname, Nickname) of
        {error, _} ->
            %% bail out, no need to clean up anything
            S0;
         {ok, Tx1} ->
             {ok, _, Tx2} = grb_client:read_key_snapshot(Coord, Tx1, {?global_indices, all_categories}, grb_gset),
             {ok, CVC} = grb_client:commit(Coord, Tx2),
             S0#state{last_cvc=CVC}
    end,
    {ok, incr_tx_id(S1)};

run(register_item, _, _, S0=#state{coord_state=Coord}) ->
    Category = random_category(),
    {Region, SellerNickname} = random_user(S0),
    Seller = {Region, users, SellerNickname},
    {ItemId, S1} = gen_new_itemid(S0),
    ItemKey = {Region, items, ItemId},
    InitialPrice = rand:uniform(100),
    Quantity = safe_uniform(hook_rubis:get_rubis_prop(item_max_quantity)),
    Description = case safe_uniform(hook_rubis:get_rubis_prop(item_description_max_len)) of
        N when N >= 1 -> random_binary(N);
        _ -> <<>>
    end,
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
        {{Region, items, ItemId, name}, grb_crdt:make_op(grb_lww, ItemId)},
        {{Region, items, ItemId, description}, grb_crdt:make_op(grb_lww, Description)},
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

run(about_me, _, _, S) ->
    %% fixme(borja): Implement
    {ok, S};

run(get_auctions_ready_for_close, _, _, S=#state{coord_state=Coord}) ->
    Region = random_region(),
    Category = random_category(),

    {ok, Tx} = start_transaction(S),
    {ok, ItemIds, Tx1} = grb_client:read_key_operation(Coord, Tx,
                                                       {Region, items_region_category, Category},
                                                       ?gset_limit_op(random_page_size())),

    KeyRequests = lists:foldl(
        fun({ItemRegion, items, ItemId}, Reqs)
            when ItemRegion =:= Region ->
                %% Here we read if they're open and check that the those items are over the limit
                ClosedKey = {ItemRegion, items, ItemId, closed},
                NBidsKey = {ItemRegion, items, ItemId, bids_number},
                {ok, Req1} = grb_client:send_read_key(Coord, Tx1, ClosedKey, grb_lww),
                {ok, Req2} = grb_client:send_read_key(Coord, Tx1, NBidsKey, grb_gcounter),
                [ {ClosedKey, Req1}, {NBidsKey, Req2} | Reqs]
        end,
        [],
        ItemIds
    ),

    Tx2 = lists:foldl(fun({Key, Req}, TxAcc0) ->
        %% We don't actually care about the result, so discard it
        {ok, _, TxAcc} = grb_client:receive_read_key(Coord, TxAcc0, Key, Req),
        TxAcc
    end, Tx1, KeyRequests),

    {ok, CVC} = grb_client:commit(Coord, Tx2),
    {ok, incr_tx_id(S#state{last_cvc=CVC})};

run(close_auction, _, _, S) ->
    %% fixme(borja): Implement
    {ok, S}.

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

-spec try_auth(Coord :: grb_client:coord(),
              Tx0 ::grb_client:tx(),
              Region :: binary(),
              Nickname :: binary(),
              Password :: binary()) -> {ok, grb_client:tx()} | {error, grb_client:tx()}.

try_auth(Coord, Tx0, Region, NickName, Password) ->
    {ok, OtherPassword, Tx1} = grb_client:read_key_snapshot(Coord, Tx0, {Region, users, NickName, password}, grb_lww),
    if
        Password =:= OtherPassword ->
            {ok, Tx1};
        true ->
            {error, Tx1}
    end.

-spec register_user_loop(state()) -> state().
register_user_loop(S0=#state{coord_state=Coord, retry_until_commit=Retry}) ->
    Region = random_region(),
    {NickName, S1} = gen_new_nickname(S0),
    {ok, Tx} = start_transaction(S1),
    case register_user(Coord, Tx, Region, NickName) of
        {abort, _} when Retry =:= true ->
            register_user_loop(incr_tx_id(S1));

        {ok, CVC} ->
            incr_tx_id(S1#state{last_cvc=CVC, last_generated_user={Region, NickName}});

        _ ->
            %% todo(borja): What to do here? (user_taken)
            {ok, incr_tx_id(S1)}
    end.

-spec register_user(grb_client:coord(), grb_client:tx(), binary(), binary()) -> {ok, _} | {abort, _} | {error, user_taken}.
register_user(Coord, Tx, Region, Nickname) ->
    Updates = [{
        {Nickname, grb_crdt:make_op(grb_lww, Nickname)},
        {{Region, users, Nickname}, grb_crdt:make_op(grb_lww, Nickname)},
        {{Region, users, Nickname, name}, grb_crdt:make_op(grb_lww, Nickname)},
        {{Region, users, Nickname, lastname}, grb_crdt:make_op(grb_lww, Nickname)},
        {{Region, users, Nickname, password}, grb_crdt:make_op(grb_lww, Nickname)},
        {{Region, users, Nickname, email}, grb_crdt:make_op(grb_lww, Nickname)},
        {{Region, users, Nickname, rating}, grb_crdt:make_op(grb_gcounter, 0)}
    }],
    {ok, Index, Tx1} = grb_client:read_key_snapshot(Coord, Tx, Nickname, grb_lww),
    case Index of
        <<>> ->
            %% username not claimed, send our operations
            {ok, Tx2} = grb_client:send_key_operations(Coord, Tx1, Updates),
            grb_client:commit_red(Coord, Tx2, ?register_user_label);
        _ ->
            %% let the caller choose what to do
            {error, user_taken}
    end.

%% Get all non-closed items (up to page size) belonging to a category, and with sellers in region, up to `Limit`
-spec search_items_in_region_category(grb_client:coord(), grb_client:tx(), binary(), binary(), pos_integer()) -> grb_client:rvc().
search_items_in_region_category(Coord, Tx, Region, Category, Limit) ->
    {ok, ItemIds, Tx1} = grb_client:read_key_operation(Coord, Tx,
                                                       {Region, items_region_in_category, Category},
                                                       ?gset_limit_op(Limit)),
    %% Now check how many of those are still open
    ItemKeyTypes = [ { {ItemRegion, items, ItemId, closed}, grb_lww} || {ItemRegion, items, ItemId} <- ItemIds ],
    {ok, Items, Tx2} = grb_client:read_key_snapshots(Coord, Tx1, ItemKeyTypes),
    %% For those that are open, read some information
    OpenKeyTypes = maps:fold(fun
        (_, true, Acc) -> Acc;
        ({R, T, Id}, false, Acc) ->
            [
                { {R, T, Id, name}, grb_lww},
                { {R, T, Id, max_id}, grb_maxtuple},
                { {R, T, Id, bids_number}, grb_gcounter},
                { {R, T, Id, initial_price}, grb_lww}
                | Acc
            ]
    end, [], Items),
    {ok, _, Tx3} = grb_client:read_key_snapshots(Coord, Tx2, OpenKeyTypes),
    grb_client:commit(Coord, Tx3).

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

-spec random_different_user(binary(), binary(), state()) -> {Region :: binary(), Nickname :: binary()}.
random_different_user(Region, Nickname, S) ->
    case random_user(S) of
        {Region, Nickname} ->
            random_different_user(Region, Nickname, S);

        Other ->
            Other
    end.

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

-spec random_page_size() -> non_neg_integer().
random_page_size() ->
    safe_uniform(hook_rubis:get_rubis_prop(result_page_size)).

-spec safe_uniform(pos_integer()) -> pos_integer().
safe_uniform(0) -> 0;
safe_uniform(X) when X >= 1 -> rand:uniform(X).

-spec random_binary(Size :: non_neg_integer()) -> binary().
random_binary(N) ->
    list_to_binary(random_string(N)).

-spec random_string(Size :: non_neg_integer()) -> string().
random_string(N) ->
    Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890",
    lists:foldl(fun(_, Acc) ->
        [lists:nth(rand:uniform(length(Chars)), Chars)] ++ Acc
    end, [], lists:seq(1, N)).
