-module(rubis_bench_strong).

%% Ignore xref for all lasp bench drivers
-ignore_xref([new/1, run/4, terminate/2]).

-export([new/1,
         run/4,
         terminate/2]).

-include("lasp_bench.hrl").

-define(all_conflict_label, <<"default">>).

%% How often do we pick the latest generated item / autor, etc
-define(REUSE_GENERATED_PROB, 0.25).

-define(gset_limit_op(__S), grb_crdt:wrap_op(grb_gset, grb_gset:limit_op(__S))).

-record(state, {
    worker_id :: non_neg_integer(),
    local_ip_str :: list(),
    last_cvc :: pvc_vclock:vc(),
    retry_until_commit :: boolean(),
    retry_on_bad_precondition :: boolean(),
    coord_state :: grb_client:coord(),

    %% Reusing commit vector for certain transactions
    red_reuse_cvc :: boolean(),

    %% Benchmark state
    item_count = 1 :: pos_integer(),
    comment_count = 1 :: pos_integer(),
    buy_now_count = 1 :: pos_integer(),
    bid_count = 1 :: pos_integer(),

    last_generated_user = undefined :: {Region :: binary(), Name :: binary()} | undefined,
    last_generated_item = undefined :: {Region :: binary(), Id :: binary()} | undefined
}).
-type state() :: #state{}.

new(WorkerId) ->
    {ok, CoordState} = hook_rubis:make_coordinator(WorkerId),
    LocalIp = hook_rubis:get_config(local_ip),

    RetryUntilCommit = lasp_bench_config:get(retry_aborts, true),
    RetryOnBadData = lasp_bench_config:get(retry_on_bad_precondition, true),

    RedReuseCVC = lasp_bench_config:get(red_reuse_cvc, true),

    RandSeed = lasp_bench_config:get(worker_seed, os:timestamp()),
    _ = rand:seed(exsss, RandSeed),

    State = #state{worker_id=WorkerId,
                   local_ip_str=inet:ntoa(LocalIp),
                   last_cvc=pvc_vclock:new(),
                   retry_until_commit=RetryUntilCommit,
                   retry_on_bad_precondition=RetryOnBadData,
                   red_reuse_cvc=RedReuseCVC,
                   coord_state=CoordState},

    {ok, State}.

retry_loop(State, Fun, Operation) ->
    retry_loop(Operation, State, Fun, 0).

retry_loop(Operation,
           S0=#state{retry_until_commit=RetryCommit,
                     retry_on_bad_precondition=RetryData}, Fun, _Aborted0) ->

    Start = os:timestamp(),
    {ok, Tx} = start_red_transaction(S0),
    {Res, State} = Fun(S0, Tx),
    ElapsedUs = erlang:max(0, timer:now_diff(os:timestamp(), Start)),
    case Res of
        {ok, CVC} ->
            {ok, ElapsedUs, 0, State#state{last_cvc=CVC}};

        {error, _} when RetryData ->
            lasp_bench_stats:op_complete({Operation, Operation}, {error, precondition}, ignore),
            retry_loop(Operation, State, Fun, 0);

        {abort, _} when RetryCommit ->
            lasp_bench_stats:op_complete({Operation, Operation}, {error, abort}, ignore),
            retry_loop(Operation, State, Fun, 0);

        {error, _} ->
            lasp_bench_stats:op_complete({Operation, Operation}, {error, precondition}, ignore),
            {ok, ElapsedUs, 0, State};

        {abort, _}=Abort ->
            {error, Abort, State}
    end.

run(register_user, _, _, State) ->
    register_user_loop(State);

run(browse_categories, _, _, State) ->
    retry_loop(State, fun(S=#state{coord_state=Coord}, Tx) ->
        {ok, _, Tx1} = grb_client:read_key_snapshot(Coord, Tx, {hook_rubis:random_global_index(Coord), all_categories}, grb_gset),
        {commit_red(Coord, Tx1), S}
    end, browse_categories);

run(search_items_in_category, _, _, State) ->
    retry_loop(State, fun(S=#state{coord_state=Coord}, Tx) ->
        Res = search_items_in_region_category(Coord, Tx, random_region(), random_category(), random_page_size()),
        {Res, S}
    end, search_items_in_category);

run(browse_regions, _, _, State) ->
    retry_loop(State, fun(S=#state{coord_state=Coord}, Tx) ->
        {ok, _, Tx1} = grb_client:read_key_snapshot(Coord, Tx, {hook_rubis:random_global_index(Coord), all_regions}, grb_gset),
        {commit_red(Coord, Tx1), S}
    end, browse_regions);

run(browse_categories_in_region, _, _, State) ->
    retry_loop(State, fun(S=#state{coord_state=Coord}, Tx) ->
        {ok, _, Tx1} = grb_client:read_key_snapshot(Coord, Tx, {hook_rubis:random_global_index(Coord), all_categories}, grb_gset),
        {commit_red(Coord, Tx1), S}
    end, browse_categories_in_region);

run(search_items_in_region, _, _, State) ->
    retry_loop(State, fun(S=#state{coord_state=Coord}, Tx) ->
        Res = search_items_in_region_category(Coord, Tx, random_region(), random_category(), random_page_size()),
        {Res, S}
    end, search_items_in_region);

run(view_item, _, _, State) ->
    retry_loop(State, fun(S=#state{coord_state=Coord}, Tx) ->
        {Region, ItemId} = random_item(S),
        {ok, _, Tx1} = grb_client:read_key_snapshots(Coord, Tx, [
            {{Region, items, ItemId, seller}, grb_lww},
            {{Region, items, ItemId, category}, grb_lww},
            {{Region, items, ItemId, initial_price}, grb_lww},
            {{Region, items, ItemId, quantity}, grb_lww},
            {{Region, items, ItemId, reserve_price}, grb_lww},
            {{Region, items, ItemId, buy_now}, grb_lww},
            {{Region, items, ItemId, closed}, grb_lww}
        ]),
        {commit_red(Coord, Tx1), S}
    end, view_item);

run(view_user_info, _, _, State) ->
    retry_loop(State, fun(S=#state{coord_state=Coord}, Tx) ->
        {Region, NickName} = random_user(S),
        CommentIndexKey = {Region, comments_to_user, {Region, users, NickName}},
        UserKeys = [
            %% User object
            {{Region, users, NickName, name}, grb_lww},
            {{Region, users, NickName, lastname}, grb_lww},
            {{Region, users, NickName, rating}, grb_gcounter}
        ],

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
        {commit_red(Coord, Tx3), S}
    end, view_user_info);

run(view_bid_history, _, _, State) ->
    retry_loop(State, fun(S=#state{coord_state=Coord}, Tx) ->
        {Region, ItemId} = random_item(S),
        ItemNameKey = {Region, items, ItemId, name},
        BidIndexKey = {Region, bids_item, {Region, items, ItemId}},
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
        {commit_red(Coord, Tx3), S}
    end, view_bid_history);

run(buy_now, _, _, State) ->
    retry_loop(State, fun(S=#state{coord_state=Coord}, Tx) ->
        {ItemRegion, ItemId} = random_item(S),
        {UserRegion, Nickname} = random_user(S),
        ItemKeys = [
            {{ItemRegion, items, ItemId, name}, grb_lww},
            {{ItemRegion, items, ItemId, seller}, grb_lww},
            {{ItemRegion, items, ItemId, buy_now}, grb_lww}
        ],
        case try_auth(Coord, Tx, UserRegion, Nickname, Nickname) of
            {error, _} ->
                {{error, auth}, S};

            {ok, Tx1} ->
                {ok, _, Tx2} = grb_client:read_key_snapshots(Coord, Tx1, ItemKeys),
                {commit_red(Coord, Tx2), S}
        end
    end, buy_now);

run(store_buy_now, _, _, S) ->
    store_buy_now_loop(S);

run(put_bid, _, _, State) ->
    retry_loop(State, fun(S=#state{coord_state=Coord}, Tx) ->
        {ItemRegion, ItemId} = random_item(S),
        {UserRegion, Nickname} = random_user(S),
        ItemKeys = [
            {{ItemRegion, items, ItemId, name}, grb_lww},
            {{ItemRegion, items, ItemId, seller}, grb_lww},
            {{ItemRegion, items, ItemId, max_bid}, grb_maxtuple},
            {{ItemRegion, items, ItemId, bids_number}, grb_gcounter},
            {{ItemRegion, items, ItemId, initial_price}, grb_lww}
        ],
        case try_auth(Coord, Tx, UserRegion, Nickname, Nickname) of
            {error, _} ->
                {{error, auth}, S};

             {ok, Tx1} ->
                 {ok, _, Tx2} = grb_client:read_key_snapshots(Coord, Tx1, ItemKeys),
                 {commit_red(Coord, Tx2), S}
        end
    end, put_bid);

run(store_bid, _, _, S) ->
    store_bid_loop(S);

run(put_comment, _, _, State) ->
    retry_loop(State, fun(S=#state{coord_state=Coord}, Tx) ->
        {FromRegion, FromNickname} = random_user(S),
        {_, ToNickname} = random_different_user(FromRegion, FromNickname),
        {ItemRegion, ItemId} = random_item(S),
        Keys = [
            {ToNickname, grb_lww},
            {{ItemRegion, items, ItemId, name}, grb_lww}
        ],
        case try_auth(Coord, Tx, FromRegion, FromNickname, FromNickname) of
            {error, _} ->
                {{error, auth}, S};

             {ok, Tx1} ->
                 {ok, _, Tx2} = grb_client:read_key_snapshots(Coord, Tx1, Keys),
                 {commit_red(Coord, Tx2), S}
        end
    end, put_comment);

run(store_comment, _, _, State) ->
    retry_loop(State, fun(S=#state{coord_state=Coord}, Tx) ->
        {FromRegion, FromNickname} = random_user(S),
        {ToRegion, ToNickname} = random_different_user(FromRegion, FromNickname),
        {ItemRegion, ItemId} = random_item(S),

        SenderKey = {FromRegion, users, FromNickname},
        RecipientKey = {ToRegion, users, ToNickname},
        ItemKey = {ItemRegion, items, ItemId},

        {CommentId, S1} = gen_new_comment_id(S),
        CommentKey = {ToRegion, comments, CommentId},
        Rating = random_rating(),
        CommentText = random_binary(safe_uniform(hook_rubis:get_rubis_prop(comment_max_len))),

        Updates = [
            %% Comment Object
            { {ToRegion, comments, CommentId, from}, grb_crdt:make_op(grb_lww, SenderKey)},
            { {ToRegion, comments, CommentId, to}, grb_crdt:make_op(grb_lww, RecipientKey)},
            { {ToRegion, comments, CommentId, on_item}, grb_crdt:make_op(grb_lww, ItemKey)},
            { {ToRegion, comments, CommentId, rating}, grb_crdt:make_op(grb_lww, Rating)},
            { {ToRegion, comments, CommentId, text}, grb_crdt:make_op(grb_lww, CommentText)},
            %% Append to COMMENTS_to_user index
            { {ToRegion, comments_to_user, RecipientKey}, grb_crdt:make_op(grb_gset, CommentKey)},
            %% Update recipient rating
            { {ToRegion, users, ToNickname, rating}, grb_crdt:make_op(grb_gcounter, Rating)}
        ],

        %% Claim the item key, read/write so we don't do any blind updates, should get back the same value
        {ok, _, Tx1} = grb_client:update_operation(Coord, Tx, CommentKey, grb_crdt:make_op(grb_lww, CommentKey)),
        {ok, Tx2} = grb_client:send_key_operations(Coord, Tx1, Updates),
        {commit_red(Coord, Tx2), S1}
    end, store_comment);

run(select_category_to_sell_item, _, _, State) ->
    retry_loop(State, fun(S=#state{coord_state=Coord}, Tx) ->
        %% same as browse_categories, but with auth step
        {UserRegion, Nickname} = random_user(S),
        case try_auth(Coord, Tx, UserRegion, Nickname, Nickname) of
            {error, _} ->
                {{error, auth}, S};
             {ok, Tx1} ->
                 {ok, _, Tx2} = grb_client:read_key_snapshot(Coord, Tx1, {hook_rubis:random_global_index(Coord), all_categories}, grb_gset),
                 {commit_red(Coord, Tx2), S}
        end
    end, select_category_to_sell_item);

run(register_item, _, _, State) ->
    retry_loop(State, fun(S=#state{coord_state=Coord}, Tx) ->
        Category = random_category(),
        {Region, SellerNickname} = random_user(S),
        Seller = {Region, users, SellerNickname},
        {ItemId, S1} = gen_new_item_id(S),
        ItemKey = {Region, items, ItemId},
        InitialPrice = rand:uniform(100),
        Quantity = safe_uniform(hook_rubis:get_rubis_prop(item_max_quantity)),
        Description = case safe_uniform(hook_rubis:get_rubis_prop(item_description_max_len)) of
            N when N >= 1 -> random_binary(N);
            _ -> <<>>
        end,
        ReservePrice = InitialPrice + begin
            case (rand:uniform(100) =< hook_rubis:get_rubis_prop(item_reserve_percentage)) of
                true -> rand:uniform(10);
                false -> 0
            end
        end,
        BuyNow = begin
            case (rand:uniform(100) =< hook_rubis:get_rubis_prop(item_buy_now_percentage)) of
                true -> rand:uniform(10);
                false -> 0
            end
        end,
        Updates = [
            %% Item Updates
            {{Region, items, ItemId, name}, grb_crdt:make_op(grb_lww, ItemId)},
            {{Region, items, ItemId, description}, grb_crdt:make_op(grb_lww, Description)},
            {{Region, items, ItemId, seller}, grb_crdt:make_op(grb_lww, {Region, users, Seller})},
            {{Region, items, ItemId, category}, grb_crdt:make_op(grb_lww, Category)},
            {{Region, items, ItemId, max_bid}, grb_crdt:make_op(grb_maxtuple, {-1, <<>>})},
            {{Region, items, ItemId, initial_price}, grb_crdt:make_op(grb_lww, InitialPrice)},
            {{Region, items, ItemId, quantity}, grb_crdt:make_op(grb_lww, Quantity)},
            {{Region, items, ItemId, reserve_price}, grb_crdt:make_op(grb_lww, ReservePrice)},
            {{Region, items, ItemId, buy_now}, grb_crdt:make_op(grb_lww, BuyNow)},
            {{Region, items, ItemId, closed}, grb_crdt:make_op(grb_lww, false)},
            %% append to ITEMS_seller index
            {{Region, items_seller, Seller}, grb_crdt:make_op(grb_gset, ItemKey)},
            %% append to ITEMS_region_category index
            {{Region, items_region_category, Category}, grb_crdt:make_op(grb_gset, ItemKey)}
        ],
        %% Claim the item key, read/write so we don't do any blind updates, should get back the same value
        {ok, _, Tx1} = grb_client:update_operation(Coord, Tx, ItemKey, grb_crdt:make_op(grb_lww, ItemKey)),
        {ok, Tx2} = grb_client:send_key_operations(Coord, Tx1, Updates),
        {commit_red(Coord, Tx2), S1#state{last_generated_item={Region, ItemId}}}
    end, register_item);

run(about_me, _, _, State) ->
    retry_loop(State, fun(S=#state{coord_state=Coord}, Tx) ->
        {UserRegion, Nickname} = random_user(S),
        case try_auth(Coord, Tx, UserRegion, Nickname, Nickname) of
            {error, _} ->
                {{error, auth}, S};

            {ok, Tx1} ->
                PageSize = random_page_size(),
                UserProfileReads = [
                    { {UserRegion, users, Nickname, name}, grb_lww },
                    { {UserRegion, users, Nickname, lastname}, grb_lww },
                    { {UserRegion, users, Nickname, email}, grb_lww },
                    { {UserRegion, users, Nickname, rating}, grb_gcounter }
                ],
                %% We can read this at once, everything is in the same partition
                {ok, Req} = grb_client:send_read_partition(Coord, Tx1, UserProfileReads),

                UserKey = {UserRegion, users, Nickname},
                SoldItemsIdxKey = {UserRegion, items_seller, UserKey},
                WonItemsIdxKey = {UserRegion, wins_user, UserKey},
                BuyNowsIdxKey = {UserRegion, buy_nows_buyer, UserKey},
                CommentIdxKey = {UserRegion, comments_to_user, UserKey},
                IndexReadOps = [
                    %% This already contains bid amount, so no need to do another roundtrip
                    { {UserRegion, bids_user, UserKey}, ?gset_limit_op(PageSize) },
                    %% Read items sold by this user. Later, we will read info from here
                    { SoldItemsIdxKey, ?gset_limit_op(PageSize) },
                    %% Read items won by this user. This already contains everything we want (item seller, paid amount, etc)
                    { WonItemsIdxKey, ?gset_limit_op(PageSize) },
                    %% BUY_nows put by this user. Later, we will read info from here
                    { BuyNowsIdxKey, ?gset_limit_op(PageSize) },
                    %% Comments send to this user. Later we will read info from here
                    { CommentIdxKey, ?gset_limit_op(PageSize) }
                ],

                {ok, IdxResults, Tx2} = grb_client:read_key_operations(Coord, Tx1, IndexReadOps),
                {ok, _UserInfo, Tx3} = grb_client:receive_read_partition(Coord, Tx2, Req),
                #{
                    SoldItemsIdxKey := SoldItemIds,
                    BuyNowsIdxKey := BoughtIds,
                    CommentIdxKey := CommentIds
                } = IdxResults,

                %% Read info from the items we sell
                Reqs0 = lists:foldl(fun({_, _, ItemId}, Reqs) ->
                    Key0 = {UserRegion, items, ItemId, name},
                    Key1 = {UserRegion, items, ItemId, bids_number},
                    Key2 = {UserRegion, items, ItemId, max_bid},
                    {ok, R0} = grb_client:send_read_key(Coord, Tx3, Key0, grb_lww),
                    {ok, R1} = grb_client:send_read_key(Coord, Tx3, Key1, grb_gcounter),
                    {ok, R2} = grb_client:send_read_key(Coord, Tx3, Key2, grb_maxtuple),
                    [ {Key0, R0}, {Key1, R1}, {Key2, R2} | Reqs ]
                end, [], SoldItemIds),

                %% Read info from the items we bought
                Reqs1 = lists:foldl(fun({_, {BoughtRegion, _, BoughtItemId}}, Reqs) ->
                    Key0 = {BoughtRegion, items, BoughtItemId, name},
                    Key1 = {BoughtRegion, items, BoughtItemId, seller},
                    Key2 = {BoughtRegion, items, BoughtItemId, description},
                    {ok, R0} = grb_client:send_read_key(Coord, Tx3, Key0, grb_lww),
                    {ok, R1} = grb_client:send_read_key(Coord, Tx3, Key1, grb_lww),
                    {ok, R2} = grb_client:send_read_key(Coord, Tx3, Key2, grb_lww),
                    [ {Key0, R0}, {Key1, R1}, {Key2, R2} | Reqs ]
                end, Reqs0, BoughtIds),

                Reqs2 = lists:foldl(fun({_, _, CommentId}, Reqs) ->
                    Key0 = {UserRegion, comments, CommentId, from},
                    Key1 = {UserRegion, comments, CommentId, text},
                    Key2 = {UserRegion, comments, CommentId, rating},
                    {ok, R0} = grb_client:send_read_key(Coord, Tx3, Key0, grb_lww),
                    {ok, R1} = grb_client:send_read_key(Coord, Tx3, Key1, grb_lww),
                    {ok, R2} = grb_client:send_read_key(Coord, Tx3, Key2, grb_lww),
                    [ {Key0, R0}, {Key1, R1}, {Key2, R2} | Reqs ]
                end, Reqs1, CommentIds),

                Tx4 = lists:foldl(fun({Key, KeyReq}, TxAcc0) ->
                    %% We don't actually care about the result, so discard it
                    {ok, _, TxAcc} = grb_client:receive_read_key(Coord, TxAcc0, Key, KeyReq),
                    TxAcc
                end, Tx3, Reqs2),

                {commit_red(Coord, Tx4), S}
        end
    end, about_me);

run(get_auctions_ready_for_close, _, _, State) ->
    retry_loop(State, fun(S=#state{coord_state=Coord}, Tx) ->
        Region = random_region(),
        Category = random_category(),

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

        {commit_red(Coord, Tx2), S}
    end, get_auctions_ready_for_close);

run(close_auction, _, _, S) ->
    close_auction_loop(S).

terminate(Reason, #state{worker_id=Id}) ->
    ?ERROR("Worker ~p (id ~b) terminated with reason ~p", [self(), Id, Reason]),
    hook_rubis:stop(),
    ok.

%%%===================================================================
%%% Transaction Utils
%%%===================================================================

-spec start_red_transaction(state()) -> {ok, grb_client:tx()}.
start_red_transaction(#state{worker_id=WorkerId,
                             coord_state=CoordState,
                             last_cvc=LastVC,
                             red_reuse_cvc=Reuse}) ->

    SVC = if Reuse -> LastVC; true -> grb_vclock:new() end,
    {ok, Tx} = grb_client:start_transaction(CoordState,
                                            hook_grb:next_transaction_id(WorkerId),
                                            SVC),
    hook_grb:trace_msg("[~b] started ~w: ~w~n", [WorkerId, element(2, Tx), element(3, Tx)]),
    {ok, Tx}.

-spec commit_red(
    Coord :: grb_client:coordd(),
    Tx :: grb_client:tx()
) -> {ok, CVC :: grb_client:rvc()} | {abort, Reason :: term()}.

commit_red(Coord, Tx) ->
    WorkerId = element(7, Coord), %% hack
    case grb_client:commit_red(Coord, Tx, ?all_conflict_label) of
        {ok, CVC} ->
            hook_grb:trace_msg("[~b] committed ~w: ~w~n", [WorkerId, element(2, Tx), CVC]),
            {ok, CVC};
        {abort, _}=Err ->
            hook_grb:trace_msg("[~b] aborted ~w: ~w~n", [WorkerId, element(2, Tx), Err]),
            Err
    end.

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

-spec register_user_loop(state()) -> {ok, integer(), integer(), state()} | {error, term(), state()}.
register_user_loop(State) ->
    register_user_loop(State, 0).

register_user_loop(State=#state{coord_state=Coord, retry_until_commit=RetryAbort,
                                retry_on_bad_precondition=RetryData}, _Aborted0) ->
    Start = os:timestamp(),
    Region = random_region(),
    NickName = gen_new_nickname(),
    {ok, Tx} = start_red_transaction(State),
    Res = register_user(Coord, Tx, Region, NickName),
    ElapsedUs = erlang:max(0, timer:now_diff(os:timestamp(), Start)),
    case Res of
        {ok, CVC} ->
            {ok, ElapsedUs, 0,
                State#state{last_cvc=CVC, last_generated_user={Region, NickName}}};

        {error, _} when RetryData ->
            lasp_bench_stats:op_complete({register_user, register_user}, {error, precondition}, ignore),
            register_user_loop(State, 0);

        {abort, _} when RetryAbort ->
            lasp_bench_stats:op_complete({register_user, register_user}, {error, abort}, ignore),
            register_user_loop(State, 0);

        {error, _} ->
            lasp_bench_stats:op_complete({register_user, register_user}, {error, precondition}, ignore),
            {ok, ElapsedUs, 0, State};

        {abort, _}=Abort ->
            {error, Abort, State}
    end.

-spec register_user(grb_client:coord(), grb_client:tx(), binary(), binary()) -> {ok, _} | {abort, _} | {error, user_taken}.
register_user(Coord, Tx, Region, Nickname) ->
    Updates = [
        {Nickname, grb_crdt:make_op(grb_lww, Nickname)},
        {{Region, users, Nickname, name}, grb_crdt:make_op(grb_lww, Nickname)},
        {{Region, users, Nickname, lastname}, grb_crdt:make_op(grb_lww, Nickname)},
        {{Region, users, Nickname, password}, grb_crdt:make_op(grb_lww, Nickname)},
        {{Region, users, Nickname, email}, grb_crdt:make_op(grb_lww, Nickname)},
        {{Region, users, Nickname, rating}, grb_crdt:make_op(grb_gcounter, 0)}
    ],
    {ok, Index, Tx1} = grb_client:read_key_snapshot(Coord, Tx, Nickname, grb_lww),
    case Index of
        <<>> ->
            UserKey = {Region, users, Nickname},
            %% username not claimed, send our operations. Pay the price of reading once (no blind writes to Region)
            {ok, Req} = grb_client:send_key_update(Coord, Tx1, UserKey, grb_crdt:make_op(grb_lww, Nickname)),
            {ok, Tx2} = grb_client:send_key_operations(Coord, Tx1, Updates),
            {ok, _, Tx3} = grb_client:receive_key_update(Coord, Tx2, UserKey, Req),
            commit_red(Coord, Tx3);
        _ ->
            %% let the caller choose what to do
            {error, user_taken}
    end.

-spec store_buy_now_loop(state()) -> {ok, integer(), integer(), state()} | {error, term(), state()}.
store_buy_now_loop(State) ->
    store_buy_now_loop(State, 0).

store_buy_now_loop(S0=#state{coord_state=Coord, retry_until_commit=RetryAbort,
                             retry_on_bad_precondition=RetryData}, _Aborted0) ->

    Start = os:timestamp(),
    {ItemRegion, ItemId} = random_item(S0),
    {UserRegion, NickName} = random_user(S0),
    Qty = safe_uniform(hook_rubis:get_rubis_prop(buy_now_max_quantity)),
    {BuyNowId, S1} = gen_new_buynow_id(S0),
    {ok, Tx} = start_red_transaction(S1),
    Res = store_buy_now(Coord, Tx, {ItemRegion, items, ItemId}, {UserRegion, users, NickName}, BuyNowId, Qty),
    ElapsedUs = erlang:max(0, timer:now_diff(os:timestamp(), Start)),
    case Res of
        {ok, CVC} ->
            {ok, ElapsedUs, 0,
                S1#state{last_cvc=CVC}};

        {error, _} when RetryData ->
            lasp_bench_stats:op_complete({store_buy_now, store_buy_now}, {error, precondition}, ignore),
            store_buy_now_loop(S1, 0);

        {abort, _} when RetryAbort ->
            lasp_bench_stats:op_complete({store_buy_now, store_buy_now}, {error, abort}, ignore),
            store_buy_now_loop(S1, 0);

        {error, _} ->
            lasp_bench_stats:op_complete({store_buy_now, store_buy_now}, {error, precondition}, ignore),
            {ok, ElapsedUs, 0, S1};

        {abort, _}=Abort ->
            {error, Abort, S1}
    end.

-spec store_buy_now(grb_client:coord(), grb_client:tx(), _, _, binary(), non_neg_integer()) -> {ok, _} | {abort, _} | {error, bad_quantity}.
store_buy_now(Coord, Tx, ItemKey={ItemRegion, items, ItemId}, UserKey={UserRegion, _, _}, BuyNowId, Quantity) ->
    ItemQtyKey = {ItemRegion, items, ItemId, quantity},
    {ok, ItemQty, Tx1} = grb_client:read_key_snapshot(Coord, Tx, ItemQtyKey, grb_lww),
    if
        (not is_integer(ItemQty)) orelse Quantity > ItemQty ->
            {error, bad_quantity};

        true ->
            NewQty = ItemQty - Quantity,
            %% we already read from the item, so we can blind-update the quantity
            %% we can't do that at the user region, but we also don't want to read from the buy_now index (huge)
            %% we read from the user table so we only read a tiny amount of data (nickname)
            {ok, NickReq} = grb_client:send_read_key(Coord, Tx1, UserKey, grb_lww),
            %% While the previous request is in flight, send our updates
            BuyNowKey = {UserRegion, buy_nows, BuyNowId},
            {ok, Tx2} = grb_client:send_key_operations(Coord, Tx1, [
                { {UserRegion, buy_nows, BuyNowId, on_item}, grb_crdt:make_op(grb_lww, ItemKey) },
                { {UserRegion, buy_nows, BuyNowId, buyer}, grb_crdt:make_op(grb_lww, UserKey) },
                { {UserRegion, buy_nows, BuyNowId, quantity}, grb_crdt:make_op(grb_lww, Quantity) },
                %% Update BUY_NOWS_buyer index. Store the item key too, so we don't perform extra roundtrips
                { {UserRegion, buy_nows_buyer, UserKey}, grb_crdt:make_op(grb_gset, {BuyNowKey, ItemKey}) },
                { ItemQtyKey, grb_crdt:make_op(grb_lww, NewQty) }
            ]),
            %% now receive it, but ignore the data
            {ok, _, Tx3} = grb_client:receive_read_key(Coord, Tx2, UserKey, NickReq),
            commit_red(Coord, Tx3)
    end.

-spec store_bid_loop(state()) -> {ok, integer(), integer(), state()} | {error, term(), state()}.
store_bid_loop(State) ->
    store_bid_loop(State, 0).

store_bid_loop(S0=#state{coord_state=Coord, retry_until_commit=RetryAbort,
                         retry_on_bad_precondition=RetryData}, _Aborted0) ->

    Start = os:timestamp(),
    {ItemRegion, ItemId} = random_item(S0),
    {UserRegion, NickName} = random_user(S0),
    Qty = safe_uniform(hook_rubis:get_rubis_prop(buy_now_max_quantity)),
    Amount = safe_uniform(hook_rubis:get_rubis_prop(item_max_initial_price)),
    {BidId, S1} = gen_new_bid_id(S0),
    {ok, Tx} = start_red_transaction(S1),
    Res = store_bid(Coord, Tx, {ItemRegion, items, ItemId}, {UserRegion, users, NickName}, BidId, Amount, Qty),
    ElapsedUs = erlang:max(0, timer:now_diff(os:timestamp(), Start)),
    case Res of
        {ok, CVC} ->
            {ok, ElapsedUs, 0, S1#state{last_cvc=CVC}};

        {error, _} when RetryData ->
            lasp_bench_stats:op_complete({store_bid, store_bid}, {error, precondition}, ignore),
            store_bid_loop(S1, 0);

        {abort, _} when RetryAbort ->
            lasp_bench_stats:op_complete({store_bid, store_bid}, {error, abort}, ignore),
            store_bid_loop(S1, 0);

        {error, _} ->
            lasp_bench_stats:op_complete({store_bid, store_bid}, {error, precondition}, ignore),
            {ok, ElapsedUs, 0, S1};

        {abort, _}=Abort ->
            {error, Abort, S1}
    end.

-spec store_bid(grb_client:coord(), grb_client:tx(), _, _, binary(), non_neg_integer(), non_neg_integer()) -> {ok, _} | {abort, _} | {error, bad_data}.
store_bid(Coord, Tx, ItemKey={ItemRegion, items, ItemId}, UserKey={UserRegion, _, _}, BidId, Amount, Qty) ->
    ItemQtyKey = {ItemRegion, items, ItemId, quantity},
    ItemClosedKey = {ItemRegion, items, ItemId, closed},
    ItemPriceKey = {ItemRegion, items, ItemId, reserve_price},
    {ok, Data0, Tx1} = grb_client:read_key_snapshots(Coord, Tx, [{ItemQtyKey, grb_lww},
                                                                 {ItemClosedKey, grb_lww},
                                                                 {ItemPriceKey, grb_lww}]),

    #{ItemQtyKey := ItemQty, ItemClosedKey := IsClosed, ItemPriceKey := ItemPrice} = Data0,
    BadData = IsClosed orelse (Qty > ItemQty) orelse (Amount < ItemPrice),
    if
        BadData ->
            {error, bad_data};
        true ->
            %% Bids are colocated with the region. Since we read from that partition, we can send our operations,
            %% including updating the item. The update to BIDS_user is in another partition, so we should read from
            %% there before. The BIDS_user index is too big, so read from something else.
            {ok, Req} = grb_client:send_read_key(Coord, Tx1, UserKey, grb_lww),

            BidKey = {ItemRegion, bids, BidId},
            Updates = [
                %% Bid object
                { {ItemRegion, bids, BidId, item}, grb_crdt:make_op(grb_lww, ItemKey) },
                { {ItemRegion, bids, BidId, quantity}, grb_crdt:make_op(grb_lww, Qty) },
                { {ItemRegion, bids, BidId, amount}, grb_crdt:make_op(grb_lww, Amount) },
                { {ItemRegion, bids, BidId, bidder}, grb_crdt:make_op(grb_lww, UserKey) },
                %% Update number of bids (increment by one)
                { {ItemRegion, items, ItemId, bids_number}, grb_crdt:make_op(grb_gcounter, 1)},
                %% Update max bid on item (no need to check, CRDT handles the max, and we save a read
                { {ItemRegion, items, ItemId, max_bid}, grb_crdt:make_op(grb_maxtuple, {Amount, {BidKey, UserKey}})},
                %% Update BIDS_item index (and include the bidder)
                { {ItemRegion, bids_item, ItemKey}, grb_crdt:make_op(grb_gset, {BidKey, UserKey}) },
                %% Update BIDS_user index (and include the amount)
                { {UserRegion, bids_user, UserKey}, grb_crdt:make_op(grb_gset, {BidKey, Amount})}
            ],

            {ok, Tx2} = grb_client:send_key_operations(Coord, Tx1, Updates),

            %% this should have finished by now
            {ok, _, Tx3} = grb_client:receive_read_key(Coord, Tx2, UserKey, Req),
            commit_red(Coord, Tx3)
    end.

-spec close_auction_loop(state()) -> {ok, integer(), integer(), state()} | {error, term(), state()}.
close_auction_loop(State) ->
    close_auction_loop(State, 0).

close_auction_loop(State=#state{coord_state=Coord, retry_until_commit=RetryAbort,
                                retry_on_bad_precondition=RetryData}, _Aborted0) ->

    Start = os:timestamp(),
    {ItemRegion, Itemid} = random_item(State),
    {ok, Tx} = start_red_transaction(State),
    Res = close_auction(Coord, Tx, {ItemRegion, items, Itemid}),
    ElapsedUs = erlang:max(0, timer:now_diff(os:timestamp(), Start)),
    case Res of
        {ok, CVC} ->
            {ok, ElapsedUs, 0, State#state{last_cvc=CVC}};

        {error, _} when RetryData ->
            lasp_bench_stats:op_complete({close_auction, close_auction}, {error, precondition}, ignore),
            close_auction_loop(State, 0);

        {abort, _} when RetryAbort ->
            lasp_bench_stats:op_complete({close_auction, close_auction}, {error, abort}, ignore),
            close_auction_loop(State, 0);

        {error, _} ->
            lasp_bench_stats:op_complete({close_auction, close_auction}, {error, precondition}, ignore),
            {ok, ElapsedUs, 0, State};

        {abort, _}=Abort ->
            {error, Abort, State}
    end.

-spec close_auction(grb_client:coord(), grb_client:tx(), {binary(), atom(), binary()}) -> {ok, _} | {abort, _} | {error, atom()}.
close_auction(Coord, Tx, ItemKey={ItemRegion, _, ItemId}) ->
    ItemSellerKey = {ItemRegion, items, ItemId, seller},
    ItemClosedKey = {ItemRegion, items, ItemId, closed},
    ItemMaxBidKey = {ItemRegion, items, ItemId, max_bid},
    {ok, Data0, Tx1} = grb_client:read_key_snapshots(Coord, Tx, [{ItemSellerKey, grb_lww}, {ItemClosedKey, grb_lww}, {ItemMaxBidKey, grb_maxtuple}]),
    #{ ItemClosedKey := IsClosed, ItemMaxBidKey := {MaxBidAmount, MaxBidPayload}, ItemSellerKey := ItemSeller } = Data0,
    if
        IsClosed ->
            {error, closed};

        is_binary(MaxBidPayload) ->
            {error, no_bids};

        true ->
            {MaxBidKey, {BidderRegion, _, _}=BidderKey} = MaxBidPayload,

            %% Read from user partition to prevent blind update
            {ok, Req} = grb_client:send_read_key(Coord, Tx1, BidderKey, grb_lww),
            {ok, Tx2} = grb_client:send_key_operations(Coord, Tx1, [
                %% Close auction
                {ItemClosedKey, grb_crdt:make_op(grb_lww, true)},
                %% Append to user index
                {{BidderRegion, wins_user, BidderKey}, grb_crdt:make_op(grb_gset, {ItemKey, ItemSeller, MaxBidKey, MaxBidAmount})}
            ]),
            {ok, _, Tx3} = grb_client:receive_read_key(Coord, Tx2, BidderKey, Req),
            commit_red(Coord, Tx3)
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
    commit_red(Coord, Tx3).

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
random_user(#state{red_reuse_cvc=RedReuse, last_generated_user=LastGenUser}) ->
    if
        RedReuse =:= false ->
            random_preloaded_user();
        true ->
            random_user_try_last_generated(LastGenUser)
    end.

random_user_try_last_generated(undefined) -> random_preloaded_user();
random_user_try_last_generated({Region, NickName}) ->
    Choose = choose_last_generated(),
    if
        Choose -> {Region, NickName};
        true -> random_preloaded_user()
    end.

-spec random_different_user(binary(), binary()) -> {Region :: binary(), Nickname :: binary()}.
random_different_user(Region, Nickname) ->
    case random_preloaded_user() of
        {Region, Nickname} ->
            random_different_user(Region, Nickname);

        Other ->
            Other
    end.

-spec random_preloaded_user() -> {Region :: binary(), Nickname :: binary()}.
random_preloaded_user() ->
    Region = random_region(),
    Id = rand:uniform(hook_rubis:get_rubis_prop(user_per_region)),
    {Region, list_to_binary(io_lib:format("~s/user/preload_~b", [Region, Id]))}.

random_item(#state{red_reuse_cvc=RedReuse, last_generated_item=LastGenItem}) ->
    if
        RedReuse =:= false ->
            random_preloaded_item();
        true ->
            random_item_try_last_generated(LastGenItem)
    end.

random_item_try_last_generated(undefined) -> random_preloaded_item();
random_item_try_last_generated({Region, ItemId}) ->
    Choose = choose_last_generated(),
    if
        Choose -> {Region, ItemId};
        true -> random_preloaded_item()
    end.

-spec random_preloaded_item() -> {Region :: binary(), ItemId :: binary()}.
random_preloaded_item() ->
    {NReg, Reg} = hook_rubis:get_rubis_prop(regions),
    {Category, ItemsInCat} = random_category_items(),
    ItemIdNumeric = rand:uniform(ItemsInCat),
    Region = erlang:element(((ItemIdNumeric rem NReg) + 1), Reg),
    ItemId = list_to_binary(io_lib:format("~s/items/preload_~b", [Category, ItemIdNumeric])),
    {Region, ItemId}.

-spec gen_new_nickname() -> binary().
gen_new_nickname() -> random_binary(24).

-spec gen_new_item_id(state()) -> {binary(), state()}.
gen_new_item_id(S=#state{local_ip_str=IPStr, worker_id=Id, item_count=N}) ->
    {
        list_to_binary(IPStr ++ integer_to_list(Id) ++ integer_to_list(N)),
        S#state{item_count=N+1}
    }.

-spec gen_new_comment_id(state()) -> {binary(), state()}.
gen_new_comment_id(S=#state{local_ip_str=IPStr, worker_id=Id, comment_count=N}) ->
    {
        list_to_binary(IPStr ++ integer_to_list(Id) ++ integer_to_list(N)),
        S#state{comment_count=N+1}
    }.

-spec gen_new_buynow_id(state()) -> {binary(), state()}.
gen_new_buynow_id(S=#state{local_ip_str=IPStr, worker_id=Id, buy_now_count=N}) ->
    {
        list_to_binary(IPStr ++ integer_to_list(Id) ++ integer_to_list(N)),
        S#state{buy_now_count=N+1}
    }.

-spec gen_new_bid_id(state()) -> {binary(), state()}.
gen_new_bid_id(S=#state{local_ip_str=IPStr, worker_id=Id, bid_count=N}) ->
    {
        list_to_binary(IPStr ++ integer_to_list(Id) ++ integer_to_list(N)),
        S#state{bid_count=N+1}
    }.

-spec random_page_size() -> non_neg_integer().
random_page_size() ->
    safe_uniform(hook_rubis:get_rubis_prop(result_page_size)).

-spec safe_uniform(non_neg_integer()) -> non_neg_integer().
safe_uniform(0) -> 0;
safe_uniform(X) when X >= 1 -> rand:uniform(X).

-spec random_binary(Size :: non_neg_integer()) -> binary().
random_binary(0) ->
    <<>>;
random_binary(N) ->
    list_to_binary(random_string(N)).

-spec random_rating() -> non_neg_integer().
random_rating() ->
    %% range: -5 to 5
    rand:uniform(11) - 6.

-spec choose_last_generated() -> boolean().
choose_last_generated() ->
    rand:uniform() < ?REUSE_GENERATED_PROB.

-spec random_string(Size :: non_neg_integer()) -> string().
random_string(N) ->
    Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890",
    lists:foldl(fun(_, Acc) ->
        [lists:nth(rand:uniform(length(Chars)), Chars)] ++ Acc
    end, [], lists:seq(1, N)).
