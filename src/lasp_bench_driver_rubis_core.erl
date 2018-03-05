-module(lasp_bench_driver_rubis_core).

-type rubis_id() :: binary().
-type username() :: bitstring().
-type user() :: {{atom(), bitstring()}, {atom(), bitstring()}}.

-record(rubis_state, {
    logged_in_as :: non_neg_integer() | not_set,

    region_ids :: list(binary()),
    region_ids_len :: non_neg_integer(),

    category_ids :: list(binary()),
    category_ids_len :: non_neg_integer(),

    user_ids :: list(binary()),
    user_ids_len :: non_neg_integer(),

    item_ids :: list(binary()),
    item_ids_len :: non_neg_integer(),

    usernames :: list(username()),
    usernames_len :: non_neg_integer(),

    non_confirmed_usernames :: sets:set(username())
}).

-type rubis_state() :: #rubis_state{}.

-export([new_rubis_state/0,
         random_region_id/1,
         random_category_id/1,
         random_user_id/1,
         random_item_id/1,

         random_user/1,

         gen_new_user/1,
         gen_buy_now/1,
         gen_bid/1,
         gen_comment/1,
         gen_item/1,

         set_logged_in/2,
         get_logged_in/1,

         update_state/2]).

-spec new_rubis_state() -> rubis_state().
new_rubis_state() ->
    UserIds = lasp_bench_config:get(user_ids),
    RegionIds = lasp_bench_config:get(region_ids),
    CategoryIds = lasp_bench_config:get(category_ids),
    ItemIds = lasp_bench_config:get(item_ids),
    Usernames = lasp_bench_config:get(user_names),
    #rubis_state{
        logged_in_as=not_set,

        user_ids=UserIds,
        user_ids_len=length(UserIds),

        region_ids=RegionIds,
        region_ids_len=length(RegionIds),

        category_ids=CategoryIds,
        category_ids_len=length(CategoryIds),

        item_ids=ItemIds,
        item_ids_len=length(ItemIds),

        usernames=Usernames,
        usernames_len=length(Usernames),
        non_confirmed_usernames=sets:new()
    }.

-spec random_region_id(rubis_state()) -> non_neg_integer().
random_region_id(#rubis_state{region_ids=Regions, region_ids_len=Len}) ->
    lists:nth(rand:uniform(Len), Regions).

-spec random_category_id(rubis_state()) -> non_neg_integer().
random_category_id(#rubis_state{category_ids=Categories, category_ids_len=Len}) ->
    lists:nth(rand:uniform(Len), Categories).

-spec random_user_id(rubis_state()) -> non_neg_integer().
random_user_id(#rubis_state{user_ids=Users,user_ids_len=Len}) ->
    lists:nth(rand:uniform(Len), Users).

-spec random_item_id(rubis_state()) -> non_neg_integer().
random_item_id(#rubis_state{item_ids=Items,item_ids_len=Len}) ->
    lists:nth(rand:uniform(Len), Items).

-spec random_user(rubis_state()) -> user().
random_user(#rubis_state{usernames = Usernames, usernames_len = Len}) ->
    Username = lists:nth(rand:uniform(Len), Usernames),
    {Username, Username}.

-spec gen_new_user(rubis_state()) -> {user(), rubis_state()}.
gen_new_user(State = #rubis_state{non_confirmed_usernames = Usernames}) ->
    Username = random_string(24),
    User = {Username, Username},
    {User, State#rubis_state{non_confirmed_usernames = sets:add_element(Username, Usernames)}}.

-spec gen_buy_now(rubis_state()) -> {Item :: rubis_id(), User :: rubis_id(), non_neg_integer()}.
gen_buy_now(State = #rubis_state{logged_in_as = As}) ->
    ItemId = random_item_id(State),
    UserId = case As of
        not_set ->
            random_user_id(State);

        LoggedInId ->
            LoggedInId
    end,
    {ItemId, UserId, rand:uniform(2000)}.

-spec gen_bid(rubis_state()) -> {Item :: rubis_id(), User :: rubis_id(), non_neg_integer()}.
gen_bid(State) ->
    gen_buy_now(State).

-spec gen_comment(rubis_state()) -> {Item :: rubis_id(),
                                     From :: rubis_id(),
                                     To :: rubis_id(),
                                     Rating :: integer(),
                                     Body :: binary()}.

gen_comment(State = #rubis_state{logged_in_as = As}) ->
    ItemId = random_item_id(State),
    FromId = case As of
        not_set ->
             random_user_id(State);

        LoggedInId ->
            LoggedInId
    end,
    ToId = gen_distinct_id(FromId, State),
    Rating = crypto:rand_uniform(-5, 5),
    Body = random_string(20),
    {ItemId, FromId, ToId, Rating, Body}.

-spec gen_item(rubis_state()) -> list().
gen_item(State = #rubis_state{logged_in_as = As}) ->
    SellerId = case As of
        not_set ->
            random_user_id(State);

        LoggedInId ->
            LoggedInId
    end,
    ItemName = random_string(10),
    Description = random_string(20),
    Quantity = rand:uniform(10000),
    CategoryId = random_category_id(State),
    {ItemName, Description, Quantity, CategoryId, SellerId}.

-spec set_logged_in(non_neg_integer(), rubis_state()) -> rubis_state().
set_logged_in(Id, State) ->
    State#rubis_state{logged_in_as=Id}.

get_logged_in(State = #rubis_state{logged_in_as=not_set}) ->
    random_user_id(State);

get_logged_in(#rubis_state{logged_in_as=Id}) ->
    Id.

%% Update state actions

-spec update_state(term(), rubis_state()) -> rubis_state().
update_state({confirm_user, UserId, Username}, State = #rubis_state{
    user_ids= UserIds,
    user_ids_len = UserIdsLen,
    usernames = Usernames,
    usernames_len = UsernamesLen,
    non_confirmed_usernames = PendingUsernames
}) ->
    case sets:is_element(Username, PendingUsernames) of
        false ->
            State;
        true ->
            State#rubis_state{
                user_ids=[UserId | UserIds],
                user_ids_len=UserIdsLen + 1,
                usernames=[Username | Usernames],
                usernames_len=UsernamesLen + 1,
                non_confirmed_usernames=sets:del_element(Username, PendingUsernames)
            }
    end;

update_state({discard_user, Username}, State = #rubis_state{
    non_confirmed_usernames=PendingUsernames
}) ->
    State#rubis_state{non_confirmed_usernames=sets:del_element(Username, PendingUsernames)};

update_state({confirm_item, ItemId}, State = #rubis_state{
    item_ids=ItemIds,
    item_ids_len=Len
}) ->
    State#rubis_state{item_ids=[ItemId | ItemIds], item_ids_len=Len + 1}.

%% Utils

gen_distinct_id(Id, State) ->
    gen_distinct_id(Id, random_user_id(State), State).

gen_distinct_id(Id1, Id2, State) when Id1 =:= Id2 ->
    gen_distinct_id(Id1, random_user_id(State), State);

gen_distinct_id(Id1, Id2, _) when Id1 =/= Id2 ->
    Id2.

random_string(N) ->
    Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890",
    String = lists:foldl(fun(_, Acc) ->
        [lists:nth(rand:uniform(length(Chars)), Chars)] ++ Acc
    end, [], lists:seq(1, N)),
    list_to_binary(String).
