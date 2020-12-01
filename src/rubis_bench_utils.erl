-module(rubis_bench_utils).

-record(state, {
    %% todo(borja, rubis): Types?
    session_id :: term(),
    session_username :: term(),
    session_password :: term(),

    %% todo(borja, rubis): Rethink this (pregenerated ids, props in config, etc)
    user_ids :: #{non_neg_integer() => term()},
    user_ids_len :: non_neg_integer(),

    item_ids :: #{non_neg_integer() => term()},
    item_ids_len :: non_neg_integer(),

    usernames :: #{non_neg_integer() := binary()},
    usernames_len :: non_neg_integer()
}).

-type t() :: #state{}.
-export_type([t/0]).
%% State API
-export([new/0,
         random_user_id/1,
         different_user_id/2,
         random_item_id/1,
         random_user/1]).

%% Util API
-export([random_binary/1,
         random_email/1,
         random_rating/0,
         random_boolean/0]).

-spec new() -> t().
new() ->
    #state{}.

-spec random_user_id(t()) -> term().
random_user_id(#state{user_ids=Users,user_ids_len=Len}) ->
    maps:get(rand:uniform(Len), Users).

-spec different_user_id(term(), t()) -> term().
different_user_id(Id, S) ->
    different_user_id(Id, random_user_id(S), S).

different_user_id(Id, Id, S) -> different_user_id(Id, random_user_id(S), S);
different_user_id(_Id, Id2, _S) -> Id2.

-spec random_item_id(t()) -> term().
random_item_id(#state{item_ids=Items,item_ids_len=Len}) ->
    maps:get(rand:uniform(Len), Items).

-spec random_user(t()) -> {UserName :: binary(), Password :: binary()}.
random_user(#state{usernames = Usernames, usernames_len = Len}) ->
    Username = maps:get(rand:uniform(Len), Usernames),
    {Username, Username}.

%% Utils

-spec random_binary(Size :: non_neg_integer()) -> binary().
random_binary(N) ->
    list_to_binary(random_string_(N)).

-spec random_email(Size :: non_neg_integer()) -> binary().
random_email(N) ->
    list_to_binary(random_string_(N) ++ "@example.com").

-spec random_rating() -> non_neg_integer().
random_rating() ->
    rand:uniform(11) - 6.

-spec random_boolean() -> boolean().
random_boolean() ->
    (rand:uniform(16) > 8).

-spec random_string_(Size :: non_neg_integer()) -> list().
random_string_(N) ->
    Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890",
    lists:foldl(fun(_, Acc) ->
        [lists:nth(rand:uniform(length(Chars)), Chars)] ++ Acc
    end, [], lists:seq(1, N)).
