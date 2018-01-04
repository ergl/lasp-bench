-module(lasp_bench_driver_rubis_core).

-define(RUBIS_CATEGORIES, 20).
-define(RUBIS_ITEMS, 727).
-define(RUBIS_REGIONS, [
    <<"AZ--Phoenix">>,
    <<"CA--Los Angeles">>,
    <<"CA--Oakland">>,
    <<"CA--Sacramento">>,
    <<"CA--San Diego">>,
    <<"CA--San Francisco">>,
    <<"CA--San Jose">>,
    <<"CO--Denver">>,
    <<"CT--Hartford">>,
    <<"DC--Washington">>,
    <<"FL--Jacksonville">>,
    <<"FL--Miami">>,
    <<"FL--Orlando">>,
    <<"FL--Tampa-St. Pete">>,
    <<"FL--West Palm Beach">>,
    <<"GA--Atlanta">>,
    <<"HI--Honolulu">>,
    <<"ID--Billings-Boise">>,
    <<"IL--Chicago">>,
    <<"IN--Indianapolis">>,
    <<"KS--Kansas City">>,
    <<"KY--Louisville">>,
    <<"LA--New Orleans">>,
    <<"MA--Boston ">>,
    <<"MD--Baltimore">>,
    <<"MI--Detroit">>,
    <<"MI--Grand Rapids">>,
    <<"MN--Minn-St. Paul">>,
    <<"MO--Kansas City">>,
    <<"MO--St. Louis">>,
    <<"MT--Billings-Boise">>,
    <<"NC--Charlotte">>,
    <<"NC--Greensboro">>,
    <<"NC--Raleigh-Durham">>,
    <<"ND--Bismarck-Pierre">>,
    <<"NM--Albuquerque">>,
    <<"NV--Las Vegas">>,
    <<"NY--Albany">>,
    <<"NY--Buffalo">>,
    <<"NY--New York">>,
    <<"NY--Rochester">>,
    <<"OH--Cincinnati">>,
    <<"OH--Cleveland">>,
    <<"OH--Columbus">>,
    <<"OH--Dayton">>,
    <<"OK--Oklahoma City">>,
    <<"OR--Portland">>,
    <<"PA--Philadelphia">>,
    <<"PA--Pittsburgh">>,
    <<"RI--Providence">>,
    <<"SD--Bismarck-Pierre">>,
    <<"TN--Memphis">>,
    <<"TN--Nashville">>,
    <<"TX--Austin">>,
    <<"TX--Dallas-Fort Worth">>,
    <<"TX--Houston">>,
    <<"TX--San Antonio">>,
    <<"UT--Salt Lake City">>,
    <<"VA--Norfolk-VA Beach">>,
    <<"VA--Richmond">>,
    <<"WA--Seattle-Tacoma">>,
    <<"WI--Milwaukee">>
]).

-type username() :: bitstring().
-type user() :: {{atom(), bitstring()}, {atom(), bitstring()}}.

-record(rubis_state, {
    max_seen_user_id :: non_neg_integer() | not_set,
    own_user_ids_start_at :: non_neg_integer() | not_set,
    generated_usernames :: list(username()),

    max_seen_item_id :: non_neg_integer() | not_set,
    own_item_ids_start_at :: non_neg_integer() | not_set
}).

-type rubis_state() :: #rubis_state{}.

-export([new_rubis_state/0,
         update_state/2,
         gen_new_user/1,
         random_user/1,
         random_user_id/1,
         random_region/0,
         random_item_id/1,
         random_region_id/0,
         random_category_id/0]).

-spec new_rubis_state() -> rubis_state().
new_rubis_state() ->
    #rubis_state{
        max_seen_user_id = not_set,
        own_user_ids_start_at = not_set,
        generated_usernames = [],

        max_seen_item_id = not_set,
        own_item_ids_start_at = not_set
    }.

-spec update_state({atom(), non_neg_integer()}, rubis_state()) -> rubis_state().
update_state({user_id, N}, State = #rubis_state{max_seen_user_id = not_set, own_user_ids_start_at = not_set}) ->
    State#rubis_state{max_seen_user_id=N,own_user_ids_start_at=N};

update_state({user_id, N}, State = #rubis_state{max_seen_user_id = Max, own_user_ids_start_at = Start}) ->
    NewMax = case N > Max of
        true -> N;
        false -> Max
     end,
    NewStart = case N < Start of
        true -> N;
        false -> Start
    end,
    State#rubis_state{max_seen_user_id = NewMax, own_user_ids_start_at = NewStart}.

-spec gen_new_user(rubis_state()) -> {user(), rubis_state()}.
gen_new_user(State = #rubis_state{generated_usernames = Usernames}) ->
    Username = random_string(24),
    User = {{username, Username}, {password, Username}},
    {User, State#rubis_state{generated_usernames = [Username | Usernames]}}.

-spec random_user_id(rubis_state()) -> non_neg_integer().
random_user_id(#rubis_state{max_seen_user_id = not_set}) ->
    1;

random_user_id(#rubis_state{max_seen_user_id = Max}) ->
    crypto:rand_uniform(1, Max + 1).

-spec random_user(rubis_state()) -> user().
random_user(State = #rubis_state{own_user_ids_start_at = N, generated_usernames = Usernames}) ->
    ChosendId = random_user_id(State),
    case ChosendId < N of
        true ->
            IdStr = integer_to_binary(ChosendId),
            {{username, <<"user_", IdStr/binary>>}, {password, <<"password_", IdStr/binary>>}};
        false ->
            Username = lists:nth((ChosendId - N), Usernames),
            {{username, Username}, {password, Username}}
    end.

-spec random_region_id() -> non_neg_integer().
random_region_id() ->
    crypto:rand_uniform(1, length(?RUBIS_REGIONS)).

-spec random_region() -> bitstring().
random_region() ->
    lists:nth(random_region_id(), ?RUBIS_REGIONS).

-spec random_category_id() -> non_neg_integer().
random_category_id() ->
    crypto:rand_uniform(1, ?RUBIS_CATEGORIES + 1).

-spec random_item_id(rubis_state()) -> non_neg_integer().
random_item_id(#rubis_state{max_seen_item_id = not_set}) ->
    1;

random_item_id(#rubis_state{max_seen_item_id = Max}) ->
    crypto:rand_uniform(1, Max + 1).

random_string(Lenght) ->
    base64:encode(crypto:strong_rand_bytes(Lenght)).
