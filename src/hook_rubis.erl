-module(hook_rubis).

-ignore_xref([start/1, stop/0]).

%% API
-export([start/1,
         stop/0]).

-export([get_config/1,
         get_rubis_prop/1,
         make_coordinator/1,
         random_global_index/1]).

start(Args) ->
    logger:info("~p:~p(~p)", [?MODULE, ?FUNCTION_NAME, Args]),

    RubisPath = proplists:get_value(rubis_props_path, Args),
    logger:info("Loading properties from~s~n", [RubisPath]),

    {ok, Terms} = file:consult(RubisPath),
    RubisPropsMap = proplists:get_value(rubis_config, Terms),

    #{
        regions := Regions,
        categories := Categories,
        user_total := TotalUsers
    } = RubisPropsMap,

    RegionsTuple = list_to_tuple(Regions),
    NRegions = erlang:size(RegionsTuple),
    CategoriesTuple = list_to_tuple(Categories),

    ok = persistent_term:put({?MODULE, user_total}, TotalUsers),
    ok = persistent_term:put({?MODULE, user_per_region}, TotalUsers div NRegions),
    ok = persistent_term:put({?MODULE, regions}, {NRegions, RegionsTuple}),
    ok = persistent_term:put({?MODULE, categories}, {erlang:size(CategoriesTuple), CategoriesTuple}),

    [
        ok = persistent_term:put({?MODULE, K}, V)
        || {K, V} <- maps:to_list(maps:without([regions, categories, user_total], RubisPropsMap))
    ],

    hook_grb:start(Args).

stop() ->
    hook_grb:stop().

get_config(Key) ->
    hook_grb:get_config(Key).

get_rubis_prop(Key) ->
    persistent_term:get({?MODULE, Key}).

make_coordinator(Id) ->
    hook_grb:make_coordinator(Id).

-spec random_global_index(grb_client:coord()) -> tuple().
random_global_index(Coordinator) ->
    {rand:uniform(grb_client:ring_size(Coordinator)), global_index}.
