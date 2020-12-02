-module(hook_rubis).

-ignore_xref([start/1, stop/0]).

%% API
-export([start/1,
         stop/0]).

-export([get_config/1,
         get_rubis_prop/1,
         make_coordinator/1]).

start(Args) ->
    logger:info("~p:~p(~p)", [?MODULE, ?FUNCTION_NAME, Args]),

    RubisPath = proplists:get_value(rubis_props_path, Args),
    logger:info("Loading properties from~s~n", [RubisPath]),

    {ok, Terms} = file:consult(RubisPath),
    RubisPropsMap = proplists:get_value(rubis_config, Terms),
    maps:fold(fun(Key, Value, _) ->
        case Key of
            regions ->
                RT = list_to_tuple(Value),
                ok = persistent_term:put({?MODULE, regions}, RT),
                ok = persistent_term:put({?MODULE, regions_size}, erlang:size(RT));
            categories ->
                CT = list_to_tuple(Value),
                ok = persistent_term:put({?MODULE, categories}, CT),
                ok = persistent_term:put({?MODULE, categories_size}, erlang:size(CT));
            _ ->
                ok = persistent_term:put({?MODULE, Key}, Value)
        end,
        ok
    end, ok, RubisPropsMap),

    hook_grb:start(Args).

stop() ->
    hook_grb:stop().

get_config(Key) ->
    hook_grb:get_config(Key).

get_rubis_prop(Key) ->
    persistent_term:get({?MODULE, Key}).

make_coordinator(Id) ->
    hook_grb:make_coordinator(Id).
