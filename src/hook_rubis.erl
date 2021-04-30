-module(hook_rubis).

-ignore_xref([start/1, stop/0]).

%% API
-export([start/1,
         stop/0]).

-export([get_config/1,
         get_rubis_prop/1,
         make_coordinator/1,
         random_global_index/1]).

-export([trace_msg/2]).

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

    ok = case proplists:get_value(tx_trace_log_path, Args, undefined) of
        undefined ->
            ok;
        TraceLogPath ->
            Self = self(),
            Ref = erlang:make_ref(),
            Pid = erlang:spawn(fun() -> init_file_loop(Self, Ref, TraceLogPath) end),
            receive
                {ok, Ref} ->
                    persistent_term:put({?MODULE, log_file_pid}, Pid);
                {error, Ref, Reason} ->
                    %% crash
                    {error, Reason}
            end
    end,

    hook_grb:start(Args).

stop() ->
    Ref = make_ref(),
    get_file_pid() ! {close_file, Ref, self()},
    receive {ok, Ref} -> ok end,
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

trace_msg(Fmt, Args) ->
    Pid = get_file_pid(),
    Pid ! {trace_msg, append_time_ts(io_lib:format(Fmt, Args))},
    ok.

-spec get_file_pid() -> pid().
get_file_pid() ->
    persistent_term:get({?MODULE, log_file_pid}).

init_file_loop(ParentPid, ParentRef, Path) ->
    case file:open(Path, [write, raw, delayed_write]) of
        {ok, IODev} ->
            ParentPid ! {ok, ParentRef},
            file_pid_loop(IODev);
        {error, Reason} ->
            ParentPid ! {error, ParentRef, Reason}
    end.

file_pid_loop(IODev) ->
    receive
        {trace_msg, Bin} ->
            ok = file:write(IODev, Bin),
            file_pid_loop(IODev);

        {close_file, Ref, From} ->
            ok = file:sync(IODev),
            ok = file:close(IODev),
            From ! {ok, Ref};

       _ ->
           file_pid_loop(IODev)
    end.

append_time_ts(Str) ->
    Ts = {_, _, Micros} = os:timestamp(),
    {_, {Hour, Min, Sec}} = calendar:now_to_datetime(Ts),
    io_lib:format("~2w:~2..0w:~2..0w.~6..0w ~s", [Hour, Min, Sec, Micros, Str]).
