#!/usr/bin/env escript

-mode(compile).

%% API
-export([main/1]).

%% Copy-pasted from hook_pvc

-define(NOT(Var), {'not', Var}).
-define(AND(Left, Right), {'andalso', Left, Right}).
-define(IS_RECORD(Var, Type), {is_record, Var, Type, record_info(size, Type)}).
-define(IS_ABORT(Var), {'=:=', {const, abort}, {element, 1, {element, #read_event.resulting_vc, Var}}}).
-define(IS_GC_ABORT(Var), ?AND(?IS_ABORT(Var), {'=:=', {const, 0}, {'map_size', {element, 2, {element, #read_event.resulting_vc, Var}}}})).
-define(READ_AT(Var, Partition), {'=:=', Partition, {element, #read_event.partition, Var}}).
-define(WRITE_AT(Var, Partition), {'=:=', Partition, {element, #write_event.partition, Var}}).

-type tx_id() :: {term(), {non_neg_integer(), non_neg_integer()}}.
-type vc() :: #{non_neg_integer() => non_neg_integer()}.

-record(read_event, {
    partition :: non_neg_integer(),
    incoming_vc :: vc(),
    resulting_vc :: vc() | {abort, vc()},
    log :: [{tx_id(), vc()}, ...],
    queue :: [{tx_id(), vc() | pending | abort}, ...]
}).

-record(write_event, {
    partition :: non_neg_integer()
}).

-record(event, {
    %% This event number
    event_num :: non_neg_integer(),
    %% Transaction id issuing this event
    tx_id :: tx_id(),
    event :: #read_event{} | #write_event{}
}).

do_if_option(Opts, Flag, Fun) ->
    case maps:get(Flag, Opts, ignore) of
        ignore -> ok;
        Other -> Fun(Other), halt(0)
    end.

ratio(_, 0) -> 0.0;
ratio(A, B) -> A / B.

main(Args) ->
    case parse_args(Args) of
        {error, Reason} ->
            io:fwrite(standard_error, "Wrong option, reason: ~s~n", [Reason]),
            usage(),
            halt(1);

        {ok, Opts=#{in_file := TraceFile}} ->
            {ok, TraceTable} = ets:file2tab(TraceFile),
            ok = preprocess_dump(TraceTable),

            do_if_option(Opts, count_aborts, fun(_) ->
                All = count_aborts(TraceTable),
                NonGC = count_non_gc(TraceTable),
                GC = count_gc_incidents(TraceTable),
                io:format("~b aborts total. ~b due to GC (ratio ~f), ~b due to unknown causes (ratio ~f)~n", [All, GC, ratio(GC, All), NonGC, ratio(NonGC, All)])
            end),

            do_if_option(Opts, walk_aborts, fun(_) ->
                walk_aborts(abort_events(TraceTable), Opts, TraceTable)
            end),

            do_if_option(Opts, show_non_gc, fun(_) ->
                All = count_aborts(TraceTable),
                Match = count_non_gc(TraceTable),
                io:format("~b aborts due to unknow reasons out of ~b total (ratio ~f)~n", [Match, All, ratio(Match, All)]),
                case confirm_show_trace() of
                    true ->
                        walk_aborts(non_gc_aborts(TraceTable), Opts, TraceTable);
                    false ->
                        ok
                end
            end),

            do_if_option(Opts, show_gc, fun(_) ->
                All = count_aborts(TraceTable),
                Match = count_gc_incidents(TraceTable),
                io:format("~b aborts due to gc out of ~b total (ratio ~f)~n", [Match, All, ratio(Match, All)]),
                case confirm_show_trace() of
                    true ->
                        walk_aborts(gc_aborts(TraceTable), Opts, TraceTable);
                    false ->
                        ok
                end
            end),

            do_if_option(Opts, from, fun(From) ->
                pprint(Opts, range(From, maps:get(to, Opts), TraceTable))
            end),

            AllIds = abort_events(TraceTable),
            FirstAbortId = lists:min(AllIds),

            do_if_option(Opts, r_partition, fun(RPartition) ->
                pprint(Opts, reads_at(FirstAbortId, RPartition, TraceTable))
            end),

            do_if_option(Opts, w_partition, fun(WPartition) ->
                pprint(Opts, writes_at(FirstAbortId, WPartition, TraceTable))
            end),

            do_if_option(Opts, event_id, fun(Id) ->
                pprint(Opts, trace_for_event_id(Id, TraceTable))
            end),

            do_if_option(Opts, tx_id, fun(TxId) ->
                pprint(Opts, trace_for_tx_id(TxId, TraceTable))
            end)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% util
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

continue_with(Id) ->
    Data = io:get_line(io_lib:format("Show trace for id ~p? [Y/n]: ", [Id])),
    case Data of
        "Y\n" -> true;
        "y\n" -> true;
        "\n" -> true;
        _ -> false
    end.

confirm_show_trace() ->
    Data = io:get_line("Want to walk through the traces? [Y/n]: "),
    case Data of
        "Y\n" -> true;
        "y\n" -> true;
        "\n" -> true;
        _ -> false
    end.

walk_aborts([], _, _) -> ok;
walk_aborts([Id | Rest], Opts, TraceTable) ->
    case continue_with(Id) of
        false -> ok;
        true ->
            pprint(Opts, trace_for_event_id(Id, TraceTable)),
            walk_aborts(Rest, Opts, TraceTable)
    end.

range(From, To, Table) ->
    Spec = [{
        #event{event_num='$1', _='_'},
        [?AND({'>=', '$1', From}, {'=<', '$1', To})],
        ['$_']
    }],
    ets:select(Table, Spec).

reads_at(Before, Partition, Table) ->
    Spec = [{
        #event{event='$1', event_num='$2', _='_'},
        [?AND({'=<', '$2', Before}, ?AND(?IS_RECORD('$1', read_event), ?READ_AT('$1', Partition)))],
        ['$_']
    }],
    ets:select(Table, Spec).

writes_at(Before, Partition, Table) ->
    Spec = [{
        #event{event='$1', event_num='$2', _='_'},
        [?AND({'=<', '$2', Before}, ?AND(?IS_RECORD('$1', write_event), ?WRITE_AT('$1', Partition)))],
        ['$_']
    }],
    ets:select(Table, Spec).

trace_for_event_id(EventId, Table) ->
    [TxId] = ets:select(Table, [{#event{event_num=EventId, tx_id='$1', _='_'}, [], ['$1']}]),
    trace_for_tx_id(TxId, Table).

trace_for_tx_id(TxId, Table) ->
    ets:match_object(Table, #event{tx_id=TxId, _='_'}).

count_gc_incidents(Table) ->
    Spec = [{
        #event{event='$1', _='_'},
        [?AND(?IS_RECORD('$1', read_event), ?IS_GC_ABORT('$1'))],
        [true]
    }],
    ets:select_count(Table, Spec).

count_non_gc(Table) ->
    Spec = [{
        #event{event='$1', event_num='$2', _='_'},
        [?AND(?IS_RECORD('$1', read_event), ?NOT(?IS_GC_ABORT('$1')))],
        [true]
    }],
    ets:select_count(Table, Spec).

non_gc_aborts(Table) ->
    Spec = [{
        #event{event='$1', event_num='$2', _='_'},
        [?AND(?IS_RECORD('$1', read_event), ?NOT(?IS_GC_ABORT('$1')))],
        ['$2']
    }],
    ets:select(Table, Spec).

gc_aborts(Table) ->
    Spec = [{
        #event{event='$1', event_num='$2', _='_'},
        [?AND(?IS_RECORD('$1', read_event), ?IS_GC_ABORT('$1'))],
        ['$2']
    }],
    ets:select(Table, Spec).

%% @doc Returns the ETS key where the first abort happens
-spec abort_events(ets:tab()) -> [non_neg_integer()].
abort_events(Table) ->
    Spec = [{#event{event='$1', event_num='$2', _='_'}, [?AND(?IS_RECORD('$1', read_event), ?IS_ABORT('$1'))], ['$2']}],
    ets:select(Table, Spec).

count_aborts(Table) ->
    Spec = [{#event{event='$1', _='_'}, [?AND(?IS_RECORD('$1', read_event), ?IS_ABORT('$1'))], [true]}],
    ets:select_count(Table, Spec).

pprint(M, P) ->
    case pp(M) of
        true -> io:format("~s~n", [to_string(P)]);
        false -> io:format("~p~n", [P])
    end.

is_mrvc(VC, Log) ->
    VC =:= element(1, lists:last(Log)).

to_string(#event{event_num=N, tx_id=Id, event=Ev}) ->
    io_lib:format("\033[35;5;206m~b\033[0m \033[93;5;206m~p\033[0m := ~s", [N, Id, to_string(Ev)]);

to_string(#read_event{partition=P, incoming_vc=InVC, resulting_vc=OutVC, log=Log, queue=Queue}) ->
    MRVCText = case is_mrvc(OutVC, Log) of
        true -> "(MRVC)";
        false -> ""
    end,
    io_lib:format(
        "READ [ ~p, ~w ] :- ~s ~s ~n\tLog (~b entries) \t~s~n~n\tQueue (~b entries) \t~s",
        [P, InVC, to_string(OutVC), MRVCText, length(Log), to_string(Log), length(Queue), to_string(Queue)]);

to_string(#write_event{partition=P}) ->
    io_lib:format("WRITE(~p)", [P]);

to_string({abort, M}) when is_map(M) ->
    io_lib:format("ABORT and ~w", [M]);

to_string(M) when is_map(M) ->
    io_lib:format("~w", [M]);

to_string([Hd |_]=EvList) when is_record(Hd, event) ->
    lists:foldl(fun(Ev, Acc) ->
        io_lib:format("~s~n~n~s~n", [Acc, to_string(Ev)])
    end, "", EvList);

to_string([]) ->
    "<emtpy>";

to_string(GeneralList) ->
    lists:foldl(fun
        ({{_,_}=L, R}, Acc) -> %% Commit Queue
            io_lib:format("~s~n\t\t~p from ~p", [Acc, R, L]);
        ({L, R={_,_}}, Acc) -> %% Commit Log
            io_lib:format("~s~n\t\t~p from ~p", [Acc, L, R]);
        (String, Acc) ->
            io_lib:format("~s~n~p", [Acc, String])
    end, "", GeneralList).

parse_tx_id(String) ->
    [L,R] = string:tokens(String, "{},"),
    {list_to_integer(L), list_to_integer(R)}.

parse_range(String) ->
    [From,To] = string:tokens(String, ":"),
    {list_to_integer(From), list_to_integer(To)}.

pp(M) -> maps:get(pp, M, false).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% clean data
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

preprocess_dump(TraceTable) ->
    %% We're lucky both kinds of events have the partition in the first key
    PresentPartitions = lists:usort(ets:select(TraceTable, [{#event{event='$1', _='_'}, [], [{element, 2, '$1'}]}])),
    PartitionReducer = build_translation(PresentPartitions),
    clean_data(ets:first(TraceTable), TraceTable, PartitionReducer).

build_translation(ToKeep) ->
    element(1, lists:foldl(fun(Partition, {Map, Char}) ->
        {Map#{Partition => list_to_atom([Char])}, Char + 1}
    end, {#{}, $a}, ToKeep)).

clean_data('$end_of_table', _, _) -> ok;
clean_data(EventId, TraceTable, PartitionReducer) ->
    [Event] = ets:lookup(TraceTable, EventId),
    ets:insert(TraceTable, clean_event(PartitionReducer, Event)),
    clean_data(ets:next(TraceTable, EventId), TraceTable, PartitionReducer).

clean_event(PartitionReducer, E=#event{tx_id=Id, event=Inner}) ->
    E#event{tx_id=element(2, Id), event=clean_inner_event(PartitionReducer, Inner)}.

clean_inner_event(PartitionReducer, #write_event{partition=P}) ->
    #write_event{partition=translate_partition(P, PartitionReducer)};

clean_inner_event(PartitionReducer, REv=#read_event{partition=P,
                                                    incoming_vc=InVC,
                                                    resulting_vc=Result,
                                                    log=Log,
                                                    queue=Queue}) ->

    REv#read_event{partition=translate_partition(P, PartitionReducer),
                   incoming_vc=translate_map(InVC, PartitionReducer),
                   resulting_vc=translate_map(Result, PartitionReducer),
                   log=translate_log(Log, PartitionReducer),
                   queue=translate_queue(Queue, PartitionReducer)}.

translate_partition(Partition, Trans) ->
    maps:get(Partition, Trans, Partition).

translate_map({abort, Map}, Reducer) ->
    {abort, translate_map(Map, Reducer)};

translate_map(Map, Reducer) when is_map(Map) ->
    maps:from_list([{translate_partition(K, Reducer), V} || {K,V} <- maps:to_list(Map)]).

translate_log(Log, Reducer) ->
    [{translate_map(M, Reducer), element(2, Id)} || {M, Id} <- Log].

translate_queue(Queue, Reducer) ->
    [{element(2, Id), translate_attr(Attr, Reducer)} || {Id, Attr} <- Queue].

translate_attr(Map, Reducer) when is_map(Map) ->
    translate_map(Map, Reducer);
translate_attr(Else, _) ->
    Else.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% getopt
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

usage() ->
    Name = filename:basename(escript:script_name()),
    ok = io:fwrite(standard_error, "Usage: ~s [-v] -f in_file.dump [-e event_num] [-t tx_id] [-pr partition] [-pw partition] [--abort] [--gc] [--nogc] [--range from:to]~n", [Name]).

parse_args([]) -> {error, "No args present"};
parse_args(Args) ->
    case parse_args(Args, #{}) of
        {ok, Opts} -> required(Opts);
        Err -> Err
    end.

parse_args([], Acc) -> {ok, Acc};
parse_args([ [$- | Flag] | Args], Acc) ->
    case Flag of
        [$f] ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{in_file => Arg} end);
        [$e] ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{event_id => list_to_integer(Arg)} end);
        [$t] ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{tx_id => parse_tx_id(Arg)} end);
        [$c] ->
            parse_args(Args, Acc#{count_aborts => true});
        "pr" ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{r_partition => list_to_atom(Arg)} end);
        "pw" ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{w_partition => list_to_atom(Arg)} end);
        "-range" ->
            parse_flag(Flag, Args, fun(Arg) -> {From, To} = parse_range(Arg), Acc#{from => From, to => To} end);
        "-aborts" ->
            parse_args(Args, Acc#{walk_aborts => true});
        "-gc" ->
            parse_args(Args, Acc#{show_gc => true});
        "-nogc" ->
            parse_args(Args, Acc#{show_non_gc => true});
        [$v] ->
            parse_args(Args, Acc#{pp => true});
        [$h] ->
            usage(),
            halt(0);
        _ ->
            {error, io_lib:format("Don't know what to do with flag ~p", [Flag])}
    end;

parse_args(_, _) ->
    {error, "No args present"}.

parse_flag(Flag, Args, Fun) ->
    case Args of
        [FlagArg | Rest] -> parse_args(Rest, Fun(FlagArg));
        _ -> {error, io_lib:format("No arg present, but required for flag ~p", [Flag])}
    end.

required(Opts) ->
    Required = [in_file],
    Either = [event_id, tx_id, r_partition, w_partition, from, walk_aborts, show_gc, show_non_gc, count_aborts],
    ValidRequired = lists:all(fun(F) -> maps:is_key(F, Opts) end, Required),
    ValidEither = lists:any(fun(F) -> maps:is_key(F, Opts) end, Either),
    case ValidRequired andalso ValidEither of
        true -> {ok, Opts};
        false -> {error, io_lib:format("Missing required fields ~p and at least one of ~s", [Required, to_string(Either)])}
    end.
