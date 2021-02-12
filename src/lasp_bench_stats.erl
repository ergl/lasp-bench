%% -------------------------------------------------------------------
%%
%% lasp_bench: Benchmarking Suite
%%
%% Copyright (c) 2009-2010 Basho Techonologies
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(lasp_bench_stats).

-behaviour(gen_server).

%% API
-export([start_link/0,
         exponential/1,
         run/0,
         op_complete/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("lasp_bench.hrl").

-record(state, { ops,
                 start_time = os:timestamp(),
                 last_write_time = os:timestamp(),
                 report_interval,
                 errors_since_last_report = false,
                 stats_writer, stats_writer_data,
                 last_warn = {0,0,0}}).

-define(WARN_INTERVAL, 1000). % Warn once a second

-define(HISTOGRAM(Name, Interval), folsom_metrics:new_histogram(Name, slide, Interval)).
-define(HISTOGRAMS(Op, Names, Interval),
    lists:foreach(fun(Name) ->
        folsom_metrics:new_histogram({Name, Op}, slide, Interval)
    end, Names)).

-define(HISTOGRAM_LINE(Tag, Op), {Tag, folsom_metrics:get_histogram_statistics({Tag, Op})}).

%% ====================================================================
%% API
%% ====================================================================

start_link() ->
    gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).

exponential(Lambda) ->
    -math:log(rand:uniform()) / Lambda.

run() ->
    gen_server:call({global, ?MODULE}, run).

op_complete(Op, ok, ElapsedUs) ->
    op_complete(Op, {ok, 1}, ElapsedUs);

op_complete(Op, {ok, Units}, Payload) ->
    %% Update the histogram and units counter for the op in question
   % io:format("Get distributed: ~p~n", [get_distributed()]),
    case get_distributed() of
        true ->
            gen_server:cast({global, ?MODULE}, {Op, {ok, Units}, Payload});
        false ->
            case Payload of
                {TableTag, Us} ->
                    folsom_metrics:notify({TableTag, Op}, Us);
                ElapsedUs ->
                    %% Same behaviour as before, increment normal latencies and units
                    folsom_metrics:notify({latencies, Op}, ElapsedUs),
                    folsom_metrics:notify({units, Op}, {inc, Units})
            end
    end,
    ok;

op_complete(Op, Result, ElapsedUs) ->
    gen_server:call({global, ?MODULE}, {op, Op, Result, ElapsedUs}, infinity).

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([]) ->
    %% Trap exits so we have a chance to flush data
    process_flag(trap_exit, true),
    process_flag(priority, high),

    %% Spin up folsom
    folsom:start(),

    %% Initialize an ETS table to track error and crash counters during
    %% reporting interval
    ets:new(lasp_bench_errors, [protected, named_table]),

    %% Initialize an ETS table to track error and crash counters since
    %% the start of the run
    ets:new(lasp_bench_total_errors, [protected, named_table]),

    %% Get the list of operations we'll be using for this test
    Ops = lasp_bench_ops:list_driver_operations(),

    %% Get the list of measurements we'll be using for this test
    F2 =
        fun({MeasurementTag, _IntervalMS}) -> {MeasurementTag, MeasurementTag};
           ({Label, MeasurementTag, _IntervalMS}) -> {Label, MeasurementTag}
        end,
    Measurements = [F2(X) || X <- lasp_bench_config:get(measurements, [])],

    ok = build_folsom_tables(Ops ++ Measurements),

    %% Schedule next write/reset of data
    ReportInterval = timer:seconds(lasp_bench_config:get(report_interval)),

    StatSpec = lasp_bench_config:get(stats, csv),
    case get_stat_writer_module(StatSpec) of
        {error, Reason} ->
            ?WARN("Cannot build stat spec ~p, from the config value of 'stats' or compiled default: ~p\n",
                  [StatSpec, Reason]),
            {stop, Reason};

        {ok, StatsSinkModule} ->
            {ok, #state{ops = Ops ++ Measurements,
                        report_interval = ReportInterval,
                        stats_writer = StatsSinkModule,
                        stats_writer_data = StatsSinkModule:new(Ops, Measurements)}}
    end.

%% @doc Setup a histogram and counter for each operation.
%%
%%      We only track latencies on successful operations
%%
-spec build_folsom_tables([any()]) -> ok.
build_folsom_tables(Ops) ->
    Interval = lasp_bench_config:get(report_interval),
    lists:foreach(fun(Op) ->
        case Op of
            {_, read_write_blue_track} ->
                ?HISTOGRAMS(Op, [mixed_start,
                                 mixed_read,
                                 mixed_update,
                                 mixed_commit], Interval);
            {_, readonly_red_track} ->
                ?HISTOGRAMS(Op, [red_start,
                                 red_read,
                                 red_commit,
                                 red_prepare,
                                 red_accept,
                                 red_coordinator_commit,
                                 red_coordinator_barrier], Interval);
            {_, readonly_track} ->
                %% Send and receive times, async read execution and wait time
                ?HISTOGRAMS(Op, [send, rcv, read_took, wait_took], Interval);
            {_, readwrite_track} ->
                %% How much do we wait to commit?
                ?HISTOGRAMS(Op, [commit], Interval);
            _ ->
                ok
        end,
        ?HISTOGRAM({latencies, Op}, Interval),
        folsom_metrics:new_counter({units, Op})
    end, Ops).

handle_call(run, _From, State) ->
    %% Schedule next report
    Now = os:timestamp(),
    timer:send_interval(State#state.report_interval, report),
    {reply, ok, State#state { start_time = Now, last_write_time = Now}};

%% Ok not to handle any result other than {error, Reason}, since
%% results of the form {ok, _} are casted instead, and only in
%% distributed mode
handle_call({op, Op, {error, Reason}, _ElapsedUs}, _From, State) ->
    increment_error_counter(Op),
    increment_error_counter({Reason, Op}),
    {reply, ok, State#state { errors_since_last_report = true }};

handle_call({op, Op, {error, Reason, Count}, _ElapsedUs}, _From, State) ->
    increment_error_counter(Op, Count),
    increment_error_counter({Reason, Op}, Count),
    {reply, ok, State#state { errors_since_last_report = true }}.

handle_cast({Op, {ok, Units}, ElapsedUs}, State = #state{last_write_time = LWT, report_interval = RI}) ->
    Now = os:timestamp(),
    TimeSinceLastReport = timer:now_diff(Now, LWT) / 1000, %% To get the diff in seconds
    TimeSinceLastWarn = timer:now_diff(Now, State#state.last_warn) / 1000,
    if
        TimeSinceLastReport > (RI * 2) andalso TimeSinceLastWarn > ?WARN_INTERVAL  ->
            ?WARN("lasp_bench_stats has not reported in ~.2f milliseconds\n", [TimeSinceLastReport]),
            {message_queue_len, QLen} = process_info(self(), message_queue_len),
            ?WARN("stats process mailbox size = ~w\n", [QLen]),
            NewState = State#state{last_warn = Now};
        true ->
            NewState = State
    end,
    folsom_metrics:notify({latencies, Op}, ElapsedUs),
    folsom_metrics:notify({units, Op}, {inc, Units}),
    {noreply, NewState};
handle_cast(_, State) ->
    {noreply, State}.

handle_info(report, State) ->
    consume_report_msgs(),
    Now = os:timestamp(),
    process_stats(Now, State),
    {noreply, State#state { last_write_time = Now, errors_since_last_report = false }}.

terminate(_Reason, #state{stats_writer=Module}=State) ->
    %% Do the final stats report and write the errors file
    process_stats(os:timestamp(), State),
    report_total_errors(State),

    Module:terminate(State#state.stats_writer_data).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% Uses the process dictionary to memoize checks
%% for checking if we're running in distributed mode
%% as constantly checking in with a centralized gen_server
%% would impede progress

get_distributed() ->
    case erlang:get(distribute_work) of
        undefined ->
            DistributeWork = lasp_bench_config:get(distribute_work, false),
            erlang:put(distribute_work, DistributeWork),
            DistributeWork;
        DistributeWork ->
            DistributeWork
    end.

increment_error_counter(Key) ->
    ets_increment(lasp_bench_errors, Key, 1).

increment_error_counter(Key, Incr) ->
    ets_increment(lasp_bench_errors, Key, Incr).

ets_increment(Tab, Key, Incr) when is_integer(Incr) ->
    _ = ets:update_counter(Tab, Key, {2, Incr}, {Key, 0}),
    ok;
ets_increment(Tab, Key, Incr) when is_float(Incr) ->
    _ = ets:select_replace(Tab, [{ {Key, '$1'}, [], [{{safe_wrap_key(Key), {'+', Incr, '$1'}}}]  }]),
    ok.

-spec safe_wrap_key(term()) -> term().
safe_wrap_key(Key) when is_tuple(Key) -> {Key};
safe_wrap_key(Key) -> Key.

error_counter(Key) ->
    lookup_or_zero(lasp_bench_errors, Key).

lookup_or_zero(Tab, Key) ->
    case catch(ets:lookup_element(Tab, Key, 2)) of
        {'EXIT', _} ->
            0;
        Value ->
            Value
    end.

process_stats(Now, #state{stats_writer=Module}=State) ->
    %% Determine how much time has elapsed (seconds) since our last report
    %% If zero seconds, round up to one to avoid divide-by-zeros in reporting
    %% tools.
    Elapsed = timer:now_diff(Now, State#state.start_time) / 1000000,
    Window  = timer:now_diff(Now, State#state.last_write_time) / 1000000,

    %% Time to report latency data to our CSV files
    {Oks, Errors, OkOpsRes} =
        lists:foldl(fun(Op, {TotalOks, TotalErrors, OpsResAcc}) ->
                            {Oks, Errors} = report_latency(State, Elapsed, Window, Op),
                            {TotalOks + Oks, TotalErrors + Errors,
                             [{Op, Oks}|OpsResAcc]}
                    end, {0,0,[]}, State#state.ops),

    %% Reset units
    [folsom_metrics_counter:dec({units, Op}, OpAmount) || {Op, OpAmount} <- OkOpsRes],

    %% Write summary
    Module:process_summary(State#state.stats_writer_data,
                           Elapsed, Window, Oks, Errors),

    %% Dump current error counts to console
    case (State#state.errors_since_last_report) of
        true ->
            ErrCounts = ets:tab2list(lasp_bench_errors),
            true = ets:delete_all_objects(lasp_bench_errors),
            ?INFO("Errors:~p\n", [lists:sort(ErrCounts)]),
            [ets_increment(lasp_bench_total_errors, Err, Count) ||
                              {Err, Count} <- ErrCounts],
            ok;
        false ->
            ok
    end.
%%
%% Write latency info for a given op to the appropriate CSV. Returns the
%% number of successful and failed ops in this window of time.
%%
report_latency(State, Elapsed, Window, Op={_, read_write_blue_track}) ->
    Stats = folsom_metrics:get_histogram_statistics({latencies, Op}),
    Errors = error_counter(Op),
    Units = folsom_metrics:get_metric_value({units, Op}),
    ExtraStats = [?HISTOGRAM_LINE(mixed_start, Op),
                  ?HISTOGRAM_LINE(mixed_read, Op),
                  ?HISTOGRAM_LINE(mixed_update, Op),
                  ?HISTOGRAM_LINE(mixed_commit, Op)],
    send_report(State, Elapsed, Window, Op, [{default, Stats} | ExtraStats], Errors, Units);

report_latency(State, Elapsed, Window, Op={_, readonly_red_track}) ->
    Stats = folsom_metrics:get_histogram_statistics({latencies, Op}),
    Errors = error_counter(Op),
    Units = folsom_metrics:get_metric_value({units, Op}),
    ExtraStats = [  ?HISTOGRAM_LINE(red_start, Op),
                    ?HISTOGRAM_LINE(red_read, Op),
                    ?HISTOGRAM_LINE(red_commit, Op),
                    ?HISTOGRAM_LINE(red_prepare, Op),
                    ?HISTOGRAM_LINE(red_accept, Op),
                    ?HISTOGRAM_LINE(red_coordinator_commit, Op),
                    ?HISTOGRAM_LINE(red_coordinator_barrier, Op)],
    send_report(State, Elapsed, Window, Op, [{default, Stats} | ExtraStats], Errors, Units);

report_latency(State, Elapsed, Window, Op={_, readonly_track}) ->
    Stats = folsom_metrics:get_histogram_statistics({latencies, Op}),
    Errors = error_counter(Op),
    Units = folsom_metrics:get_metric_value({units, Op}),

    ReadStats = [{send,         folsom_metrics:get_histogram_statistics({send, Op})},
                 {rcv,          folsom_metrics:get_histogram_statistics({rcv, Op})},
                 {read_took,    folsom_metrics:get_histogram_statistics({read_took, Op})},
                 {wait_took,    folsom_metrics:get_histogram_statistics({wait_took, Op})}],

    send_report(State, Elapsed, Window, Op, [{default, Stats} | ReadStats], Errors, Units);

report_latency(State, Elapsed, Window, Op={_, readwrite_track}) ->
    Stats = folsom_metrics:get_histogram_statistics({latencies, Op}),
    Errors = error_counter(Op),
    Units = folsom_metrics:get_metric_value({units, Op}),

    UpdateStats = [{commit, folsom_metrics:get_histogram_statistics({commit, Op})}],

    send_report(State, Elapsed, Window, Op, [{default, Stats} | UpdateStats], Errors, Units);

report_latency(State, Elapsed, Window, Op) ->
    Stats = folsom_metrics:get_histogram_statistics({latencies, Op}),
    Errors = error_counter(Op),
    Units = folsom_metrics:get_metric_value({units, Op}),
    send_report(State, Elapsed, Window, Op, Stats, Errors, Units).

send_report(State, Elapsed, Window, Op, Stats, Errors, Units) ->
    #state{stats_writer=Module, stats_writer_data=WriterData} = State,
    Module:report_latency({Module, WriterData}, Elapsed, Window, Op, Stats, Errors, Units),
    {Units, Errors}.

report_total_errors(#state{stats_writer=Module}=State) ->
    case ets:tab2list(lasp_bench_total_errors) of
        [] ->
            ?INFO("No Errors.\n", []);
        UnsortedErrCounts ->
            ErrCounts = lists:sort(UnsortedErrCounts),
            ?INFO("Total Errors:\n", []),
            F = fun({Key, Count}) ->
                        case lists:member(Key, State#state.ops) of
                            true ->
                                %% generic errors for operations are reported in summary and latencies
                                ok;
                            false ->
                                ?INFO("  ~p: ~p\n", [Key, Count]),
                                Module:report_error(State#state.stats_writer_data, Key, Count)
                        end
                end,
            lists:foreach(F, ErrCounts)
    end.

consume_report_msgs() ->
    receive
        report ->
            consume_report_msgs()
    after 0 ->
            ok
    end.

-spec get_stat_writer_module(term()) -> {ok, module()} | {error, Reason :: term()}.
get_stat_writer_module(Spec) ->
    case build_stat_module(Spec) of
        {error, Reason} ->
            {error, Reason};

        {ok, Module} ->
            try
                _ = Module:module_info(),
                {file, _} = code:is_loaded(Module),
                {ok, Module}
            catch _:_ ->
                {error, {Module, invalid_module}}
            end
    end.

build_stat_module({module, Module}) -> {ok, Module};
build_stat_module(Spec) -> normalize_name(Spec).

% Assuming all stats sink modules are prefixed with lasp_bench_stats_writer_
normalize_name(StatsSink) when is_atom(StatsSink) ->
    {ok, list_to_atom("lasp_bench_stats_writer_" ++ atom_to_list(StatsSink))};
normalize_name(StatsSink) -> {error, {StatsSink, invalid_name}}.
