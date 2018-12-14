%% -------------------------------------------------------------------
%%
%% lasp_bench: Benchmarking Suite
%%
%% Copyright (c) 2009-2014 Basho Techonologies
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
%% HOWTO:
%%
%% * To run lasp_bench with the default CSV writer, nothing needs to
%%   be done. But if wanting to override a former setting, then
%%   writing the following in the benchmark config file will switch
%%   the stats writer to CSV:
%%
%%    {stats, {csv}}.
%%.

-module(lasp_bench_stats_writer_csv).

-include("hack.hrl").

-export([new/2,
         terminate/1,
         process_summary/5,
         report_error/3,
         report_latency/7]).

-include("lasp_bench.hrl").

new(Ops, Measurements) ->
    ?INFO("module=~s event=start stats_sink=csv\n", [?MODULE]),
    %% Setup output file handles for dumping periodic CSV of histogram results.
    [erlang:put({csv_file, X}, op_csv_file(X)) || X <- Ops],

    %% Setup output file handles for dumping periodic CSV of histogram results.
    [erlang:put({csv_file, X}, measurement_csv_file(X)) || X <- Measurements],

    %% Setup output file w/ counters for total requests, errors, etc.
    {ok, SummaryFile} = file:open("summary.csv", [raw, binary, write]),
    file:write(SummaryFile, <<"elapsed, window, total, successful, failed\n">>),

    %% Setup errors file w/counters for each error.  Embedded commas likely
    %% in the error messages so quote the columns.
    {ok, ErrorsFile} = file:open("errors.csv", [raw, binary, write]),
    file:write(ErrorsFile, <<"\"error\",\"count\"\n">>),

    {SummaryFile, ErrorsFile}.

terminate({SummaryFile, ErrorsFile}) ->
    ?INFO("module=~s event=stop stats_sink=csv\n", [?MODULE]),
    [begin
        case Op of
            {_, Tag} when ?hack_tag(Tag) ->
                ok = lists:foreach(fun file:close/1, F);
            _ ->
                ok = file:close(F)
        end
    end || {{csv_file, Op}, F} <- erlang:get()],
    ok = file:close(SummaryFile),
    ok = file:close(ErrorsFile),
    ok.

process_summary({SummaryFile, _ErrorsFile},
                Elapsed, Window, Oks, Errors) ->
    file:write(SummaryFile,
               io_lib:format("~w, ~w, ~w, ~w, ~w\n",
                             [Elapsed,
                              Window,
                              Oks + Errors,
                              Oks,
                              Errors])).

report_error({_SummaryFile, ErrorsFile},
             Key, Count) ->
    file:write(ErrorsFile,
               io_lib:format("\"~w\",\"~w\"\n",
                             [Key, Count])).

report_latency(_, Elapsed, Window, Op={_, noop}, Payload, Errors, Units) ->
    {SendStats, RcvStats, Stats} = Payload,
    Fds = erlang:get({csv_file, Op}),
    lists:foreach(fun({Type, Fd}) ->
        case Type of
            default ->
                file:write(Fd, get_line_from_stats(Op, Elapsed, Window, Stats, Errors, Units));
            send ->
                file:write(Fd, get_line_from_stats(Op, Elapsed, Window, SendStats, Errors, Units));
            rcv ->
                file:write(Fd, get_line_from_stats(Op, Elapsed, Window, RcvStats, Errors, Units))
        end
    end, Fds);

report_latency(_, Elapsed, Window, Op={_, ping}, Payload, Errors, Units) ->
    {SendStats, ExecuteStats, StartStats, CommitStats, RcvStats, Stats} = Payload,
    Fds = erlang:get({csv_file, Op}),
    lists:foreach(fun({Type, Fd}) ->
        case Type of
            default ->
                file:write(Fd, get_line_from_stats(Op, Elapsed, Window, Stats, Errors, Units));
            send ->
                file:write(Fd, get_line_from_stats(Op, Elapsed, Window, SendStats, Errors, Units));
            exe ->
                file:write(Fd, get_line_from_stats(Op, Elapsed, Window, ExecuteStats, Errors, Units));
            start ->
                file:write(Fd, get_line_from_stats(Op, Elapsed, Window, StartStats, Errors, Units));
            commit ->
                file:write(Fd, get_line_from_stats(Op, Elapsed, Window, CommitStats, Errors, Units));
            rcv ->
                file:write(Fd, get_line_from_stats(Op, Elapsed, Window, RcvStats, Errors, Units))
        end
    end, Fds);

report_latency(_, Elapsed, Window, Op={_, timed_read}, Payload, Errors, Units) ->
    {SendStats, ExecuteStats, StartStats, ReadStats, CommitStats, RcvStats, Stats} = Payload,
    Fds = erlang:get({csv_file, Op}),
    lists:foreach(fun({Type, Fd}) ->
        case Type of
            default ->
                file:write(Fd, get_line_from_stats(Op, Elapsed, Window, Stats, Errors, Units));
            send ->
                file:write(Fd, get_line_from_stats(Op, Elapsed, Window, SendStats, Errors, Units));
            exe ->
                file:write(Fd, get_line_from_stats(Op, Elapsed, Window, ExecuteStats, Errors, Units));
            start ->
                file:write(Fd, get_line_from_stats(Op, Elapsed, Window, StartStats, Errors, Units));
            read ->
                file:write(Fd, get_line_from_stats(Op, Elapsed, Window, ReadStats, Errors, Units));
            commit ->
                file:write(Fd, get_line_from_stats(Op, Elapsed, Window, CommitStats, Errors, Units));
            rcv ->
                file:write(Fd, get_line_from_stats(Op, Elapsed, Window, RcvStats, Errors, Units))
        end
    end, Fds);

report_latency({_SummaryFile, _ErrorsFile}, Elapsed, Window, Op, Stats, Errors, Units) ->
    Line = get_line_from_stats(Op, Elapsed, Window, Stats, Errors, Units),
    file:write(erlang:get({csv_file, Op}), Line).

get_line_from_stats(Op, Elapsed, Window, Stats, Errors, Units) ->
    case proplists:get_value(n, Stats) > 0 of
        true ->
            P = proplists:get_value(percentile, Stats),
            io_lib:format("~w, ~w, ~w, ~w, ~.1f, ~w, ~w, ~w, ~w, ~w, ~w\n",
                          [Elapsed,
                           Window,
                           Units,
                           proplists:get_value(min, Stats),
                           proplists:get_value(arithmetic_mean, Stats),
                           proplists:get_value(median, Stats),
                           proplists:get_value(95, P),
                           proplists:get_value(99, P),
                           proplists:get_value(999, P),
                           proplists:get_value(max, Stats),
                           Errors]);
        false ->
            ?WARN("No data for op: ~p\n", [Op]),
            io_lib:format("~w, ~w, 0, 0, 0, 0, 0, 0, 0, 0, ~w\n", [Elapsed, Window, Errors])
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================
%% TODO(borja): Hack
op_csv_file({_, noop}) ->
    Names = [{default, "noop_latencies.csv"},
             {send, "noop_send_latencies.csv"},
             {rcv, "noop_rcv_latencies.csv"}],

    lists:map(fun({Type, Fname}) ->
        {ok, F} = file:open(Fname, [raw, binary, write]),
        ok = file:write(F, <<"elapsed, window, n, min, mean, median, 95th, 99th, 99_9th, max, errors\n">>),
        {Type, F}
    end, Names);

op_csv_file({_, ping}) ->
    Names = [{default, "ping_latencies.csv"},
             {send, "ping_send_latencies.csv"},
             {exe, "ping_execute_latencies.csv"},
             {start, "ping_start_tx_latencies.csv"},
             {commit, "ping_commit_tx_latencies.csv"},
             {rcv, "ping_rcv_latencies.csv"}],

    lists:map(fun({Type, Fname}) ->
        {ok, F} = file:open(Fname, [raw, binary, write]),
        ok = file:write(F, <<"elapsed, window, n, min, mean, median, 95th, 99th, 99_9th, max, errors\n">>),
        {Type, F}
    end, Names);

op_csv_file({_, timed_read}) ->
    Names = [{default, "timed_read_latencies.csv"},
             {send, "timed_read_send_latencies.csv"},
             {exe, "timed_read_execute_latencies.csv"},
             {start, "timed_read_start_tx_latencies.csv"},
             {read, "timed_read_read_latencies.csv"},
             {commit, "timed_read_commit_tx_latencies.csv"},
             {rcv, "timed_read_rcv_latencies.csv"}],

    lists:map(fun({Type, Fname}) ->
        {ok, F} = file:open(Fname, [raw, binary, write]),
        ok = file:write(F, <<"elapsed, window, n, min, mean, median, 95th, 99th, 99_9th, max, errors\n">>),
        {Type, F}
    end, Names);

op_csv_file({Label, _Op}) ->
    Fname = normalize_label(Label) ++ "_latencies.csv",
    {ok, F} = file:open(Fname, [raw, binary, write]),
    ok = file:write(F, <<"elapsed, window, n, min, mean, median, 95th, 99th, 99_9th, max, errors\n">>),
    F.

measurement_csv_file({Label, _Op}) ->
    Fname = normalize_label(Label) ++ "_measurements.csv",
    {ok, F} = file:open(Fname, [raw, binary, write]),
    ok = file:write(F, <<"elapsed, window, n, min, mean, median, 95th, 99th, 99_9th, max, errors\n">>),
    F.

normalize_label(Label) when is_list(Label) ->
    replace_special_chars(Label);
normalize_label(Label) when is_binary(Label) ->
    normalize_label(binary_to_list(Label));
normalize_label(Label) when is_integer(Label) ->
    normalize_label(integer_to_list(Label));
normalize_label(Label) when is_atom(Label) ->
    normalize_label(atom_to_list(Label));
normalize_label(Label) when is_tuple(Label) ->
    Parts = [normalize_label(X) || X <- tuple_to_list(Label)],
    string:join(Parts, "-").

replace_special_chars([H|T]) when
      (H >= $0 andalso H =< $9) orelse
      (H >= $A andalso H =< $Z) orelse
      (H >= $a andalso H =< $z) ->
    [H|replace_special_chars(T)];
replace_special_chars([_|T]) ->
    [$-|replace_special_chars(T)];
replace_special_chars([]) ->
    [].
