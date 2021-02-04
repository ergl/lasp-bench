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
-module(lasp_bench_worker).

-behaviour(gen_server).

%% API
-export([start_link/2,
         start_link_local/2,
         run/1,
         stop/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
    id :: non_neg_integer(),
    keygen :: lasp_bench_keygen:t(),
    valgen :: lasp_bench_valgen:t(),
    driver :: module(),
    driver_state :: term(),
    driver_ops :: lasp_bench_ops:t(),
    shutdown_on_error :: boolean(),
    parent_pid :: pid(),
    worker_pid :: pid() | undefined,
    sup_id :: pid()
}).

-include("lasp_bench.hrl").

%% ====================================================================
%% API
%% ====================================================================

start_link(SupChild, Id) ->
    case lasp_bench_config:get(distribute_work, false) of
        true ->
            start_link_distributed(SupChild, Id);
        false ->
            start_link_local(SupChild, Id)
    end.

start_link_distributed(SupChild, Id) ->
    Node = pool:get_node(),
    rpc:block_call(Node, ?MODULE, start_link_local, [SupChild, Id]).

start_link_local(SupChild, Id) ->
    gen_server:start_link(?MODULE, [SupChild, Id], []).

run(Pids) ->
    [ok = gen_server:call(Pid, run, infinity) || Pid <- Pids],
    ok.

stop(Pids) ->
    [ok = gen_server:call(Pid, stop, infinity) || Pid <- Pids],
    ok.

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([SupChild, Id]) ->
    %% Pull all config settings from environment
    Driver  = lasp_bench_config:get(driver),
    Ops = lasp_bench_ops:get_driver_operations(Id),
    ShutdownOnError = lasp_bench_config:get(shutdown_on_error, false),

    %% Finally, initialize key and value generation. We pass in our ID to the
    %% initialization to enable (optional) key/value space partitioning
    KeyGen = lasp_bench_keygen:new(lasp_bench_config:get(key_generator), Id),
    ValGen = lasp_bench_valgen:new(lasp_bench_config:get(value_generator), Id),

    State = #state{id=Id,
                   keygen=KeyGen,
                   valgen=ValGen,
                   driver=Driver,
                   driver_ops=Ops,
                   shutdown_on_error=ShutdownOnError,
                   parent_pid=self(),
                   worker_pid=undefined, %% will be overwritten later
                   sup_id=SupChild},

    %% Use a dedicated sub-process to do the actual work. The work loop may need
    %% to sleep or otherwise delay in a way that would be inappropriate and/or
    %% inefficient for a gen_server. Furthermore, we want the loop to be as
    %% tight as possible for peak load generation and avoid unnecessary polling
    %% of the message queue.
    %%
    %% Link the worker and the sub-process to ensure that if either exits, the
    %% other goes with it.
    process_flag(trap_exit, true),
    WorkerPid = spawn_link(fun() -> worker_init(State) end),
    WorkerPid ! {init_driver, self()},
    receive
        driver_ready ->
            ok;
        {driver_failed, Why} ->
            exit({init_driver_failed, Why})
    end,

    %% If the system is marked as running this is a restart; queue up the run
    %% message for this worker
    case lasp_bench_app:is_running() of
        true ->
            ?WARN("Restarting crashed worker.\n", []),
            gen_server:cast(self(), run);
        false ->
            ok
    end,

    {ok, State#state { worker_pid = WorkerPid }}.

handle_call(run, _From, State) ->
    State#state.worker_pid ! run,
    {reply, ok, State}.

handle_cast(run, State) ->
    State#state.worker_pid ! run,
    {noreply, State}.

handle_info({'EXIT', Pid, Reason}, State) ->
    case Reason of
        normal ->
            %% Clean shutdown of the worker; spawn a process to terminate this
            %% process via the supervisor API and make sure it doesn't restart.
            spawn(fun() -> stop_worker(State#state.sup_id) end),
            {noreply, State};

        _ ->
            ?ERROR("Worker ~p exited with ~p~n", [Pid, Reason]),
            %% Worker process exited for some other reason; stop this process
            %% as well so that everything gets restarted by the sup
            {stop, normal, State}
    end.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%% ====================================================================
%% Internal functions
%% ====================================================================

%%
%% Stop a worker process via the supervisor and terminate the app
%% if there are no workers remaining
%%
%% WARNING: Must run from a process other than the worker!
%%
stop_worker(SupChild) ->
    ok = lasp_bench_sup:stop_child(SupChild),
    case lasp_bench_sup:workers() of
        [] ->
            %% No more workers -- stop the system
            lasp_bench_app:stop();
        _ ->
            ok
    end.

worker_init(State=#state{driver_ops=Ops}) ->
    %% Trap exits from linked parent process; use this to ensure the driver
    %% gets a chance to cleanup
    process_flag(trap_exit, true),
    worker_idle_loop(State#state{driver_ops=lasp_bench_ops:init_ops(Ops)}).

worker_idle_loop(State) ->
    Driver = State#state.driver,
    receive
        {init_driver, Caller} ->
            %% Spin up the driver implementation
            DriverState = case catch(Driver:new(State#state.id)) of
                {ok, InternalState} ->
                    Caller ! driver_ready,
                    InternalState;

                Error ->
                    Caller ! {init_driver_failed, Error},
                    ?FAIL_MSG("Failed to initialize driver ~p: ~p\n", [Driver, Error]),
                    undefined
            end,
            worker_idle_loop(State#state { driver_state = DriverState });
        run ->
            case lasp_bench_config:get(mode) of
                max ->
                    ?INFO("Starting max worker: ~p on ~p~n", [self(), node()]),
                    max_worker_run_loop(State);
                {rate, max} ->
                    ?INFO("Starting max worker: ~p on ~p~n", [self(), node()]),
                    max_worker_run_loop(State);
                {rate, Rate} ->
                    %% Calculate mean interarrival time in in milliseconds. A
                    %% fixed rate worker can generate (at max) only 1k req/sec.
                    MeanArrival = 1000 / Rate,
                    ?INFO("Starting ~w ms/req fixed rate worker: ~p on ~p\n", [MeanArrival, self(), node()]),
                    rate_worker_run_loop(State, 1 / MeanArrival)
            end
    end.

-spec worker_next_op(State :: #state{}) -> {ok, NewState :: #state{}} | {error, ExitReason :: term()}.
worker_next_op(State) ->
    case lasp_bench_ops:next_op(State#state.driver_ops) of
        {error, Reason} ->
            io:format("Next op exited with reason ~p~n", [Reason]),
            %% Driver (or something within it) has requested that this worker
            %% terminate cleanly.
            ?INFO("Driver ~p (~p) has requested stop: ~p\n", [State#state.driver, self(), Reason]),
            %% Give the driver a chance to cleanup
            (catch (State#state.driver):terminate(normal, State#state.driver_state)),
            {error, normal};

        {ok, Next, NextOps} ->
            worker_next_op_continue(Next, State#state{driver_ops=NextOps});

        {ok, Next, WaitTime, NextOps} ->
            Res = worker_next_op_continue(Next, State#state{driver_ops=NextOps}),
            if
                element(1, Res) =:= ok -> timer:sleep(WaitTime);
                true -> ok
            end,
            Res
    end.

worker_next_op2(State, OpTag) ->
    Driver = State#state.driver,
    catch Driver:run(OpTag, State#state.keygen,
                     State#state.valgen, State#state.driver_state).

-spec worker_next_op_continue({term(), term()}, #state{}) -> {ok, #state{}} | {error, Reason :: term()}.
worker_next_op_continue({_Label, OpTag}=Next, State) ->
    Start = os:timestamp(),
    Result = worker_next_op2(State, OpTag),
    ElapsedUs = erlang:max(0, timer:now_diff(os:timestamp(), Start)),
    case Result of
        {Res, DriverState} when Res == ok orelse element(1, Res) == ok ->
            lasp_bench_stats:op_complete(Next, Res, ElapsedUs),
            {ok, State#state{driver_state = hack_preprocess_driver_state(Next, DriverState)}};

        {Res, DriverState} when Res == silent orelse element(1, Res) == silent ->
            {ok, State#state { driver_state = DriverState}};

        {ok, _ElapsedT, DriverState} ->
            lasp_bench_stats:op_complete(Next, ok, ElapsedUs),
            {ok, State#state { driver_state = hack_preprocess_driver_state(Next, DriverState) }};

        {ok, _ElapsedT, Retries, Total, DriverState} ->
            lasp_bench_stats:op_complete(Next, {ok, Total}, ElapsedUs),
            if
                Retries > 0 ->
                    lasp_bench_stats:op_complete(Next, {error, abort, Retries}, ElapsedUs);
                true ->
                    ok
            end,
            {ok, State#state {driver_state = hack_preprocess_driver_state(Next, DriverState) }};

        {error, Reason, DriverState} ->
            %% Driver encountered a recoverable error
            lasp_bench_stats:op_complete(Next, {error, Reason}, ElapsedUs),
            State#state.shutdown_on_error andalso
                erlang:send_after(500, lasp_bench,
                                  {shutdown, "Shutdown on errors requested", 1}),
            {ok, State#state { driver_state = DriverState}};

        {'EXIT', Reason} ->
            %% Driver crashed, generate a crash error and terminate. This will take down
            %% the corresponding worker which will get restarted by the appropriate supervisor.
            lasp_bench_stats:op_complete(Next, {error, crash}, ElapsedUs),

            %% Give the driver a chance to cleanup
            (catch (State#state.driver):terminate({'EXIT', Reason}, State#state.driver_state)),

            ?DEBUG("Driver ~p crashed: ~p\n", [State#state.driver, Reason]),
            case State#state.shutdown_on_error of
                true ->
                    %% Yes, I know this is weird, but currently this
                    %% is how you tell Basho Bench to return a
                    %% non-zero exit status.  Ideally this would all
                    %% be done in the `handle_info' callback where it
                    %% would check `Reason' and `shutdown_on_error'.
                    %% Then I wouldn't have to return a bullshit "ok"
                    %% here.
                    erlang:send_after(500, lasp_bench,
                                      {shutdown, "Shutdown on errors requested", 2}),
                    {ok, State};
                false ->
                    {error, crash}
            end;

        {stop, Reason} ->
            %% Driver (or something within it) has requested that this worker
            %% terminate cleanly.
            ?INFO("Driver ~p (~p) has requested stop: ~p\n", [State#state.driver, self(), Reason]),

            %% Give the driver a chance to cleanup
            (catch (State#state.driver):terminate(normal, State#state.driver_state)),

            {error, normal}
    end.

hack_preprocess_driver_state({_, read_write_blue_track}=Op, Payload) ->
    {track_mixed_blue, State, Start, {RN, ReadTook}, {WN, UpdateTook}, Commit} = Payload,
    ok = lasp_bench_stats:op_complete(Op, ok, {mixed_start, Start}),
    ok = lasp_bench_stats:op_complete(Op, {ok, RN}, {mixed_read, ReadTook}),
    ok = lasp_bench_stats:op_complete(Op, {ok, WN}, {mixed_update, UpdateTook}),
    ok = lasp_bench_stats:op_complete(Op, ok, {mixed_commit, Commit}),
    State;

hack_preprocess_driver_state({_, readonly_red_track}=Op, {track_red_commit, State, Start, Read, Commit, CommitTimings}) ->
    PrepareTook = maps:get(prepare, CommitTimings, 0),
    CoordinatorCommit = maps:get(coordinator_commit, CommitTimings, 0),
    CoordinatorBarrier = case maps:get(coordinator_barrier, CommitTimings, undefined) of
        undefined -> 0;
        Lat -> Lat
    end,
    ok = lasp_bench_stats:op_complete(Op, ok, {red_start, Start}),
    ok = lasp_bench_stats:op_complete(Op, ok, {red_read, Read}),
    ok = lasp_bench_stats:op_complete(Op, ok, {red_commit, Commit}),
    ok = lasp_bench_stats:op_complete(Op, ok, {red_prepare, PrepareTook}),
    AcceptAvg = case maps:get(accept_acc, CommitTimings, undefined) of
        Accepts when is_list(Accepts) andalso length(Accepts) > 0 -> lists:sum(Accepts) / length(Accepts);
        _ -> 0
    end,
    ok = lasp_bench_stats:op_complete(Op, ok, {red_accept, AcceptAvg}),
    ok = lasp_bench_stats:op_complete(Op, ok, {red_coordinator_commit, CoordinatorCommit}),
    ok = lasp_bench_stats:op_complete(Op, ok, {red_coordinator_barrier, CoordinatorBarrier}),
    State;

hack_preprocess_driver_state({_, readonly_track}=Op, {track_reads, Sent, Received, StampMap, State}) ->
    %% *Took times were measured on the server
    #{rcv := ServerRcv,
      read_took := ReadTook,
      wait_took := WaitTook,
      send := ServerSend} = StampMap,

    RequestTime = erlang:max(0, timer:now_diff(ServerRcv, Sent)),
    ResponseTime = erlang:max(0, timer:now_diff(Received, ServerSend)),

    ok = lasp_bench_stats:op_complete(Op, ok, {send, RequestTime}),
    ok = lasp_bench_stats:op_complete(Op, ok, {read_took, ReadTook}),
    ok = lasp_bench_stats:op_complete(Op, ok, {wait_took, WaitTook}),
    ok = lasp_bench_stats:op_complete(Op, ok, {rcv, ResponseTime}),

    State;

hack_preprocess_driver_state({_, readwrite_track}=Op, {track_commits, CommitTime, State}) ->
    ok = lasp_bench_stats:op_complete(Op, ok, {commit, CommitTime}),

    State;

hack_preprocess_driver_state(_, State) ->
    State.

needs_shutdown(State) ->
    Parent = State#state.parent_pid,
    receive
        {'EXIT', Pid, _Reason} ->
            case Pid of
                Parent ->
                    %% Give the driver a chance to cleanup
                    (catch (State#state.driver):terminate(normal,
                                                          State#state.driver_state)),
                    true;
                _Else ->
                    %% catch this so that selective recieve doesn't kill us when running
                    %% the riakclient_driver
                    false
            end
    after 0 ->
            false
    end.


max_worker_run_loop(State) ->
    case worker_next_op(State) of
        {ok, State2} ->
            case needs_shutdown(State2) of
                true ->
                    ok;
                false ->
                    max_worker_run_loop(State2)
            end;
        {error, ExitReason} ->
            exit(ExitReason)
    end.

rate_worker_run_loop(State, Lambda) ->
    %% Delay between runs using exponentially distributed delays to mimic
    %% queue.
    timer:sleep(trunc(lasp_bench_stats:exponential(Lambda))),
    case worker_next_op(State) of
        {ok, State2} ->
            case needs_shutdown(State2) of
                true ->
                    ok;
                false ->
                    rate_worker_run_loop(State2, Lambda)
            end;
        {error, ExitReason} ->
            exit(ExitReason)
    end.
