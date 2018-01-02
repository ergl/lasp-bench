#!/usr/bin/env escript

-define(CONFIG_HEADER,
    fun(({concurrent, Conc})) ->
        io_lib:fwrite(
            "{mode,max}.~n{duration,1}.~n{report_interval,1}.~n{concurrent, ~p}.~n",
            [Conc]
        )
    end).

-define(RUBIS_IP, "127.0.0.1").
-define(RUBIS_PORT, 5000).

%% Remove the states that call no transactions, don't care about them
-define(SHOULD_DISCARD, [
    home,
    register,
    browse,
    buynowauth,
    sell,
    selectcategorytosellitem,
    putbid,
    putcomment,
    sellitemform,
    aboutme_auth
]).

-define(OP_PROBS(N), (case N > 100 of true -> 1; _ -> 100 div N end)).

-type probability() :: integer() | float().
-type matrix() :: list(list(probability())).

-record(state, {
    %% RNG State
    random :: rand:state(),

    rows :: integer(),
    columns :: integer(),
    matrix :: matrix(),
    table_name :: binary() | undefined,

    %% Name of the states
    state_names :: list(atom()),
    %% Current state, expressed as an index on the state names
    current_state :: integer(),

    %% Probability of going back, indexed by state
    back_probs :: list(probability()),
    %% Stack of previous states, in case we have to go back
    previous_states :: queue:queue(integer()),

    %% Probability of ending the session, indexed by state
    end_session_probs :: list(probability()),

    %% Waiting times (in ms)
    waiting_times :: dict:dict(atom(), integer()),

    %% True if the session is over, false otherwise
    reached_end_of_session :: boolean()
}).

-type transition_table() :: #state{}.

main([TableFile]) ->
    MatrixState = new(TableFile),
    StateList = generate_state_seq(MatrixState),
    ValidStateNames = discard_no_tx(StateList),
    WaitingTimes = (hd(StateList))#state.waiting_times,
    write_config_file(ValidStateNames, WaitingTimes).

discard_no_tx(StateList) ->
    lists:filtermap(fun(State) ->
        Name = state_name(State),
        case lists:member(Name, ?SHOULD_DISCARD) of
            true -> false;
            _ -> {true, Name}
        end
    end, StateList).

write_config_file(StateNames, WaitingTimes) ->
    io:fwrite(?CONFIG_HEADER({concurrent, 8}), []),
    io:fwrite("{driver, lasp_bench_driver_rubis}.~n"),
    io:fwrite("{key_generator, {partitioned_sequential_int, 5000000}}.~n"),
    io:fwrite("{disable_sequential_int_progress_report, true}.~n"),
    io:fwrite("{value_generator, {fixed_bin, 10248}}.~n"),
    io:fwrite("{rubis_ip, ~p}.~n", [?RUBIS_IP]),
    io:fwrite("{rubis_port, ~p}.~n", [?RUBIS_PORT]),
    io:fwrite("{operations, [~n"),
    Prob = ?OP_PROBS(length(StateNames)),
    [FirstOp | RestOperations] = lists:map(fun(Name) -> {Name, Prob} end, StateNames),
    io:fwrite("\t~p~n", [FirstOp]),
    ok = lists:foreach(fun(Op) ->
        io:fwrite("\t,~p~n", [Op])
    end, RestOperations),
    io:fwrite("]}.~n"),
    io:fwrite("{waiting_times, ~n~p}.~n", [dict:to_list(WaitingTimes)]),
    ok.

-spec new(string()) -> transition_table().
new(FilePath) ->
    parse_file(FilePath).

-spec generate_state_seq(transition_table()) -> list(transition_table()).
generate_state_seq(State) ->
    generate_state_seq(State, [State]).

-spec generate_state_seq(
    transition_table(),
    list(transition_table())
) -> list(transition_table()).
generate_state_seq(State = #state{reached_end_of_session=true}, Acc) ->
    lists:reverse([State | Acc]);

generate_state_seq(State, Acc) ->
    NextState = next_state(State),
    generate_state_seq(NextState, [NextState | Acc]).

-spec next_state(transition_table()) -> transition_table().
next_state(MatrixState) ->
    {Step, NewRState} = rand:uniform_s(MatrixState#state.random),

    BackProb = current_back_probabilty(MatrixState),
    EndProb = current_end_of_session(MatrixState),
    ToCheck = case BackProb < EndProb of
        true ->
            {check_end, should_end(Step, EndProb)};
        false ->
            {check_back, should_go_back(Step, BackProb)}
    end,

    {Continue, NewMatrixState} = case ToCheck of
        {check_end, true} ->
            %% io:format("Reached end of session~n"),
            {stop, MatrixState#state{reached_end_of_session=true}};

            {check_back, true} ->
                case queue:out_r(MatrixState#state.previous_states) of
                    {{value, PopState}, NewStateQueue} ->
                        TmpMatrixState = MatrixState#state{
                            current_state=PopState,
                            previous_states=NewStateQueue
                        },
                        {stop, TmpMatrixState};
                    {empty, _} ->
                        %% Had to go back, but there's nowhere to go, ignore
                        {continue, MatrixState}
                end;

        _ ->
            %% We can do a normal step
            {continue, MatrixState}
    end,

    FinalState = case Continue of
        stop ->
            NewMatrixState#state{random=NewRState};
        continue ->
            #state{matrix=Matrix,current_state=CurrentState,rows=NRows} = NewMatrixState,
            CurrentColumn = column_for_current_state(CurrentState, Matrix),
            NewState = case calc_next_state(CurrentColumn, NRows, Step) of
                {ok, NextState} -> NextState;
                {error, end_of_column} -> CurrentState
            end,
            QueuedStates = queue:in(CurrentState, NewMatrixState#state.previous_states),
            NewMatrixState#state{
                random=NewRState,
                current_state=NewState,
                previous_states=QueuedStates
            }
    end,
    FinalState.

-spec column_for_current_state(integer(), list(list())) -> list().
column_for_current_state(CurState, Matrix) ->
    lists:map(fun(Row) -> lists:nth(CurState, Row) end, Matrix).

-spec current_back_probabilty(transition_table()) -> float().
current_back_probabilty(#state{current_state=C, back_probs=B}) ->
    lists:nth(C, B).

-spec current_end_of_session(transition_table()) -> float().
current_end_of_session(#state{current_state=C, end_session_probs=E}) ->
    lists:nth(C, E).

-spec should_go_back(float(), float()) -> boolean().
should_go_back(Step, BackProbabilty) ->
    Step < BackProbabilty.

-spec should_end(float(), float()) -> boolean().
should_end(Step, EndOfSessionProb) ->
    Step < EndOfSessionProb.

-spec calc_next_state(list(float()), integer(), float()) -> {ok, integer()}
                                                          | {error, atom()}.
calc_next_state(CurrentColumn, NumRows, Step) ->
    calc_next_state(CurrentColumn, 1, NumRows, Step, 0).

calc_next_state(CurrentColumn, CurrentRow, NumRows, Step, AccProb) ->
    case CurrentRow =< NumRows of
        false ->
            {error, end_of_column};
        true ->
            TargetProb = lists:nth(CurrentRow, CurrentColumn),
            NewAccProb = AccProb + TargetProb,
            case Step < NewAccProb of
                false ->
                    calc_next_state(
                        CurrentColumn,
                        CurrentRow + 1,
                        NumRows,
                        Step,
                        NewAccProb
                    );
                true ->
                    {ok, CurrentRow}
            end
    end.

%%
%% FILE PARSING
%%

-spec new_state(integer(), integer()) -> transition_table().
new_state(Rows, Cols) ->
    #state{
        random=rand:seed(exs64),

        rows=Rows,
        columns=Cols,
        matrix=[],
        table_name=undefined,

        state_names=[],
        current_state=1,

        back_probs=[],
        end_session_probs=[],

        waiting_times=dict:new(),
        previous_states=queue:new(),
        reached_end_of_session=false
    }.

-spec parse_file(string()) -> transition_table().
parse_file(Filename) ->
    {ok, Fd} = file:open(Filename, [read, binary]),
    NewState = parse_matrix(Fd, new_state(27, 27)),
    ok = file:close(Fd),
    NewState#state{matrix=lists:reverse(NewState#state.matrix)}.

-spec parse_matrix(file:io_device(), transition_table()) -> transition_table().
parse_matrix(Fd, Acc) ->
    case io:get_line(Fd, "") of
        eof ->
            Acc;
        Line ->
            case parse_line(Line, Acc) of
                {continue, NewAcc} ->
                    parse_matrix(Fd, NewAcc);
                {stop, Acc} ->
                    Acc#state{state_names=lists:reverse(Acc#state.state_names)}
            end
    end.

%% Skip newlines
-spec parse_line(binary(), transition_table()) -> {continue, transition_table()}
                                                | {stop, transition_table()}.
parse_line(<<"\n", _/binary>>, Acc) ->
    {continue, Acc};

parse_line(<<"\"", _/binary>>, Acc) ->
    {continue, Acc};

parse_line(<<"From", _/binary>>, Acc) ->
    {continue, Acc};

parse_line(<<"Total", _/binary>>, Acc) ->
    {stop, Acc};

parse_line(Line, State) ->
    Splitted = binary:split(Line, [<<"\t">>, <<"\n">>], [global, trim_all]),
    {continue, append_row(Splitted, State)}.

-spec append_row(list(binary()), transition_table()) -> transition_table().
append_row([], State) ->
    State;

append_row([<<"RUBiS Transition Table">> | _]=Row, State) ->
    State#state{table_name=lists:nth(2, Row)};

append_row([<<"Back probability">> | Data], State) ->
    FlData = lists:map(fun(D) -> bin_to_numeric(D) end, lists:droplast(Data)),
    State#state{back_probs=FlData};

append_row([<<"End of Session">> | Data], State) ->
    FlData = lists:map(fun(D) -> bin_to_numeric(D) end, lists:droplast(Data)),
    State#state{end_session_probs=FlData};

append_row([RowName | Data], State = #state{columns=Columns}) ->
    Size = length(Data),
    case Size of
        N when N =:= Columns + 2 orelse N =:= Columns + 1 ->
            AtomizedRowName = state_name_to_atom(RowName),
            NumData = lists:map(fun(D) -> bin_to_numeric(D) end, Data),
            {RowData, Extra} = lists:partition(fun(Num) -> Num =< 1 end, NumData),
            case Extra of
                [WaitingTime] ->
                    process_row(AtomizedRowName, RowData, WaitingTime, State);

                %% Browse-only transition tables contain an extra column that means nothing
                [_, WaitingTime] ->
                    process_row(AtomizedRowName, RowData, WaitingTime, State)
            end;
        _ ->
            State
    end.

-spec process_row(
    binary(),
    list(integer() | float()),
    integer(),
    transition_table()
) -> transition_table().

process_row(RowName, RawData, WaitingTime, State=#state{
    matrix=Matrix,
    state_names=StateNames,
    waiting_times=WaitingTimes
}) ->
    NameState = State#state{state_names=[RowName | StateNames]},
    case WaitingTime of
        0 ->
            NameState;
        _ ->
            NameState#state{
                matrix=[RawData | Matrix],
                waiting_times=dict:store(RowName, WaitingTime, WaitingTimes)
            }
    end.

%%
%% UTIL FUNCTIONS
%%

-spec state_name(transition_table()) -> atom().
state_name(#state{end_session_probs=true}) ->
    end_of_session;

state_name(#state{current_state=CState,state_names=SNames}) ->
    lists:nth(CState, SNames).

-spec bin_to_numeric(binary()) -> integer() | float().
bin_to_numeric(N) ->
    try
        binary_to_float(N)
    catch _:_ ->
        binary_to_integer(N)
    end.

state_name_to_atom(<<"AboutMe (auth form)">>) ->
    aboutme_auth;

state_name_to_atom(StateName) ->
    list_to_atom(string:to_lower(binary_to_list(StateName))).
