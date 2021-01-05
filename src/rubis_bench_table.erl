-module(rubis_bench_table).
-behavior(lasp_bench_ops).

%% Remove the states that call no transactions, don't care about them
-define(SHOULD_DISCARD, [home,
                         register,
                         browse,
                         buy_now_auth,
                         put_bid_auth,
                         put_comment_auth,
                         sell,
                         sell_item_form,
                         about_me_auth]).

-define(STATE_NAMES, {
    %% no transaction
    home,
    %% no transaction
    register,
    register_user,
    %% no transaction
    browse,
    %% browse ALL categories
    browse_categories,
    search_items_in_category,
    browse_regions,
    browse_categories_in_region,
    search_items_in_region,
    view_item,
    view_user_info,
    view_bid_history,
    %% no transaction
    buy_now_auth,
    buy_now,
    store_buy_now,
    %% no transaction
    put_bid_auth,
    put_bid,
    store_bid,
    %% no transaction
    put_comment_auth,
    put_comment,
    store_comment,
    %% no transaction
    sell,
    select_category_to_sell_item,
    %% no transaction
    sell_item_form,
    register_item,
    %% no transaction
    about_me_auth,
    about_me,
    get_auctions_ready_for_close,
    close_auction
}).

-type probability() :: integer() | float().
-type matrix() :: tuple().

-record(state, {
    %% RNG State
    random :: rand:state(),

    %% Limit?
    steps_until_stop :: pos_integer() | undefined,

    %% Override thinking time
    thinking_time :: non_neg_integer() | undefined,

    %% +/- ms to apply to thinking time
    sleep_noise :: non_neg_integer(),

    rows :: non_neg_integer(),
    columns :: non_neg_integer(),
    matrix :: matrix(),
    table_name :: binary(),

    %% Name of the states
    state_names :: {atom()},
    %% Current state, expressed as an index on the state names
    current_state :: non_neg_integer(),

    %% Probability of going back, tuple indexed by state
    back_probs :: {probability()},
    %% Stack of previous states, in case we have to go back
    previous_states :: queue:queue(non_neg_integer()),

    %% Probability of ending the session, tuple indexed by state
    end_session_probs :: {probability()},

    %% Waiting times (in ms), tuple indexed by state
    waiting_times :: {non_neg_integer()},

    %% True if the session is over, false otherwise
    reached_end_of_session :: boolean()
}).

-record(parse_state, {
    current_row = undefined :: non_neg_integer() | undefined,
    rows = undefined :: non_neg_integer() | undefined,
    cols = undefined :: non_neg_integer() | undefined,
    matrix = undefined :: {{probability()}} | undefined,
    table_name = undefined :: binary() | undefined,
    state_names = undefined :: {atom()} | undefined,
    back_probability = undefined :: {probability()} | undefined,
    end_probability = undefined :: {probability()} | undefined,
    waiting_times = undefined :: {probability()} | undefined
}).

-opaque t() :: #state{}.
-type state_name() :: atom().
-export_type([t/0]).

%% Ops API
-export([init/1,
         operations/0,
         next_operation/1,
         terminate/1]).

%% API
-export([new/1,
         new/5,
         next_state/1]).

-export([wait_time/1,
         wait_time_exp/1,
         wait_time_default/1]).

init([_Id, ArgMap = #{transition_table := FilePath}]) ->
    SleepNoise = maps:get(thinking_noise, ArgMap, 5),
    <<A:32, B:32, C:32>> = crypto:strong_rand_bytes(12),
    Seed = maps:get(transition_seed, ArgMap, {A, B, C}),
    TransitionLimit = maps:get(transition_limit, ArgMap, undefined),
    ThinkingTime = case ArgMap of
        #{thinking_time := Time} when Time > 0 -> Time;
        _ -> undefined
    end,
    new(FilePath, Seed, TransitionLimit, ThinkingTime, SleepNoise).

operations() ->
    [{OpTag, OpTag} || OpTag <- (tuple_to_list(?STATE_NAMES) -- ?SHOULD_DISCARD)].

next_operation(S0) ->
    case next_state(S0) of
        stop ->
            {error, stop};
        {ok, State, S} ->
            {ok, {State, State}, S};
        {ok, State, WaitTime, S} ->
            {ok, {State, State}, WaitTime, S}
    end.

terminate(_) ->
    ok.

-spec new(string()) -> t().
new(FilePath) ->
    <<A:32, B:32, C:32>> = crypto:strong_rand_bytes(12),
    new(FilePath, {A, B, C}, undefined, undefined, 5).

-spec new(string(), rand:seed(), pos_integer() | undefined, non_neg_integer() | undefined, non_neg_integer()) -> t().
new(FilePath, Seed, TransitionLimit, ThinkingTime, SleepNoise)  ->
    {ok, Fd} = file:open(FilePath, [read, binary]),
    ParseState = parse_table(Fd, #parse_state{}),
    ok = file:close(Fd),
    make_table(Seed, TransitionLimit, ThinkingTime, SleepNoise, ParseState).

-spec next_state(t()) -> {ok, state_name(), t()} | {ok, state_name(), non_neg_integer(), t()} | stop.
next_state(#state{reached_end_of_session=true}) ->
    stop;

next_state(#state{steps_until_stop=N}) when N =< 0 ->
    stop;

next_state(S0) ->
    case next_state_internal(S0) of
        S=#state{reached_end_of_session=true} ->
            next_state(recycle_state(decr_steps(S)));

        NextState ->
            StateName = state_name(NextState),
            case lists:member(StateName, ?SHOULD_DISCARD) of
                false ->
                    {WaitDefault, NextState1}  = wait_time_default(NextState),
                    {Wait, NextState2} = wait_time_exp(NextState1),
                    if
                        WaitDefault =/= undefined ->
                            {ok, StateName, WaitDefault, decr_steps(NextState2)};
                        Wait =/= 0 ->
                            {ok, StateName, Wait, decr_steps(NextState2)};
                        true ->
                            {ok, StateName, decr_steps(NextState2)}
                    end;
                true ->
                    %% loop until we reach another transaction, but don't consume steps
                    next_state(NextState)
            end
    end.

-spec decr_steps(t()) -> t().
decr_steps(S=#state{steps_until_stop=undefined}) -> S;
decr_steps(S=#state{steps_until_stop=N}) -> S#state{steps_until_stop=N-1}.

-spec recycle_state(t()) -> t().
recycle_state(S) ->
    S#state{reached_end_of_session=false,
            previous_states=queue:new(),
            current_state=1}.

%%
%%  STATE GENERATION
%%

-spec next_state_internal(t()) -> t().
next_state_internal(Table0=#state{random=Random0,
                                  rows=NRows,
                                  matrix=Matrix,
                                  current_state=CurrentState,
                                  back_probs=BackProbs,
                                  previous_states=PrevStates0,
                                  end_session_probs=EndProbs}) ->

    {Step, Random} = rand:uniform_s(Random0),
    case calc_next_state(Step, Matrix, CurrentState, NRows) of
        {ok, NextState} ->
            %% If there's no probability of going back from this state, flush the queue
            PrevStates = case erlang:element(NextState, BackProbs) of
                N when N > 0 -> queue:in(CurrentState, PrevStates0);
                _ -> queue:new()
            end,
            Table0#state{random=Random,
                         current_state=NextState,
                         previous_states=PrevStates};

        {not_found, AccProbability} ->
            BackProb = erlang:element(CurrentState, BackProbs),
            EndProb =  erlang:element(CurrentState, EndProbs),
            if
                Step < (AccProbability + BackProb) ->
                    {ok, Table1} = back_to_previous_state(Table0#state{random=Random}),
                    Table1;

                Step < (AccProbability + BackProb + EndProb) ->
                    Table0#state{random=Random, reached_end_of_session=true};

                true ->
                    %% The Java code implicitly assumes that if we don't match on any probability,
                    %% we will reach the end and terminate
                    Table0#state{random=Random, reached_end_of_session=true}
            end
    end.

-spec back_to_previous_state(t()) -> {ok, t()} | error.
back_to_previous_state(Table=#state{previous_states=PrevStates0}) ->
    case queue:out_r(PrevStates0) of
        {{value, PrevState}, PrevStates} ->
            {ok, Table#state{current_state=PrevState, previous_states=PrevStates}};
        {empty, _} ->
            error
    end.

-spec calc_next_state(Prob :: probability(),
                      Matrix :: matrix(),
                      Col :: non_neg_integer(),
                      NRows :: non_neg_integer()) -> {ok, non_neg_integer()} | {not_found, probability()}.

calc_next_state(Prob, Matrix, Col, NRows) ->
    calc_next_state(Prob, 0, Matrix, Col, 1, NRows).

-spec calc_next_state(Prob :: probability(),
                      AccProb :: probability(),
                      Matrix :: matrix(),
                      Col :: non_neg_integer(),
                      Row :: non_neg_integer(),
                      NRows :: non_neg_integer()) -> {ok, non_neg_integer()} | {not_found, probability()}.

calc_next_state(Prob, AccProb0, Matrix, Col, Row, NRows) when Row =< NRows ->
    MatrixProb = erlang:element(Col, erlang:element(Row, Matrix)),
    AccProb = AccProb0 + MatrixProb,
    if
        Prob < AccProb ->
            {ok, Row};
        true ->
            calc_next_state(Prob, AccProb, Matrix, Col, Row + 1, NRows)
    end;

calc_next_state(_Prob, AccProb, _Matrix, _Col, _Row, _NRows) ->
    {not_found, AccProb}.

%%
%% FILE PARSING
%%

-spec parse_table(file:io_device(), #parse_state{}) -> #parse_state{}.
parse_table(Fd, Acc0) ->
    case io:get_line(Fd, "") of
        eof ->
            Acc0;
        Line ->
            case parse_line(Line, Acc0) of
                {continue, Acc} -> parse_table(Fd, Acc);
                {stop, Acc} -> Acc
            end
    end.

%% Skip newlines
-spec parse_line(binary(), #parse_state{}) -> {continue, #parse_state{}}
                                            | {stop, #parse_state{}}.

parse_line(<<"\n", _/binary>>, Acc) ->
    {continue, Acc};

parse_line(<<"\"", _/binary>>, Acc) ->
    {continue, Acc};

parse_line(<<"Total", _/binary>>, Acc) ->
    {stop, Acc};

parse_line(Line, State0) ->
    Splitted = binary:split(Line, [<<"\t">>, <<"\n">>], [global, trim_all]),
    case append_row(Splitted, State0) of
        {stop, State} ->
            {stop, State};
        State ->
            {continue, State}
    end.

-spec append_row(list(binary()), #parse_state{}) -> #parse_state{} | {stop, #parse_state{}}.
append_row([], State) ->
    State;

append_row([<<"RUBiS Transition Table">> | _]=Row, State) ->
    State#parse_state{table_name=lists:nth(2, Row)};

append_row([<<"From", _/binary>> | Rest], State) ->
    %% Skip the column for waiting time, matrix is square
    NumCols = length(Rest) - 1,
    State#parse_state{current_row=1,
                      rows=NumCols,
                      cols=NumCols,
                      matrix=erlang:make_tuple(NumCols, undefined),
                      state_names=erlang:make_tuple(NumCols, <<>>),
                      waiting_times=erlang:make_tuple(NumCols, 0)};

append_row([<<"Back probability">> | Data], State) ->
    %% Skip waiting time
    FlData = [bin_to_numeric(D) || D <- lists:droplast(Data)],
    State#parse_state{back_probability=list_to_tuple(FlData)};

append_row([<<"End of Session">> | Data], State) ->
    %% Skip waiting time
    FlData = [bin_to_numeric(D) || D <- lists:droplast(Data)],
    %% Docs say that everything after this row can be ignored
    {stop, State#parse_state{end_probability=list_to_tuple(FlData)}};

append_row([_RowName | Data], State0 = #parse_state{current_row=NRow,
                                                    cols=Cols,
                                                    matrix=Matrix0,
                                                    state_names=Names0,
                                                    waiting_times=WaitTimes0}) ->
    Size = length(Data),
    case Size of
        N when N =:= Cols + 1 ->
            NumData = [ bin_to_numeric(D) || D <- Data],
            %% The last item is WaitingTime
            {RowData, [WaitingTime]} = lists:split(length(NumData) - 1, NumData),
            State0#parse_state{current_row=NRow + 1,
                               matrix=erlang:setelement(NRow, Matrix0, list_to_tuple(RowData)),
                               state_names=erlang:setelement(NRow, Names0, erlang:element(NRow, ?STATE_NAMES)),
                               waiting_times=erlang:setelement(NRow, WaitTimes0, WaitingTime)};
        _ ->
            State0#parse_state{current_row=NRow + 1}
    end.

make_table(Seed, TransitionLimit, ThinkingTime, SleepNoise, #parse_state{rows=Rows,
                                                                         cols=Cols,
                                                                         matrix=Matrix,
                                                                         table_name=TableName,
                                                                         state_names=Names,
                                                                         back_probability=BackProbs,
                                                                         end_probability=EndProbs,
                                                                         waiting_times=WaitTimes}) ->
    #state{random=rand:seed(exsss, Seed),
           steps_until_stop=TransitionLimit,
           thinking_time=ThinkingTime,
           sleep_noise=SleepNoise,
           rows=Rows,
           columns=Cols,
           matrix=Matrix,
           table_name=TableName,
           state_names=Names,
           current_state=1,
           back_probs=BackProbs,
           previous_states=queue:new(),
           end_session_probs=EndProbs,
           waiting_times=WaitTimes,
           reached_end_of_session=false}.

%%
%%  UTIL FUNCTIONS
%%

-spec state_name(t()) -> atom().
state_name(#state{current_state=CState,state_names=SNames}) ->
    erlang:element(CState, SNames).

-spec wait_time(t()) -> {non_neg_integer(), t()}.
wait_time(S=#state{current_state=CState, waiting_times=WaitTimes}) ->
    dither(erlang:element(CState, WaitTimes), S).

-spec wait_time_default(t()) -> {non_neg_integer() | undefined, t()}.
wait_time_default(S=#state{thinking_time=Time}) ->
    dither(Time, S).

%% Negative exponential distribution used by
%% TPC-W spec for Think Time (Clause 5.3.2.1) and USMD (Clause 6.1.9.2)
-spec wait_time_exp(t()) -> {non_neg_integer(), t()}.
wait_time_exp(S=#state{random=R0}) ->
    {Step, R1} = rand:uniform_s(R0),
    SleepTime = if
        Step < 4.54e-5 ->
            Step + 0.5;
        true ->
            (-7000.0 * math:log(Step)) + 0.5
    end,
    {erlang:round(SleepTime), S#state{random=R1}}.

-spec dither(non_neg_integer() | undefined, t()) -> {non_neg_integer() | undefined, t()}.
dither(undefined, S) ->
    {undefined, S};
dither(Time, S=#state{sleep_noise=0}) ->
    {Time, S};
dither(Time, S=#state{random=R0, sleep_noise=Noise}) ->
    {Steps, R1} = rand:uniform_s(Noise, R0),
    {SignP, R2} = rand:uniform_s(16, R1),
    if
        SignP > 8 ->
            {Time + Steps, S#state{random=R2}};
        true ->
            {Time - Steps, S#state{random=R2}}
    end.

-spec bin_to_numeric(binary()) -> integer() | float().
bin_to_numeric(N) ->
    try
        binary_to_float(N)
    catch _:_ ->
        binary_to_integer(N)
    end.
