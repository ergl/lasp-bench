-module(lasp_bench_sleep_ops).
-behavior(lasp_bench_ops).

%% API
-export([init/1,
         operations/1,
         next_operation/1]).

-record(state, {
    %% RNG State
    random :: rand:state(),

    %% Override thinking time
    thinking_time :: non_neg_integer() | undefined,

    %% +/- ms to apply to thinking time
    thinking_jitter :: non_neg_integer(),

    %% use tcp-w thinking time instead
    override_neg_exp :: boolean(),

    %% The list of operations
    operations :: tuple()
}).


init([_Id, ArgMap = #{operation_list := Ops}]) ->
    Jitter = maps:get(thinking_jitter_ms, ArgMap, 5),
    OverrideTime = maps:get(use_tcpw_time, ArgMap, false),
    ThinkingTime = case ArgMap of
        #{thinking_time := Time} when Time > 0 -> Time;
        _ -> undefined
    end,
    <<A:32, B:32, C:32>> = crypto:strong_rand_bytes(12),
    Seed = maps:get(transition_seed, ArgMap, {A, B, C}),
    #state{
        random=rand:seed(exsss, Seed),
        thinking_jitter=Jitter,
        thinking_time=ThinkingTime,
        override_neg_exp=OverrideTime,
        operations=list_to_tuple(Ops)
    }.

operations([#{operation_list := Ops}]) ->
    [{OpTag, OpTag} || OpTag <- Ops];
operations(_) ->
    [{noop, noop}].

next_operation(S0=#state{random=Random0, operations=Ops}) ->
    {N, Random} = rand:uniform_s(erlang:size(Ops), Random0),
    NextOp = erlang:element(N, Ops),
    {WaitDefault, S1} = wait_time_default(S0#state{random=Random}),
    {TCPWTime, S2} = wait_state_time(S1),
    if
        WaitDefault =/= undefined ->
            {ok, {NextOp, NextOp}, WaitDefault, S2};
        TCPWTime =/= 0 ->
            {ok, {NextOp, NextOp}, TCPWTime, S2};
        true ->
            {ok, {NextOp, NextOp}, S2}
    end.

%%
%%  UTIL FUNCTIONS
%%

wait_time_default(S=#state{thinking_time=Time}) ->
    jitter(Time, S).

jitter(undefined, S) ->
    {undefined, S};
jitter(Time, S=#state{thinking_jitter=0}) ->
    {Time, S};
jitter(Time, S=#state{random=R0, thinking_jitter=Noise}) ->
    {Steps, R1} = rand:uniform_s(Noise, R0),
    {SignP, R2} = rand:uniform_s(16, R1),
    if
        SignP > 8 ->
            {Time + Steps, S#state{random=R2}};
        true ->
            {erlang:max(Time - Steps, 0), S#state{random=R2}}
    end.

wait_state_time(S=#state{override_neg_exp=true}) ->
    {Time, S1} = wait_time_exp(S),
    jitter(Time, S1);
wait_state_time(S=#state{override_neg_exp=false}) ->
    {0, S}.

%% Negative exponential distribution used by
%% TPC-W spec for Think Time (Clause 5.3.2.1) and USMD (Clause 6.1.9.2)
wait_time_exp(S=#state{random=R0}) ->
    {Step, R1} = rand:uniform_s(R0),
    SleepTime = if
        Step < 4.54e-5 ->
            Step + 0.5;
        true ->
            (-7000.0 * math:log(Step)) + 0.5
    end,
    {erlang:round(SleepTime), S#state{random=R1}}.
