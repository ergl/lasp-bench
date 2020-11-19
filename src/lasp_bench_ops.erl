-module(lasp_bench_ops).

-callback init(Args :: [Id :: non_neg_integer(), ...]) -> term().
-callback operations() -> [{atom(), atom()}].
-callback next_operation(term()) -> {ok, {atom(), atom()}, term()} | {error, atom()}.
-callback terminate(term()) -> ok.

-optional_callbacks([terminate/1]).

-export([get_driver_operations/1,
         list_driver_operations/0,
         init_ops/1,
         next_op/1]).

-record(distribution, {
    seed :: rand:seed(),
    ops :: tuple(),
    ops_len :: non_neg_integer()
}).

-record(ops_mod, {
    mod :: module(),
    init_args :: [term()],
    mod_state = undefined :: term()
}).

-opaque t() :: #distribution{} | #ops_mod{}.
-export_type([t/0]).

-spec get_driver_operations(Id :: non_neg_integer()) -> t().
get_driver_operations(Id) ->
    Ops = lasp_bench_config:get(operations, []),
    case Ops of
        {module, Module, Args} ->
            #ops_mod{mod=Module, init_args=[Id | Args]};

        L when is_list(L) ->
            make_weighted_ops(Id, L)
    end.

-spec list_driver_operations() -> [{term(), term()}].
list_driver_operations() ->
    case lasp_bench_config:get(operations, []) of
        {module, Module, _} ->
            Module:operations();
        L when is_list(L) ->
            F = fun
                    ({OpTag, _Count}) -> {OpTag, OpTag};
                    ({Label, OpTag, _Count}) -> {Label, OpTag}
            end,
            [F(X) || X <- L]
    end.

make_weighted_ops(Id, Ops0) ->
    %% Setup RNG seed for worker sub-process to use; incorporate the ID of
    %% the worker to ensure consistency in load-gen
    %%
    %% NOTE: If the worker process dies, this obviously introduces some entroy
    %% into the equation since you'd be restarting the RNG all over.
    %%
    %% The RNG_SEED is static by default for replicability of key size
    %% and value size generation between test runs.
    {A1, A2, A3} =
        case lasp_bench_config:get(rng_seed, {42, 23, 12}) of
            {Aa, Ab, Ac} -> {Aa, Ab, Ac};
            now -> erlang:timestamp()
        end,
    RngSeed = {A1+Id, A2+Id, A3+Id},
    F = fun
            ({OpTag, Count}) ->
                lists:duplicate(Count, {OpTag, OpTag});
            ({Label, OpTag, Count}) ->
                lists:duplicate(Count, {Label, OpTag})
        end,
    Ops = list_to_tuple(lists:flatten([F(X) || X <- Ops0])),

    #distribution{seed = RngSeed,
                  ops = Ops,
                  ops_len = size(Ops)}.

-spec init_ops(t()) -> t().
init_ops(T=#distribution{seed=Seed}) ->
    _ = rand:seed(exs1024, Seed),
    T;

init_ops(T=#ops_mod{mod=Mod, init_args=Args}) ->
    T#ops_mod{mod_state=Mod:init(Args), init_args=undefined}.

-spec next_op(t()) -> {ok, {term(), term()}, t()} | {error, term()}.
next_op(T=#distribution{ops=Ops, ops_len=Len}) ->
    {ok, erlang:element(rand:uniform(Len), Ops), T};

next_op(T=#ops_mod{mod=Mod, mod_state=S0}) ->
    case Mod:next_operation(S0) of
        {error, Reason} ->
            {error, Reason};

        {ok, NextOp, S} ->
            {ok, NextOp, T#ops_mod{mod_state=S}}
    end.
