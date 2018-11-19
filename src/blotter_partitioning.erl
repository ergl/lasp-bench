-module(blotter_partitioning).

%% API
-export([new/1,
         get_key_node/2]).


new(BinRing) ->
    new(BinRing, []).

new([], Acc) ->
    lists:reverse(Acc);

new([{TermId, BinNode} | Rest], Acc) ->
    Id = binary_to_term(TermId, [safe]),
    Node = binary_to_atom(BinNode, latin1),
    new(Rest, [{Id, Node} | Acc]).

get_key_node(Key, Ring) ->
    Hash = convert_key(Key),
    NumPartitions = length(Ring),
    Pos = Hash rem NumPartitions + 1,
    lists:nth(Pos, Ring).

convert_key(BinKey) when is_binary(BinKey) ->
    try
        abs(binary_to_integer(BinKey))
    catch _:_ ->
        %% Looked into the internals of riak_core for this
        HashedKey = crypto:hash(sha, term_to_binary({<<"antidote">>, BinKey})),
        abs(crypto:bytes_to_integer(HashedKey))
    end.
