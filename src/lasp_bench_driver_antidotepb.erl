-module(lasp_bench_driver_antidotepb).

-export([new/1,
         run/4]).

-include("lasp_bench.hrl").

-define(TYPE, antidote_crdt_lwwreg).
-define(BUCKET, <<"bench_bucket">>).

-record(state, {
    %% Number assigned by lasp_bench
    client_id,

    %% Antidote connection to the remote node
    connection_pid,
    target_node,
    target_port,

    %% Commit time of last transaction
    commit_time,

    read_only_num :: non_neg_integer(),
    read_write_num :: {non_neg_integer(), non_neg_integer()},

    batch_reads :: boolean(),
    batch_writes :: boolean()
}).

new(Id) ->
    Ips = lasp_bench_config:get(antidote_ips),
    Ports = lasp_bench_config:get(antidote_ports),

    ReadOnlyNum = lasp_bench_config:get(read_only_num),
    ReadWriteNum = lasp_bench_config:get(read_write_num),

    BatchReads = lasp_bench_config:get(batch_reads),
    BatchWrites = lasp_bench_config:get(batch_writes),

    TargetNode = lists:nth((Id rem length(Ips) + 1), Ips),
    TargetPort = lists:nth((Id rem length(Ips) + 1), Ports),

    {ok, Pid} = antidotec_pb_socket:start_link(TargetNode, TargetPort),
    {ok, #state{client_id=Id,
                connection_pid=Pid,
                target_node=TargetNode,
                target_port=TargetPort,

                commit_time=ignore,

                read_only_num=ReadOnlyNum,
                read_write_num=ReadWriteNum,

                batch_reads=BatchReads,
                batch_writes=BatchWrites}}.

run(single_read, KeyGen, _, State = #state{
    connection_pid=Pid,
    commit_time=CT,
    client_id=ClientId
}) ->
    Result = run_transaction(ClientId,
                             Pid,
                             CT,
                             KeyGen,
                             ignore,
                             {1, true},
                             {0, ignore}),

    case Result of
        {error, Reason} ->
            {error, Reason, State};

        {ok, NewCT} ->
            {ok, State#state{commit_time=NewCT}}
    end;

run(read_write, KeyGen, ValueGen, State = #state{
    connection_pid=Pid,
    commit_time=CT,
    batch_reads=BatchReads,
    batch_writes=BatchWrites,
    read_write_num={Reads,Writes},
    client_id=ClientId
}) ->
    Result = run_transaction(ClientId,
                             Pid,
                             CT,
                             KeyGen,
                             ValueGen,
                             {Reads, BatchReads},
                             {Writes, BatchWrites}),

    case Result of
        {error, Reason} ->
            {error, Reason, State};

        {ok, NewCT} ->
            {ok, State#state{commit_time=NewCT}}
    end;

run(read_only, KeyGen, _, State = #state{
    connection_pid=Pid,
    commit_time=CT,
    batch_reads=BatchReads,
    read_write_num=Reads,
    client_id=ClientId
}) ->
    Result = run_transaction(ClientId,
                             Pid,
                             CT,
                             KeyGen,
                             ignore,
                             {Reads, BatchReads},
                             {0, ignore}),

    case Result of
        {error, Reason} ->
            {error, Reason, State};

        {ok, NewCT} ->
            {ok, State#state{commit_time=NewCT}}
    end.

run_transaction(Id, Pid, LastCT, KeyGen, ValueGen, ReadConfig, WriteConfig) ->
    case start_transaction(Pid, LastCT) of
        {error, Reason} ->
            {error, {Id, Reason}};
        {ok, TxId} ->
            ReadResult = do_reads(Pid, TxId, KeyGen, ReadConfig),
            WriteResult = case ReadResult of
                {error, _}=ReadErr ->
                    convert_error(Id, ReadErr);

                {ok, Keys} ->
                    do_writes(Pid, TxId, Keys, KeyGen, ValueGen, ReadConfig, WriteConfig)
            end,

            CommitResult = case WriteResult of
                {error, _}=WriteErr ->
                    convert_error(Id, WriteErr);

                ok ->
                    commit_transaction(Pid, TxId)
            end,

            case CommitResult of
                {error, _}=CommitErr ->
                    convert_error(Id, CommitErr);

                {ok, NewCT} ->
                    {ok, NewCT}
            end
    end.

%% Associate worker id to error message
convert_error(Id, {error, {Id, _Reason}}=Err) ->
    Err;

convert_error(Id, {error, Reason}) ->
    {error, {Id, Reason}}.

%% Execute N reads, return the keys that were generated
do_reads(_Pid, _TxId, _KeyGen, {0, _}) ->
    {ok, []};

do_reads(Pid, TxId, KeyGen, {ReadN, BatchReads}) ->
    Keys = generate_keys(ReadN, KeyGen),
    Objects = [key_to_lww(K) || K <- Keys],
    case read_objects(Pid, Objects, TxId, BatchReads) of
        {ok, _RS} ->
            {ok, Keys};
        {error, Reason} ->
            {error, Reason}
    end.

%% Execute N writes, if given generated keys, pick a sample for the writes
%% If ReadN != 0 -> must WriteN <= ReadN
do_writes(_Pid, _TxId, _GivenKeys, _Keygen, _ValueGen, _ReadConfig, {0,_}) ->
    ok;

do_writes(Pid, TxId, GivenKeys, KeyGen, ValueGen, {ReadN, _}, {WriteN, BatchWrites}) ->
    UpdateKeys = case ReadN of
        0 ->
            generate_keys(WriteN, KeyGen);
        N ->
            lists:sublist(GivenKeys, N - WriteN + 1, WriteN)
    end,
    Updates = [key_to_lww_assign(K, ValueGen()) || K <- UpdateKeys],
    update_objects(Pid, Updates, TxId, BatchWrites).

start_transaction(Pid, CT) ->
    antidotec_pb:start_transaction(Pid, term_to_binary(CT), [{static, false}]).

read_objects(Pid, Objects, TxId, _Batch=false) ->
    Result = lists:map(fun(Obj) ->
        {ok, [Value]} = antidotec_pb:read_objects(Pid, [Obj], TxId),
        Value
    end, Objects),
    {ok, Result};

read_objects(Pid, Objects, TxId, _Batch=true) ->
    antidotec_pb:read_objects(Pid, Objects, TxId).

update_objects(Pid, Updates, TxId, _Batch=false) ->
    lists:foreach(fun(Update) ->
        ok = antidotec_pb:update_objects(Pid, [Update], TxId)
    end, Updates);

update_objects(Pid, Updates, TxId, _Batch=true) ->
    antidotec_pb:update_objects(Pid, Updates, TxId).

commit_transaction(Pid, TxId) ->
    case antidotec_pb:commit_transaction(Pid, TxId) of
        {ok, CT} ->
            {ok, binary_to_term(CT)};
        Error ->
            Error
    end.

key_to_lww(Key) ->
    BinKey = integer_to_binary(Key),
    {BinKey, ?TYPE, ?BUCKET}.

key_to_lww_assign(Key, Value) when is_integer(Value) ->
    key_to_lww_assign(Key, integer_to_binary(Value));

key_to_lww_assign(Key, Value) when is_binary(Value) ->
    Obj = key_to_lww(Key),
    {Obj, assign, Value}.

generate_keys(N, Gen) ->
    Seq = lists:seq(1, N),
    KeySet = lists:foldl(fun(_, AccSet) ->
        Key = unique_key(Gen, AccSet),
        sets:add_element(Key, AccSet)
    end, sets:new(), Seq),
    sets:to_list(KeySet).

unique_key(Gen, KeySet) ->
    K = Gen(),
    case sets:is_element(K, KeySet) of
        true ->
            unique_key(Gen, KeySet);
        false ->
            K
    end.
