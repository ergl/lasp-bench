-module(blotter_bench).

%% Ignore xref for all lasp bench drivers
-ignore_xref([new/1, run/4]).

-export([new/1,
         run/4]).

-include("lasp_bench.hrl").

-type tx_id() :: {non_neg_integer(), non_neg_integer()}.
-type key_gen(T) :: fun(() -> T).

-record(state, {
    %% Id of this worker thread
    worker_id :: non_neg_integer(),
    %% Auto incremented counter to create unique transaction ids
    transaction_count :: non_neg_integer(),

    %% Client coordinator state
    coord_state :: pvc:coord_state(),

    %% Keys to read in readonly op
    keys_to_read :: non_neg_integer(),
    %% Ratio of read/writes in readwrite op
    mixed_readwrite_ratio :: {non_neg_integer(), non_neg_integer()},

    retry_until_commit :: boolean()
}).

new(Id) ->
    KeysToRead = lasp_bench_config:get(read_keys),
    ReadWriteRatio = lasp_bench_config:get(ratio),
    RetryUntilCommit = lasp_bench_config:get(retry_aborts, true),

    Protocol = lasp_bench_config:get(client_protocol, psi),
    RingInfo = ets:lookup_element(hook_pvc, ring, 2),
    Connections = hook_pvc:conns_for_worker(Id),

    %% Use our worker id to generate the messages for this coordinator
    {ok, CoordState} = pvc:new(RingInfo, Connections, Id, Protocol),

    State = #state{worker_id = Id,
                   transaction_count = 0,
                   coord_state = CoordState,
                   keys_to_read = KeysToRead,
                   mixed_readwrite_ratio = ReadWriteRatio,
                   retry_until_commit = RetryUntilCommit},

    {ok, State}.

run(ping, _, _, State = #state{worker_id=Id, coord_state=CoordState}) ->
    %% Hack to get raw socket field from connection
    {coord_state, _, _, ConnectionsDict} = CoordState,

    %% Get a node from the cluster at random
    RandomChoice = rand:uniform(orddict:size(ConnectionsDict)),
    {_, Connection} = lists:nth(RandomChoice, ConnectionsDict),
    {ok, BinReply} = pvc_connection:send(Connection, Id, ppb_simple_driver:ping(), 5000),
    case pvc_proto:decode_serv_reply(BinReply) of
        {error, Reason} ->
            {error, Reason, State};
        ok ->
            {ok, State}
    end;

run(readonly_track, KeyGen, _, State = #state{coord_state=CoordState}) ->
    {ok, Tx} = pvc:start_transaction(CoordState, next_tx_id(State)),
    Sent = os:timestamp(),
    case pvc:read(CoordState, Tx, KeyGen()) of
        {abort, AbortReason} ->
            {error, AbortReason, incr_tx_id(State)};

        {ok, StampMap, Tx1} ->
            Received = os:timestamp(),
            ok = pvc:commit(CoordState, Tx1),
            %% Return stamp map for further processing
            {ok, {track_reads, Sent, Received, StampMap, incr_tx_id(State)}}
    end;

run(readonly, KeyGen, _, State = #state{keys_to_read=1}) ->
    perform_readonly_tx(KeyGen(), State, 1);

run(readonly, KeyGen, _, State = #state{keys_to_read=KeysToRead,
                                        retry_until_commit=Retry}) ->

    Keys = gen_keys(KeysToRead, KeyGen),
    perform_readonly_tx(Keys, State, Retry);

run(readwrite, KeyGen, ValueGen, State = #state{mixed_readwrite_ratio={ToRead, ToWrite},
                                                retry_until_commit=Retry}) ->

    %% If readwrite is {2, 1}, means we want to read 2 keys, update 1
    %% If it is {1, 2}, we need to read 2, then update 2 (no blind writes)

    %% We pick the max of read and write to read all the keys we need
    Total = erlang:max(ToRead, ToWrite),
    Keys = gen_keys(Total, KeyGen),

    %% Then perform updates on the sublist of total keys (ToWrite)
    Updates = lists:map(fun(K) -> {K, ValueGen()} end, lists:sublist(Keys, ToWrite)),
    perform_write_tx(Keys, Updates, State, Retry).

perform_readonly_tx(Keys, State=#state{coord_state=CoordState}, ShouldRetry) ->
    {ok, Tx} = pvc:start_transaction(CoordState, next_tx_id(State)),
    case pvc:read(CoordState, Tx, Keys) of
        {abort, _AbortReason}=ReadErr ->
            %% Will only abort under PSI or SER
            maybe_retry_readonly(Keys, incr_tx_id(State), ReadErr, ShouldRetry);

        {ok, _, Tx1} ->
            case pvc:commit(CoordState, Tx1) of
                {error, Reason} ->
                    %% TODO(borja): Can this error happen?
                    {error, Reason, incr_tx_id(State)};

                {abort, _AbortReason}=CommitErr ->
                    %% Can only abort under SER
                    maybe_retry_readonly(Keys, incr_tx_id(State), CommitErr, ShouldRetry);

                ok ->
                    {ok, incr_tx_id(State)}
            end
    end.

maybe_retry_readonly(Keys, State, _Err, true) ->
    perform_readonly_tx(Keys, State, true);
maybe_retry_readonly(_Keys, State, Err, false) ->
    {error, Err, State}.

perform_write_tx(Keys, Updates, State=#state{coord_state=Conn}, ShouldRetry) ->
    {ok, Tx} = pvc:start_transaction(Conn, next_tx_id(State)),
    case pvc:read(Conn, Tx, Keys) of
        {abort, _AbortReason}=ReadErr ->
            maybe_retry_readwrite(Keys, Updates, incr_tx_id(State), ReadErr, ShouldRetry);

        {ok, _, Tx1} ->
            {ok, Tx2} = pvc:update(Conn, Tx1, Updates),
            case pvc:commit(Conn, Tx2) of
                {error, Reason} ->
                    %% TODO(borja): Can this error happen?
                    {error, Reason, incr_tx_id(State)};

                {abort, _AbortReason}=CommitErr ->
                    maybe_retry_readwrite(Keys, Updates, incr_tx_id(State), CommitErr, ShouldRetry);

                ok ->
                    {ok, incr_tx_id(State)}
            end
    end.

maybe_retry_readwrite(Keys, Updates, State, _Err, Retry=true) ->
    perform_write_tx(Keys, Updates, State, Retry);
maybe_retry_readwrite(_Keys, _Updates, State, Err, false) ->
    {error, Err, State}.

%%====================================================================
%% Util functions
%%====================================================================

%% @doc Generate N random keys
-spec gen_keys(non_neg_integer(), key_gen(non_neg_integer())) -> binary() | [binary()].
gen_keys(N, K) ->
    gen_keys(N, K, []).

gen_keys(0, _, Acc) ->
    Acc;

gen_keys(N, K, Acc) ->
    gen_keys(N - 1, K, [integer_to_binary(K(), 36) | Acc]).

-spec next_tx_id(#state{}) -> tx_id().
next_tx_id(#state{worker_id=Id, transaction_count=N}) ->
    {Id, N}.

-spec incr_tx_id(#state{}) -> #state{}.
incr_tx_id(State=#state{transaction_count=N}) ->
    State#state{transaction_count = N + 1}.
