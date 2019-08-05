-module(blotter_bench).

%% Ignore xref for all lasp bench drivers
-ignore_xref([new/1, run/4]).

-export([new/1,
         run/4]).

-include("lasp_bench.hrl").

-type tx_id() :: {non_neg_integer(), non_neg_integer()}.
-type key_gen(T) :: fun(() -> T).
-type abort_retries() :: non_neg_integer() | infinity.

-record(state, {
    %% Id of this worker thread
    worker_id :: non_neg_integer(),
    %% Auto incremented counter to create unique transaction ids
    transaction_count :: non_neg_integer(),

    %% Client coordinator state
    coord_state :: pvc:coord_state(),

    %% Keys to read in readonly op
    keys_to_read :: non_neg_integer(),
    %% Keys to write in writeonly op
    keys_to_write :: non_neg_integer(),
    %% Ratio of read/writes in readwrite op
    mixed_readwrite_ratio :: {non_neg_integer(), non_neg_integer()},

    %% Number of retries on aborted transaction
    abort_retries :: abort_retries()
}).

new(Id) ->
    KeysToRead = lasp_bench_config:get(read_keys),
    KeysToWrite = lasp_bench_config:get(written_keys),
    ReadWriteRatio = lasp_bench_config:get(ratio),
    AbortTries = lasp_bench_config:get(abort_retries, infinity),

    RingInfo = ets:lookup_element(hook_pvc, ring, 2),
    Connections = hook_pvc:conns_for_worker(Id),

    %% Use our worker id to generate the messages for this coordinator
    {ok, CoordState} = pvc:new(RingInfo, Connections, Id),

    State = #state{worker_id = Id,
                   transaction_count = 0,
                   coord_state = CoordState,
                   keys_to_read = KeysToRead,
                   keys_to_write = KeysToWrite,
                   mixed_readwrite_ratio = ReadWriteRatio,
                   abort_retries = AbortTries},

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
                                        abort_retries=Tries}) ->

    Keys = gen_keys(KeysToRead, KeyGen),
    perform_readonly_tx(Keys, State, Tries);

run(writeonly, KeyGen, ValueGen, State = #state{keys_to_write=KeysToWrite,
                                                abort_retries=Tries}) ->

    Keys = gen_keys(KeysToWrite, KeyGen),
    Updates = lists:map(fun(K) -> {K, ValueGen()} end, Keys),
    perform_write_tx(Keys, Updates, State, Tries);

run(readwrite, KeyGen, ValueGen, State = #state{mixed_readwrite_ratio={ToRead, ToWrite},
                                                abort_retries=Tries}) ->

    %% If readwrite is {2, 1}, means we want to read 2 keys, update 1
    %% If it is {1, 2}, we need to read 2, then update 2 (no blind writes)

    %% We pick the max of read and write to read all the keys we need
    Total = erlang:max(ToRead, ToWrite),
    Keys = gen_keys(Total, KeyGen),

    %% Then perform updates on the sublist of total keys (ToWrite)
    Updates = lists:map(fun(K) -> {K, ValueGen()} end, lists:sublist(Keys, ToWrite)),
    perform_write_tx(Keys, Updates, State, Tries).


%% @doc Try a readonly transaction N times before giving up
perform_readonly_tx(_, State, 0) ->
    {error, too_many_retries, State};

perform_readonly_tx(Keys, State=#state{coord_state=CoordState}, Tries) ->
    {ok, Tx} = pvc:start_transaction(CoordState, next_tx_id(State)),
    case pvc:read(CoordState, Tx, Keys) of
        {abort, _AbortReason} ->
            perform_readonly_tx(Keys, incr_tx_id(State), next_try(Tries));
        {ok, _, Tx1} ->
            %% Readonly transactions always commit
            ok = pvc:commit(CoordState, Tx1),
            {ok, incr_tx_id(State)}
    end.

%% @doc Try a writeonly transaction N times before giving up
%%
%%      Since we can't perform blind writes, go through a read before
%%
perform_write_tx(_, _, State, 0) ->
    {error, too_many_retries, State};

perform_write_tx(Keys, Updates, State=#state{coord_state=Conn}, Tries) ->
    {ok, Tx} = pvc:start_transaction(Conn, next_tx_id(State)),
    case pvc:read(Conn, Tx, Keys) of
        {abort, _AbortReason} ->
            perform_write_tx(Keys, Updates, incr_tx_id(State), next_try(Tries));

        {ok, _, Tx1} ->
            {ok, Tx2} = pvc:update(Conn, Tx1, Updates),
            case pvc:commit(Conn, Tx2) of
                {error, Reason} ->
                    %% TODO(borja): Can this error happen?
                    {error, Reason, incr_tx_id(State)};

                {abort, _AbortReason} ->
                    perform_write_tx(Keys, Updates, incr_tx_id(State), next_try(Tries));

                ok ->
                    {ok, incr_tx_id(State)}
            end
    end.

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

-spec next_try(abort_retries()) -> abort_retries().
next_try(infinity) -> infinity;
next_try(N) -> N - 1.
