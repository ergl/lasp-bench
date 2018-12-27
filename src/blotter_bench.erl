-module(blotter_bench).

-export([new/1,
         run/4,
         terminate/2]).

-include("lasp_bench.hrl").

-type key_gen(T) :: fun(() -> T).

-record(partition_info, {
    sockets :: orddict:orddict(inet:hostname(), gen_tcp:socket()),
    unique_nodes :: ordsets:ordset(inet:hostname()),
    ring :: [inet:hostname()]
}).

-record(state, {
    local_socket :: gen_tcp:socket(),
    connection_mode :: local | #partition_info{},
    read_keys :: non_neg_integer(),
    written_keys :: non_neg_integer(),
    key_ratio :: {non_neg_integer(), non_neg_integer()},
    abort_retries :: non_neg_integer()
}).

-type state() :: #state{}.

-define(local_conn(State), State#state.connection_mode =:= local).

new(_Id) ->
    _ = application:ensure_all_started(rubis_proto),
    Ip = lasp_bench_config:get(rubis_ip, '127.0.0.1'),
    Port = lasp_bench_config:get(rubis_port, 7878),
    Options = [binary, {active, false}, {packet, 2}, {nodelay, true}],

    NRead = lasp_bench_config:get(read_keys),
    NWrite = lasp_bench_config:get(written_keys),
    RWRatio = lasp_bench_config:get(ratio),
    AbortTries = lasp_bench_config:get(abort_retries, 50),

    {ok, Sock} = gen_tcp:connect(Ip, Port, Options),
    Mode = lasp_bench_config:get(connection_mode, local),
    ConnMode = case Mode of
        local -> local;
        noproxy ->
            Ring = get_remote_ring(Sock),
            UniqueNodes = ordsets:from_list(Ring),
            RemoteSockets = open_ring_sockets(Ip, UniqueNodes, Port, Options, [{Ip, Sock}]),
            #partition_info{ring=Ring, unique_nodes=UniqueNodes, sockets=RemoteSockets}
    end,

    {ok, #state{local_socket=Sock,
                connection_mode=ConnMode,
                read_keys=NRead,
                key_ratio=RWRatio,
                written_keys=NWrite,
                abort_retries=AbortTries}}.

terminate(_Reason, _State) ->
    ok.

run(noop, _, _, State) when ?local_conn(State) ->
    do_noop(State#state.local_socket, State);

run(noop, _, _, State) ->
    with_random_node(fun do_noop/2, State);

run(ping, _, _, State) when ?local_conn(State) ->
    do_ping(State#state.local_socket, State);

run(ping, _, _, State) ->
    with_random_node(fun do_ping/2, State);

run(timed_read, K,  _, State) when ?local_conn(State) ->
    do_timed_read(State#state.local_socket, integer_to_binary(K(), 36), State);

run(timed_read, K, _, State) ->
    Key = integer_to_binary(K(), 36),
    route_to_node(fun do_timed_read/3, Key, State);

run(readonly, K, _, State=#state{local_socket=Socket, read_keys=NRead}) when ?local_conn(State) ->
    Keys = gen_keys(NRead, K),
    Msg = rpb_simple_driver:read_only(Keys),

    ok = gen_tcp:send(Socket, Msg),
    {ok, BinReply} = gen_tcp:recv(Socket, 0),

    Resp = rubis_proto:decode_serv_reply(BinReply),
    case Resp of
        {error, Reason} ->
            {error, Reason, State};

        ok ->
            {ok, State}
    end;

run(writeonly, KeyGen, ValueGen, State = #state{local_socket=Sock,
                                                written_keys=W,
                                                abort_retries=Tries}) ->

    Keys = gen_keys(W, KeyGen),
    Updates = lists:map(fun(K) -> {K, ValueGen()} end, Keys),
    Msg = rpb_simple_driver:read_write(Keys, Updates),

    perform_write(Sock, Msg, State, Tries);

run(readwrite, KeyGen, ValueGen, State = #state{local_socket=Sock,
                                                key_ratio={R, W},
                                                abort_retries=Tries}) ->
    Total = erlang:max(R, W),
    Keys = gen_keys(Total, KeyGen),

    %% Make Updates from the first W keys
    Updates = lists:map(fun(K) -> {K, ValueGen()} end, lists:sublist(Keys, W)),
    Msg = rpb_simple_driver:read_write(Keys, Updates),

    perform_write(Sock, Msg, State, Tries).

perform_write(_, _, State, 0) ->
    {error, too_many_retries, State};

perform_write(Sock, Msg, State, Retries) ->
    ok = gen_tcp:send(Sock, Msg),
    {ok, BinReply} = gen_tcp:recv(Sock, 0),

    Resp = rubis_proto:decode_serv_reply(BinReply),
    case Resp of
        {error, _} ->
            perform_write(Sock, Msg, State, Retries - 1);
        ok ->
            {ok, State}
    end.

do_noop(Socket, State) ->
    SendTime = os:timestamp(),
    ok = gen_tcp:send(Socket, rpb_simple_driver:timed_noop()),
    {ok, BinReply} = gen_tcp:recv(Socket, 0),
    ReplyTime = os:timestamp(),
    case rubis_proto:decode_serv_reply(BinReply) of
        {no_op, ServerTime={_, _, _}} ->
            {ok, {noop, SendTime, ServerTime, ReplyTime, State}};
        Other ->
            {error, Other, State}
    end.

do_ping(Socket, State) ->
    SendTime = os:timestamp(),
    ok = gen_tcp:send(Socket, rpb_simple_driver:timed_ping()),
    {ok, BinReply} = gen_tcp:recv(Socket, 0),
    ReplyTime = os:timestamp(),
    case rubis_proto:decode_serv_reply(BinReply) of
        {ping, StampMap} ->
            {ok, {ping, SendTime, StampMap, ReplyTime, State}};
        Other ->
            {error, Other, State}
    end.

do_timed_read(Socket, Key, State) ->
    Msg = rpb_simple_driver:timed_read(Key),
    SendTime = os:timestamp(),
    ok = gen_tcp:send(Socket, Msg),
    {ok, BinReply} = gen_tcp:recv(Socket, 0),
    ReplyTime = os:timestamp(),
    Resp = rubis_proto:decode_serv_reply(BinReply),
    case Resp of
        {error, Reason} ->
            {error, Reason, State};
        {ok, StampMap} ->
            {ok, {timed_read, SendTime, StampMap, ReplyTime, State}}
    end.

%% Util
-spec with_random_node(fun((inet:socket(), state()) -> _), state()) -> _.
with_random_node(Fn, State) when is_function(Fn, 2) ->
    #partition_info{unique_nodes=Nodes,sockets=Sockets} = State#state.connection_mode,
    Target = lists:nth(rand:uniform(length(Nodes)), Nodes),
    case orddict:find(Target, Sockets) of
        error ->
            {error, unknown_target_node, State};
        {ok, Sock} ->
            Fn(Sock, State)
    end.

-spec route_to_node(fun((inet:socket(), binary(), state()) -> _), binary(), state()) -> _.
route_to_node(Fn, Key, State) when is_function(Fn, 3) ->
    #partition_info{ring=Ring,sockets=Sockets} = State#state.connection_mode,
    Target = blotter_partitioning:get_key_node(Key, Ring),
    case orddict:find(Target, Sockets) of
        error ->
            {error, unknown_target_node, State};
        {ok, Sock} ->
            Fn(Sock, Key, State)
    end.

-spec gen_keys(non_neg_integer(), key_gen(non_neg_integer())) -> [binary()].
gen_keys(N, K) ->
    gen_keys(N, K, []).

gen_keys(0, _, Acc) ->
    Acc;

gen_keys(N, K, Acc) ->
    gen_keys(N - 1, K, [integer_to_binary(K(), 36) | Acc]).

get_remote_ring(Sock) ->
    ok = gen_tcp:send(Sock, rpb_simple_driver:partitions()),
    {ok, BinReply} = gen_tcp:recv(Sock, 0),
    {ring, RingData} = rubis_proto:decode_serv_reply(BinReply),
    blotter_partitioning:new(RingData).

%% @doc Given a list of nodes, open sockets to all except the current node
-spec open_ring_sockets(
    inet:hostname(),
    ordsets:ordset(inet:hostname()),
    inet:port_number(),
    proplists:proplist(),
    orddict:orddict(inet:socket(), inet:socket())
) -> orddict:orddict(inet:hostname(), inet:socket()).

open_ring_sockets(SelfNode, Nodes, Port, Options, Sockets) ->
    ordsets:fold(fun(Node, AccSock) ->
        case Node of
            SelfNode ->
                AccSock;

            OtherNode ->
                {ok, Sock} = gen_tcp:connect(OtherNode, Port, Options),
                orddict:store(OtherNode, Sock, AccSock)
        end
    end, Sockets, Nodes).
