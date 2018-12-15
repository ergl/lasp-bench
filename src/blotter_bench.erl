-module(blotter_bench).

-export([new/1,
         run/4,
         terminate/2]).

-include("lasp_bench.hrl").

-type key_gen(T) :: fun(() -> T).

-record(partition_info, {
    sockets :: orddict:orddict(inet:hostname(), gen_tcp:socket()),
    ring :: [inet:hostname()]
}).

-record(state, {
    local_socket :: gen_tcp:socket(),
    connection_mode :: local | {noproxy, #partition_info{}},
    read_keys :: non_neg_integer(),
    written_keys :: non_neg_integer(),
    key_ratio :: {non_neg_integer(), non_neg_integer()},
    abort_retries :: non_neg_integer()
}).

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
        local ->
            local;
        noproxy ->
            Ring = get_remote_ring(Sock),
            RemoteSockets = open_ring_sockets(Ip, Ring, Port, Options, [{Ip, Sock}]),
            {noproxy, #partition_info{ring=Ring,
                                      sockets=RemoteSockets}}
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
    route_to_node(fun do_noop/2, State);

run(ping, _, _, State) when ?local_conn(State) ->
    do_ping(State#state.local_socket, State);

run(ping, _, _, State) ->
    route_to_node(fun do_ping/2, State);

run(timed_read, K,  _, State) when ?local_conn(State) ->
    do_timed_read(State#state.local_socket, integer_to_binary(K(), 36), State);

run(timed_read, K, _, State) ->
    route_to_node(fun(Sock, FunState) ->
        do_timed_read(Sock, integer_to_binary(K(), 36), FunState)
    end, State);

run(readonly, KeyGen, _, State = #state{read_keys=1,
                                        connection_mode={noproxy, #partition_info{ring=Ring,sockets=Sockets}}}) ->

    Key = integer_to_binary(KeyGen(), 36),
    Target = blotter_partitioning:get_key_node(Key, Ring),
    case orddict:find(Target, Sockets) of
        error ->
            {error, unknown_target_node, State};

        {ok, Sock} ->
            Msg = rpb_simple_driver:read_only([Key]),
            ok = gen_tcp:send(Sock, Msg),
            {ok, BinReply} = gen_tcp:recv(Sock, 0),
            Resp = rubis_proto:decode_serv_reply(BinReply),
            case Resp of
                {error, Reason} ->
                    {error, Reason, State};
                ok ->
                    {ok, State}
            end

    end;


run(readonly, KeyGen, _, State = #state{local_socket=Sock, connection_mode=local, read_keys=NRead}) ->
    Keys = gen_keys(NRead, KeyGen),
    Msg = rpb_simple_driver:read_only(Keys),

    ok = gen_tcp:send(Sock, Msg),
    {ok, BinReply} = gen_tcp:recv(Sock, 0),

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
route_to_node(Fn, State) ->
    {noproxy, #partition_info{ring=Ring,sockets=Sockets}} = State#state.connection_mode,
    Target = lists:nth(rand:uniform(length(Ring)), Ring),
    case orddict:find(Target, Sockets) of
        error ->
            {error, unknown_target_node, State};
        {ok, Sock} ->
            Fn(Sock, State)
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

open_ring_sockets(SelfNode, Ring, Port, Options, Sockets) ->
    lists:foldl(fun(Node, AccSock) ->
        case Node of
            SelfNode ->
                AccSock;
            OtherNode ->
                {ok, Sock} = gen_tcp:connect(OtherNode, Port, Options),
                orddict:store(OtherNode, Sock, AccSock)
        end
    end, Sockets, Ring).
