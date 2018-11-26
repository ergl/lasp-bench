-module(blotter_bench).

-export([new/1,
         run/4]).

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

run(ping, _, _, State = #state{local_socket=Sock, connection_mode=local}) ->
    ok = gen_tcp:send(Sock, rpb_simple_driver:ping()),
    {ok, BinReply} = gen_tcp:recv(Sock, 0),
    case rubis_proto:decode_serv_reply(BinReply) of
        {error, Reason} -> {error, Reason, State};
        ok -> {ok, State}
    end;

run(ping, _, _, State = #state{connection_mode={noproxy, #partition_info{ring=Ring,sockets=Sockets}}}) ->
    Target = lists:nth(rand:uniform(length(Ring)), Ring),
    case orddict:find(Target, Sockets) of
        error ->
            {error, unknown_target_node, State};
        {ok, Sock} ->
            ok = gen_tcp:send(Sock, rpb_simple_driver:ping()),
            {ok, BinReply} = gen_tcp:recv(Sock, 0),
            case rubis_proto:decode_serv_reply(BinReply) of
                {error, Reason} -> {error, Reason, State};
                ok -> {ok, State}
            end
    end;

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

%% Util
-spec gen_keys(non_neg_integer(), key_gen(non_neg_integer())) -> [binary()].
gen_keys(N, K) ->
    gen_keys(N, K, []).

gen_keys(0, _, Acc) ->
    Acc;

gen_keys(N, K, Acc) ->
    gen_keys(N - 1, K, [integer_to_binary(K(), 36) | Acc]).

get_remote_ring(Sock) ->
    ok = gen_tcp:send(Sock, rpb_simple_driver:get_ring()),
    {ok, BinReply} = gen_tcp:recv(Sock, 0),
    Data = rubis_proto:decode_serv_reply(BinReply),
    blotter_partitioning:new(Data).

open_ring_sockets(SelfNode, Ring, Port, Options, Sockets) ->
    Unique = ordsets:from_list(Ring),
    ordsets:fold(fun(Node, AccSock) ->
        case Node of
            SelfNode ->
                AccSock;

            OtherNode ->
                {ok, Sock} = gen_tcp:connect(OtherNode, Port, Options),
                orddict:store(Node, Sock, AccSock)
        end
                 end, Sockets, Unique).
