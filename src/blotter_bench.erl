-module(blotter_bench).

-export([new/1,
         run/4]).

-include("lasp_bench.hrl").

-type key_gen(T) :: fun(() -> T).

-record(state, {
    %% Connection to the main target node
    main_socket :: gen_tcp:socket(),

    %% Port to connect to
    remote_port :: inet:port_number(),
    socket_options :: [gen_tcp:connect_option()],
    %% Mapping of other nodes -> sockets,
    %% used for rerouting reads
    remote_sockets :: orddict:orddict(inet:hostname(), gen_tcp:socket()),
    %% Remote partitoning
    remote_ring :: [atom()],

    read_keys :: non_neg_integer(),
    written_keys :: non_neg_integer(),
    key_ratio :: {non_neg_integer(), non_neg_integer()},
    abort_retries :: non_neg_integer()
}).

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

new(_Id) ->
    _ = application:ensure_all_started(rubis_proto),
    Ip = lasp_bench_config:get(rubis_ip, '127.0.0.1'),
    Port = lasp_bench_config:get(rubis_port, 7878),
    NRead = lasp_bench_config:get(read_keys),
    NWrite = lasp_bench_config:get(written_keys),
    RWRatio = lasp_bench_config:get(ratio),
    AbortTries = lasp_bench_config:get(abort_retries, 50),

    Options = [binary, {active, false}, {packet, 2}, {nodelay, true}],
    {ok, Sock} = gen_tcp:connect(Ip, Port, Options),
    Ring = get_remote_ring(Sock),
    {ok, #state{main_socket=Sock,
                remote_port=Port,
                socket_options=Options,
                remote_sockets=open_ring_sockets(Ip, Ring, Port, Options, [{Ip, Sock}]),
                remote_ring=Ring,
                read_keys=NRead,
                key_ratio=RWRatio,
                written_keys=NWrite,
                abort_retries=AbortTries}}.

run(ping, _, _, State = #state{main_socket=Sock}) ->
    ok = gen_tcp:send(Sock, rpb_simple_driver:ping()),
    {ok, BinReply} = gen_tcp:recv(Sock, 0),
    case rubis_proto:decode_serv_reply(BinReply) of
        {error, Reason} -> {error, Reason, State};
        ok -> {ok, State}
    end;

run(readonly, KeyGen, _, State = #state{read_keys=1, remote_ring=Ring, remote_sockets=Sockets}) ->
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
                {request_to, _} ->
                    {error, too_many_reroutes, State};
                {error, Reason} ->
                    {error, Reason, State};
                ok ->
                    {ok, State}
            end

    end;


run(readonly, KeyGen, _, State = #state{main_socket=Sock, read_keys=NRead}) ->
    Keys = gen_keys(NRead, KeyGen),
    Msg = rpb_simple_driver:read_only(Keys),

    ok = gen_tcp:send(Sock, Msg),
    {ok, BinReply} = gen_tcp:recv(Sock, 0),

    Resp = rubis_proto:decode_serv_reply(BinReply),
    case Resp of
        {request_to, BinIp} ->
            reroute_read(binary_to_atom(BinIp, latin1), Msg, State);

        {error, Reason} ->
            {error, Reason, State};

        ok ->
            {ok, State}
    end;

run(writeonly, KeyGen, ValueGen, State = #state{main_socket=Sock,
                                                written_keys=W,
                                                abort_retries=Tries}) ->

    Keys = gen_keys(W, KeyGen),
    Updates = lists:map(fun(K) -> {K, ValueGen()} end, Keys),
    Msg = rpb_simple_driver:read_write(Keys, Updates),

    perform_write(Sock, Msg, State, Tries);

run(readwrite, KeyGen, ValueGen, State = #state{main_socket=Sock,
                                                key_ratio={R, W},
                                                abort_retries=Tries}) ->
    Total = erlang:max(R, W),
    Keys = gen_keys(Total, KeyGen),

    %% Make Updates from the first W keys
    Updates = lists:map(fun(K) -> {K, ValueGen()} end, lists:sublist(Keys, W)),
    Msg = rpb_simple_driver:read_write(Keys, Updates),

    perform_write(Sock, Msg, State, Tries).

reroute_read(Ip, Msg, State = #state{remote_port=Port,
                                     socket_options=Options,
                                     remote_sockets=Sockets}) ->

    {Sock, NewSockets} = case orddict:find(Ip, Sockets) of
        {ok, Socket} ->
            {Socket, Sockets};

        error ->
            {ok, Socket} = gen_tcp:connect(Ip, Port, Options),
            {Socket, orddict:store(Ip, Socket, Sockets)}
    end,

    ok = gen_tcp:send(Sock, Msg),
    {ok, BinReply} = gen_tcp:recv(Sock, 0),

    Resp = rubis_proto:decode_serv_reply(BinReply),
    NewState = State#state{remote_sockets=NewSockets},
    case Resp of
        {request_to, _} ->
            {error, too_many_reroutes, NewState};

        {error, Reason} ->
            {error, Reason, NewState};

        ok ->
            {ok, NewState}

    end.

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
