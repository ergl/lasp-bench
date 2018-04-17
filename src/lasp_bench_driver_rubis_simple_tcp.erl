-module(lasp_bench_driver_rubis_simple_tcp).

-export([new/1,
         run/4]).

-include("lasp_bench.hrl").

-type key_gen(T) :: fun(() -> T).
-type value_gen(T) :: fun(() -> T).

-record(state, { socket :: gen_tcp:socket(), key_ratio :: {non_neg_integer(), non_neg_integer()} }).

new(_Id) ->
    _ = application:ensure_all_started(rubis_proto),
    Ip = lasp_bench_config:get(rubis_ip, '127.0.0.1'),
    Port = lasp_bench_config:get(rubis_port, 7878),
    Ratio = lasp_bench_config:get(ratio),

    Options = [binary, {active, false}, {packet, 2}],
    {ok, Sock} = gen_tcp:connect(Ip, Port, Options),
    {ok, #state{socket=Sock, key_ratio=Ratio}}.

run(ping, _, _, State = #state{socket=Sock}) ->
    ok = gen_tcp:send(Sock, rpb_simple_driver:ping()),
    {ok, BinReply} = gen_tcp:recv(Sock, 0),
    case rubis_proto:decode_serv_reply(BinReply) of
        {error, Reason} -> {error, Reason, State};
        ok -> {ok, State}
    end;

run(readonly, KeyGen, _, State = #state{socket=Sock, key_ratio={NReads, _}}) ->
    Keys = gen_keys(NReads, KeyGen),
    Msg = rpb_simple_driver:read_only(Keys),

    ok = gen_tcp:send(Sock, Msg),
    {ok, BinReply} = gen_tcp:recv(Sock, 0),

    Resp = rubis_proto:decode_serv_reply(BinReply),
    case Resp of
        {error, Reason} -> {error, Reason, State};
        ok -> {ok, State}
    end;

run(readwrite, KeyGen, ValueGen, State = #state{socket=Sock, key_ratio={NReads, NWrites}}) ->
    Keys = gen_keys(NReads, KeyGen),
    Updates = gen_updates(NWrites, KeyGen, ValueGen),
    Msg = rpb_simple_driver:read_write(Keys, Updates),

    ok = gen_tcp:send(Sock, Msg),
    {ok, BinReply} = gen_tcp:recv(Sock, 0),

    Resp = rubis_proto:decode_serv_reply(BinReply),
    case Resp of
        {error, Reason} -> {error, Reason, State};
        ok -> {ok, State}
    end.

%% Util
-spec gen_keys(non_neg_integer(), key_gen(binary())) -> [binary()].
gen_keys(N, K) ->
    gen_keys(N, K, []).

gen_keys(0, _, Acc) ->
    Acc;

gen_keys(N, K, Acc) ->
    gen_keys(N - 1, K, [K() | Acc]).

-spec gen_updates(non_neg_integer(), key_gen(binary()), value_gen(binary())) -> [{binary(), binary()}].
gen_updates(N, K, V) ->
    gen_updates(N, K, V, []).

gen_updates(0, _, _, Acc) ->
    Acc;

gen_updates(N, K, V, Acc) ->
    gen_updates(N - 1, K, V, [{K(), V()} | Acc]).
