#!/usr/bin/env escript
%%! -pa ../_build/default/lib/pvc_proto/ebin -Wall

%% A grep-like utility to search through pcap files (https://wiki.wireshark.org/Development/LibpcapFileFormat, used in
%% tcpdump and other libpcap-dependent utilities.
%%
%% The pcap file will contain a set of binary packets as captured by tcpdump. It contains bare ethernet, IP, and TCP
%% packets.
%%
%% Since our program uses a mix of protocol buffers and home-grown encoding, searching through the packets can be hard.
%% This tool can parse application-level messages and match on them, as well.

-mode(compile).

-define(REST_TABLE, rest_data_table).
-define(DEFAULT_PB_PORT, 7878).

-record(pcap_header, {
    endianess :: big | little,
    nano :: boolean(),
    major :: non_neg_integer(),
    minor :: non_neg_integer(),
    gmt_correction :: non_neg_integer(),
    ts_accuracy :: non_neg_integer(),
    snap_len :: non_neg_integer(),
    network :: ethernet
}).

-export([main/1]).

usage() ->
    Name = filename:basename(escript:script_name()),
    ok = io:fwrite(standard_error, "Usage: ~s [-dve] [-p pb_port] [--id msg_id] [--type msg_type] [--src-ip ip] [--dst-ip ip] [--src-port port] [--dst-port port] -f trace.pcap~n", [Name]).

main(Args) ->
    case parse_args(Args) of
        {error, Reason} ->
            io:fwrite(standard_error, "Wrong option: reason ~p~n", [Reason]),
            usage(),
            halt(1);
        {ok, Opts=#{file := FilePath}} ->
            %% This table will hold any extra data carried over from previous packets
            ?REST_TABLE = ets:new(?REST_TABLE, [set, named_table]),

            {ok, Contents} = file:read_file(FilePath),
            {_Header, Packets} = parse_header(Contents),
            filter_pcap_packets(Opts, Packets)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Output functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

print_packet(PacketInfo, Verbose, ShowData) ->
    #{tcp_id := {SrcIp, SrcPort, DstIp, DstPort}, payload := Msgs} = PacketInfo,
    case Msgs of
        {error, Reason} ->
            io:format("~s:~p \t~s:~p \tN/A \t~w~n", [inet:ntoa(SrcIp), SrcPort, inet:ntoa(DstIp), DstPort, {error, Reason}]),
            Verbose andalso print_extra(PacketInfo);
        _ ->
            [begin
                io:format("~s:~p \t~s:~p \tid=~b \t~p~n", [inet:ntoa(SrcIp), SrcPort, inet:ntoa(DstIp), DstPort, Id, Type]),
                Verbose andalso print_extra(PacketInfo),
                ShowData andalso io:format("~n~w~n~n", [Data])
             end || #{id := Id, pb_type := Type, pb_payload := Data} <- Msgs]
    end.

print_extra(PacketInfo) ->
    #{start := Secs, seq := Seq, ack := Ack} = PacketInfo,
    io:format("Time := ~b Seq := ~b ACK := ~b~n", [Secs, Seq, Ack]),
    Trimmed = maps:get(packet_trimmed, PacketInfo, false),
    Trimmed andalso io:format("(message trimmed)~n"),
    FlowId = maps:get(tcp_id, PacketInfo, ignore),
    FlowId =/= ignore andalso begin
        Bytes = get_extra_bytes(FlowId),
        Bytes =/= <<>> andalso io:format("Extra bytes := ~s~n", [bin_to_hexstr(Bytes)])
    end,
    io:format("~n").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% PCAP Parsing
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

parse_header(Contents) ->
    <<Magic:32, Rest/binary>> = Contents,
    case parse_ordering(Magic) of
        {ok, Order} ->
            parse_header(Order, Rest);
        Err ->
            Err
    end.

parse_ordering(16#A1B2C3D4) -> {ok, {big, false}};
parse_ordering(16#D4C3B2A1) -> {ok, {little, false}};
parse_ordering(16#A1B23C4D) -> {ok, {big, true}};
parse_ordering(16#4D2CB2A1) -> {ok, {little, true}};
parse_ordering(_) -> {error, bad_magic}.

parse_header({big, Nano}, Binary) ->
    <<VsnMajor:16/big-unsigned, VsnMinor:16/big-unsigned,
      ThisZone:32/big-signed, SigFigs:32/big-unsigned,
      SnapLen:32/big-unsigned, Network:32/big-unsigned, Packets/binary>> = Binary,
    {#pcap_header{endianess=big, nano=Nano,
                  major=VsnMajor, minor=VsnMinor,
                  gmt_correction=ThisZone, ts_accuracy=SigFigs,
                  snap_len=SnapLen, network=parse_network(Network)}, Packets};

parse_header({little, Nano}, Binary) ->
    <<VsnMajor:16/little-unsigned, VsnMinor:16/little-unsigned,
      ThisZone:32/little-signed, SigFigs:32/little-unsigned,
      SnapLen:32/little-unsigned, Network:32/little-unsigned, Packets/binary>> = Binary,
    {#pcap_header{endianess=little, nano=Nano,
                  major=VsnMajor, minor=VsnMinor,
                  gmt_correction=ThisZone, ts_accuracy=SigFigs,
                  snap_len=SnapLen, network=parse_network(Network)}, Packets}.

parse_network(1) -> ethernet;
parse_network(_) -> {error, unknown_network}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Packet Parsing (Ethernet, IP, TCP)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

filter_pcap_packets(_, <<>>) -> ok;
filter_pcap_packets(Opts, Bin) ->
    {Packet, Rest} = parse_packet(Opts, Bin),
    case Packet of
        skip -> ok;
        _ -> print_packet(Packet, maps:get(verbose, Opts, false), maps:get(print_data, Opts, false))
    end,
    filter_pcap_packets(Opts, Rest).

parse_packet(Opts, Bin) ->
    <<StartSec:32/little-unsigned, _:32, ByteLen:32/little-unsigned, OrigLen:32/little-unsigned, Packet:ByteLen/binary, Rest/binary>> = Bin,
    ParsedPacket = parse_ethernet_ip_level(Packet, Opts, #{packet_trimmed => OrigLen > ByteLen, start => start_offset(StartSec)}),
    {ParsedPacket, Rest}.

parse_ethernet_ip_level(Packet, Opts, PacketInfo) ->
    %% Packets are always TCP, so have Ethernet and IP headers
    <<_:112, _:96, RawSrcIp:32/bits, RawDstIp:32/bits, TCP_Packet/binary>> = Packet,
    SrcIp = to_addr(RawSrcIp),
    DstIp = to_addr(RawDstIp),
    case match_ip_level(Opts, SrcIp, DstIp) of
        false -> skip;
        true -> parse_tcp_level(TCP_Packet, Opts, PacketInfo#{src_ip => SrcIp, dst_ip => DstIp})
    end.

parse_tcp_level(Packet, Opts, PacketInfo=#{src_ip := SrcIp, dst_ip := DstIp}) ->
    <<SrcPort:16, DstPort:16, Seq:32, ACK:32, OffsetWords:4, _:3, Flags:9/bits, _/bits>> = Packet,
    case match_tcp_level(Opts, SrcPort, DstPort) of
        false -> skip;
        true ->
            Offset = OffsetWords * 4 * 8,
            <<_:Offset, RawData/binary>> = Packet,
            Conn = make_tcp_id(SrcIp, SrcPort, DstIp, DstPort),
            ACKSet = is_ack_set(Flags),
            NewPacketInfo = PacketInfo#{tcp_id => Conn, src_port => SrcPort, dst_port => DstPort,
                                        seq => rel_seq(Seq, Conn), ack => rel_ack(ACKSet, ACK, Conn)},
            case valid_protocol(Conn, Opts) of
                false ->
                    skip_on_error(Opts, NewPacketInfo, {error, bad_protocol});

                true ->
                    parse_app_level(RawData, Opts, NewPacketInfo)
            end
    end.

parse_app_level(<<>>, _, _) -> skip;
parse_app_level(Packet, Opts, PacketInfo=#{tcp_id := FlowId}) ->
    ExtraBytes = get_extra_bytes(FlowId),
    NewPacketPayload = <<ExtraBytes/binary, Packet/binary>>,
    case parse_data(16, PacketInfo, NewPacketPayload) of
        {ok, Data, Rest} ->
            ok = set_extra_bytes(FlowId, Rest),
            case match_app_level(Opts, Data) of
                [] -> skip;
                FilterData -> PacketInfo#{payload => FilterData}
            end;

        %% We got some bad data. It might be that this packet is a retransmission, in which case we
        %% can ignore the content (we have processed it before).
        %% TODO(borja): Have we?
        {error, {bad_data, _BadData}}=Err ->
            ok = clear_extra_bytes(FlowId),
            case reuse_ack(PacketInfo) of
                true ->
                    skip_on_error(Opts, PacketInfo, {error, retransmission});
                false ->
                    %% Give up, can't decide
                    Err
            end;

        %% If the entire packet doesn't contain enough data, stash it for later
        incomplete_data ->
            ok = set_extra_bytes(FlowId, NewPacketPayload),
            skip_on_error(Opts, PacketInfo, {error, incomplete});

        {error, Reason} ->
            ok = clear_extra_bytes(FlowId),
            skip_on_error(Opts, PacketInfo, {error, Reason})
    end.

match_ip_level(Opts, SrcIp, DstIp) ->
    MatchSrc = SrcIp =:= maps:get(src_ip, Opts, SrcIp),
    MatchDst = DstIp =:= maps:get(dst_ip, Opts, DstIp),
    MatchSrc andalso MatchDst.

match_tcp_level(Opts, SrcPort, DstPort) ->
    MatchSrc = SrcPort =:= maps:get(src_port, Opts, SrcPort),
    MatchDst = DstPort =:= maps:get(dst_port, Opts, DstPort),
    MatchSrc andalso MatchDst.

match_app_level(Opts, Msgs) ->
    lists:filtermap(fun(Msg=#{id := MsgId, pb_type := PbType}) ->
        MatchedId = maps:get(msg_id, Opts, MsgId),
        MatchedType = maps:get(msg_type, Opts, PbType),
        case {MatchedId, MatchedType} of
            {MsgId, PbType} -> {true, Msg};
            _ -> false
        end
    end, Msgs).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Protocol Parsing (Ethernet, IP, TCP)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Does this TCP flow match an application connection?
-spec valid_protocol(flow_id(), #{}) -> boolean().
valid_protocol({_, SrcPort, _, DstPort}, Opts) ->
    DefaultPBPort = maps:get(pb_port, Opts, ?DEFAULT_PB_PORT),
    (SrcPort =:= DefaultPBPort) orelse (DstPort =:= DefaultPBPort).

valid_data(Msgs) ->
    not lists:any(fun(<<>>) -> true; (_) -> false end, Msgs).

decode_msg(Len, DstPort, Msg) ->
    <<Id:Len, Content/binary>> = Msg,
    {Module, MsgType, MsgData} = pvc_proto:decode_client_req(Content),
    Payload = case DstPort of
        7878 -> MsgData;
        _ -> pvc_proto:decode_serv_reply(Content)
    end,
    #{id => Id, pb_mod => Module, pb_type => MsgType, pb_payload => Payload}.

parse_data(Len, #{dst_port := DstPort}, Data) ->
    case decode_data(Data) of
        {ok, Msgs, Rest} ->
            case valid_data(Msgs) of
                false ->
                    {error, {bad_data, Data}};
                true ->
                    MsgInfo = [decode_msg(Len, DstPort, Msg) || Msg <- Msgs],
                    {ok, MsgInfo, Rest}
            end;

        Other ->
            Other
    end.

%% Decode client buffer
-spec decode_data(Data :: binary()) -> {ok, [binary()], binary()} | {error, atom()} | incomplete_data.
decode_data(Data) ->
    case erlang:decode_packet(4, Data, []) of
        {more, _} ->
            incomplete_data;

        {error, Reason} ->
            {error, Reason};

        {ok, Message, More} ->
            decode_data_inner(More, [Message])
    end.

decode_data_inner(<<>>, Acc) ->
    {ok, Acc, <<>>};

decode_data_inner(Data, Acc) ->
    case erlang:decode_packet(4, Data, []) of
        {ok, Message, More} ->
            decode_data_inner(More, [Message | Acc]);
        _ ->
            {ok, Acc, Data}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% getopt
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

parse_args([]) -> {error, noargs};
parse_args(Args) ->
    case parse_args(Args, #{}) of
        {ok, Opts} -> required(Opts);
        Err -> Err
    end.

parse_args([], Acc) -> {ok, Acc};
parse_args([ [$- | Flag] | Args], Acc) ->
    case Flag of
        [$v] ->
            parse_args(Args, Acc#{verbose => true});
        [$e] ->
            parse_args(Args, Acc#{data_errors => true});
        [$d] ->
            parse_args(Args, Acc#{print_data => true});
        [$p] ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{pb_port => list_to_integer(Arg)} end);
        [$f] ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{file => Arg} end);
        "-id" ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{msg_id => list_to_integer(Arg)} end);
        "-type" ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{msg_type => list_to_atom(Arg)} end);
        "-src-ip" ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{src_ip => element(2, inet:parse_address(Arg))} end);
        "-dst-ip" ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{dst_ip => element(2, inet:parse_address(Arg))} end);
        "-src-port" ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{src_port => list_to_integer(Arg)} end);
        "-dst-port" ->
            parse_flag(Flag, Args, fun(Arg) -> Acc#{dst_port => list_to_integer(Arg)} end);
        [$h] ->
            usage(),
            halt(0);
        _ ->
            {error, {badarg, Flag}}
    end;

parse_args(_, _) ->
    {error, noarg}.

parse_flag(Flag, Args, Fun) ->
    case Args of
        [FlagArg | Rest] -> parse_args(Rest, Fun(FlagArg));
        _ -> {error, {noarg, Flag}}
    end.

required(Opts) ->
    Required = [file],
    Valid = lists:all(fun(F) -> maps:is_key(F, Opts) end, Required),
    case Valid of
        true -> {ok, Opts};
        false -> {error, "Missing required fields"}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Util functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Get the packet start second as offset from the first packet time
-spec start_offset(non_neg_integer()) -> non_neg_integer().
start_offset(TimeS) ->
    case erlang:get(first_start) of
        undefined ->
            erlang:put(first_start, TimeS),
            0;
        StartS ->
            TimeS - StartS
    end.

%% @doc Get the relative sequence number for this TCP flow
-spec rel_seq(non_neg_integer(), flow_id()) -> non_neg_integer().
rel_seq(Seq, TCPConn) ->
    case erlang:get({TCPConn, seq}) of
        undefined ->
            erlang:put({TCPConn, seq}, Seq),
            Seq;
        FirstSeq ->
            Seq - FirstSeq
    end.

%% @doc Get the relative ack number for this TCP flow
-spec rel_ack(boolean(), non_neg_integer(), flow_id()) -> non_neg_integer().
rel_ack(false, _, _) -> ignore;
rel_ack(true, ACK, TCPConn) ->
    erlang:put({TCPConn, last_ack}, ACK),
    case erlang:get({TCPConn, ack}) of
        undefined ->
            erlang:put({TCPConn, ack}, ACK),
            erlang:put({TCPConn, last_ack}, 1),
            1;
        FirstACK ->
            %% ACKs start at 1, not 0
            OffsetACK = 1 + (ACK - FirstACK),
            erlang:put({TCPConn, last_ack}, OffsetACK),
            OffsetACK
    end.

%% @doc Is this TCP packet an ACK?
-spec is_ack_set(bitstring()) -> boolean().
is_ack_set(<<_:4, 1:1, _:4>>) -> true;
is_ack_set(<<_:4, 0:1, _:4>>) -> false.

-type flow_id() :: {inet:ip4_address(), non_neg_integer(), inet:ip4_address(), non_neg_integer()}.
-spec make_tcp_id(inet:ip4_address(), non_neg_integer(), inet:ip4_address(), non_neg_integer()) -> flow_id().
make_tcp_id(SrcIp, SrcPort, DstIp, DstPort) ->
    {SrcIp, SrcPort, DstIp, DstPort}.

-spec to_addr(bitstring()) -> inet:ip4_address().
to_addr(<<A,B,C,D>>) -> {A,B,C,D}.

%% @doc Get any extra bytes from a previous packet in the flow
-spec get_extra_bytes(flow_id()) -> bitstring().
get_extra_bytes(FlowId) ->
    case ets:lookup(?REST_TABLE, FlowId) of
        [{FlowId, Binary}] -> Binary;
        [] -> <<>>
    end.

%% @doc Save any extra bytes from protocol parsing
-spec set_extra_bytes(flow_id(), bitstring()) -> ok.
set_extra_bytes(FlowId, Bytes) ->
    true = ets:insert(?REST_TABLE, {FlowId, Bytes}),
    ok.

-spec clear_extra_bytes(flow_id()) -> ok.
clear_extra_bytes(FlowId) ->
    true = ets:insert(?REST_TABLE, {FlowId, <<>>}),
    ok.

%% @doc Skip packet on error, unless user requested to see errors
-spec skip_on_error(#{}, #{}, {error, atom()}) -> skip | {error, atom()}.
skip_on_error(Opts, PacketInfo, Error) ->
    case maps:get(data_errors, Opts, false) of
        false -> skip;
        true -> PacketInfo#{payload => Error}
    end.

%% @doc Have we seen the ACK from this packet before?
-spec reuse_ack(#{}) -> boolean().
reuse_ack(#{tcp_id := Id, ack := CurrentACK}) ->
    case erlang:get({Id, last_ack}) of
        CurrentACK -> true;
        _ -> false
    end.

%% Hextream parsing
%% Taken from https://github.com/b/hex, and that itself
%% from http://necrobious.blogspot.com/2008/03/binary-to-hex-string-back-to-binary-in.html
bin_to_hexstr(Bin) ->
    lists:flatten([io_lib:format("~2.16.0B", [X]) || X <- binary_to_list(Bin)]).
