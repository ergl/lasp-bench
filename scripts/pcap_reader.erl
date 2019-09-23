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
    ok = io:fwrite(standard_error, "Usage: ~s [-v] [--id msg_id] [--type msg_type] [--src-ip ip] [--dst-ip ip] [--src-port port] [--dst-port port] -f trace.pcap~n", [Name]).

main(Args) ->
    case parse_args(Args) of
        {error, Reason} ->
            io:fwrite("Wrong option: reason ~p~n", [Reason]),
            usage();
        {ok, Opts=#{file := FilePath}} ->
            {ok, Contents} = file:read_file(FilePath),
            {_Header, Packets} = parse_header(Contents),
            filter_pcap_packets(Opts, Packets)
    end.

print_packet(PacketInfo, Verbose) ->
    #{src_ip := SrcIp, src_port := SrcPort,
      dst_ip := DstIp, dst_port := DstPort, payload := Msgs} = PacketInfo,
    [begin
         io:format("~s:~p \t ~s:~p\t~p\t~p~n", [inet:ntoa(SrcIp), SrcPort, inet:ntoa(DstIp), DstPort, Id, Type]),
         Verbose andalso begin
             io:format("~w~n", [Data])
         end
     end || #{id := Id, pb_type := Type, pb_payload := Data} <- Msgs].

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
        _ -> print_packet(Packet, maps:get(verbose, Opts, false))
    end,
    filter_pcap_packets(Opts, Rest).

parse_packet(Opts, Bin) ->
    <<_:32, _:32, ByteLen:32/little-unsigned, _:32, Packet:ByteLen/binary, Rest/binary>> = Bin,
    ParsedPacket = parse_ethernet_ip_level(Packet, Opts),
    {ParsedPacket, Rest}.

parse_ethernet_ip_level(Packet, Opts) ->
    %% Packets are always TCP, so have Ethernet and IP headers
    <<_:112, _:96, RawSrcIp:32/bits, RawDstIp:32/bits, TCP_Packet/binary>> = Packet,
    SrcIp = to_addr(RawSrcIp),
    DstIp = to_addr(RawDstIp),
    case match_ip_level(Opts, SrcIp, DstIp) of
        false -> skip;
        true ->
            parse_tcp_level(TCP_Packet, Opts, #{src_ip => SrcIp, dst_ip => DstIp})
    end.

parse_tcp_level(Packet, Opts, PacketInfo) ->
    <<SrcPort:16, DstPort:16, _:64, OffsetWords:4, _/bitstring>> = Packet,
    case match_tcp_level(Opts, SrcPort, DstPort) of
        false -> skip;
        true ->
            Offset = OffsetWords * 4 * 8,
            <<_:Offset, RawData/binary>> = Packet,
            parse_app_level(RawData, Opts, PacketInfo#{src_port => SrcPort, dst_port => DstPort})
    end.

parse_app_level(<<>>, _, _) -> skip;
parse_app_level(Packet, Opts, PacketInfo) ->
    {ok, Data, _Rest} = parse_data(16, PacketInfo, Packet),
    case match_app_level(Opts, Data) of
        [] -> skip;
        FilterData ->
            PacketInfo#{payload => FilterData}
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

to_addr(<<A,B,C,D>>) -> {A,B,C,D}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Protocol Parsing (Ethernet, IP, TCP)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

parse_data(Len, #{dst_port := DstPort}, Data) ->
    case decode_data(Data) of
        {more, _Rest} ->
            {error, not_enough_data};
        {ok, Msgs, Rest} ->
            Maps = [begin
                 <<Id:Len, Content/binary>> = Msg,
                 {Module, MsgType, MsgData} = pvc_proto:decode_client_req(Content) ,
                 Payload = case DstPort of
                     7878 ->
                         MsgData;
                     _ ->
                        pvc_proto:decode_serv_reply(Content)
                 end,
                 #{id => Id, pb_mod => Module, pb_type => MsgType, pb_payload => Payload}
             end || Msg <- Msgs],
            {ok, Maps, Rest}
    end.

%% Decode client buffer
-spec decode_data(Data :: binary()) -> {ok, [binary()], binary()} | {more, binary()}.
decode_data(Data) ->
    case erlang:decode_packet(4, Data, []) of
        {ok, Message, More} ->
            decode_data_inner(More, [Message]);
        _ ->
            {more, Data}
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
        [FlagArg | Rest] ->
            parse_args(Rest, Fun(FlagArg));
        _ -> {error, {noarg, Flag}}
    end.

required(Opts) ->
    Required = [file],
    Valid = lists:all(fun(F) -> maps:is_key(F, Opts) end, Required),
    case Valid of
        true -> {ok, Opts};
        false -> {error, "Missing required fields"}
    end.
