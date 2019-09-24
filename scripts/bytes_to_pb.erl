#!/usr/bin/env escript
%%! -pa ../_build/default/lib/pvc_proto/ebin -Wall

-mode(compile).

%% API
-export([main/1]).

main(Args) ->
    case parse_args(Args) of
        {error, Reason} ->
            io:fwrite("Wrong option: reason ~p~n", [Reason]),
            usage();
        {ok, #{mode := Mode, id_len := Len, file := FileName}} ->
            {ok, Stream} = file:read_file(FileName),
            parse_data(Mode, Len, Stream);
        {ok, #{mode := Mode, id_len := Len, stdin := StringStream}} ->
            Stream = hexstr_to_bin(StringStream),
            parse_data(Mode, Len, Stream)
    end.

parse_data(server, Len, Data) ->
    %% client -> server, should have entire buffer?
    case decode_data(Data) of
        {more, _} ->
            io:fwrite(standard_error, "Not enough data~n", []),
            halt(1);
        {ok, Msgs, Rest} ->
            [begin
                 case Msg of
                     <<>> ->
                         io:format("Got weird data ~w~n", [Data]);
                     _ -> ok
                 end,
                 <<Id:Len, Content/binary>> = Msg,
                 Decoded = pvc_proto:decode_client_req(Content) ,
                 io:format("msg id := ~p, data := ~p~n", [Id, Decoded])
             end || Msg <- Msgs],
            print_rest(Rest)
    end;

parse_data(client, Len, Data) ->
    %% server -> client, single packet
    {ok, Packet, Rest} = erlang:decode_packet(4, Data, []),
    <<Id:Len, Content/binary>> = Packet,
    Decoded = pvc_proto:decode_serv_reply(Content),
    io:format("msg id := ~p, data := ~p~n", [Id, Decoded]),
    print_rest(Rest).

print_rest(<<>>) ->
    ok;
print_rest(Rest) ->
    io:format("With rest ~s~n", [bin_to_hexstr(Rest)]).

usage() ->
    Name = filename:basename(escript:script_name()),
    ok = io:fwrite(standard_error, "Usage: ~s [-cs] [-l <len>]:=16 [-f bytes_file] | [-r bytes]~n", [Name]).

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

%% Hacked-together getopt

-spec parse_args([term()]) -> {ok, proplists:proplist()} | {error, Reason :: atom()}.
parse_args([]) ->
    {error, empty};

parse_args(Args) ->
    case parse_args(Args, #{}) of
        {ok, Opts} ->
            validate_required(Opts);
        Err ->
            Err
    end.

parse_args([], Acc) ->
    {ok, Acc};

parse_args([ [$- | Flag] | Args], Acc) ->
    case Flag of
        [$c] ->
            parse_args(Args, Acc#{mode => client});
        [$s] ->
            parse_args(Args, Acc#{mode => server});
        [$l] ->
            case Args of
                [FlagArg | Rest] ->
                    parse_args(Rest, Acc#{id_len => list_to_integer(FlagArg)});
                _ ->
                    {error, {noarg, Flag}}
            end;
        [$f] ->
            case Args of
                [FlagArg | Rest] ->
                    parse_args(Rest, Acc#{file => FlagArg});
                _ ->
                    {error, {noarg, Flag}}
            end;
        [$r] ->
            case Args of
                [FlagArg | Rest] ->
                    parse_args(Rest, Acc#{stdin => FlagArg});
                _ ->
                    {error, {noarg, Flag}}
            end;
        _ ->
            {error, {option, Flag}}
    end;

parse_args(_, _) ->
    {error, noarg}.

validate_required(Opts) ->
    HasMode = maps:is_key(mode, Opts),
    HasInput = maps:is_key(file, Opts) orelse maps:is_key(stdin, Opts),
    case (HasMode andalso HasInput) of
        true ->
            case maps:is_key(id_len, Opts) of
                true ->
                    {ok, Opts};
                false ->
                    {ok, Opts#{id_len => 16}}
            end;
        _ ->
            {error, "Have to specify mode _and_ input type"}
    end.

%% Hextream parsing

%% Taken from https://github.com/b/hex, and that itself
%% from http://necrobious.blogspot.com/2008/03/binary-to-hex-string-back-to-binary-in.html
hexstr_to_bin(S) ->
    hexstr_to_bin(S, []).
hexstr_to_bin([], Acc) ->
    list_to_binary(lists:reverse(Acc));
hexstr_to_bin([X,Y|T], Acc) ->
    {ok, [V], []} = io_lib:fread("~16u", [X,Y]),
    hexstr_to_bin(T, [V | Acc]);
hexstr_to_bin([X|T], Acc) ->
    {ok, [V], []} = io_lib:fread("~16u", lists:flatten([X,"0"])),
    hexstr_to_bin(T, [V | Acc]).

bin_to_hexstr(Bin) ->
    lists:flatten([io_lib:format("~2.16.0B", [X]) ||
        X <- binary_to_list(Bin)]).
