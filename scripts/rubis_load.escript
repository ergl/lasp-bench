#!/usr/bin/env escript
%%! -pa ../_build/default/lib/jsx/ebin ../_build/default/lib/pvc_proto/ebin -Wall

main([Host, Port, ConfigFile, Outfile]) ->
    Resp = load_config(ConfigFile),
    case Resp of
        {error, Reason} ->
            io:fwrite(standard_error, "config file: ~p~n", [Reason]),
            halt(1);
        {ok, Config} ->
            {ok, Bin} = do_load(Host, Port, Config),
            ok = file:write_file(Outfile, Bin)
    end;

main(_) ->
    io:fwrite("rubis_load.escript <host> <port> <config-file> <output-file>~n"),
    halt(1).

load_config(ConfigFile) ->
    case file:read_file(ConfigFile) of
        {error, Reason} ->
            {error, Reason};

        {ok, Contents} ->
            case jsx:is_json(Contents) of
                false ->
                    {error, no_json};
                true ->
                    Json = jsx:decode(Contents, [{labels, atom}, return_maps]),
                    {ok, Json}
            end
    end.

do_load(BinHost, BinPort, Config) ->
    Host = list_to_atom(BinHost),
    Port = (catch list_to_integer(BinPort)),
    case is_integer(Port) of
        false ->
            {error, bad_port};
        true ->
            {ok, Sock} = gen_tcp:connect(Host, Port, [binary, {active,false},{packet,2}]),
            Bin = do_load(Sock, Config),
            gen_tcp:close(Sock),
            {ok, Bin}
    end.

do_load(Socket, Config) ->
    #{regions := Regions} = Config,
    RegionIds = load_regions(Socket, Regions),
    #{categories := CategoryMapping} = Config,
    Categories = lists:map(fun(#{name := Name}) -> Name end, CategoryMapping),
    CategoryIds = load_categories(Socket, Categories),
    #{users := UserNum} = Config,
    {UserNameSet, UserIds} = load_users(Socket, UserNum, RegionIds),
    #{items_description_length := ItemLen} = Config,
    ItemIds = load_items(Socket, ItemLen, UserIds, CategoryIds, CategoryMapping),
    #{bids := BidNum} = Config,
    BidIds = load_bids(Socket, BidNum, UserIds, ItemIds),
    #{comments := CommentNum, comments_max_length := CommentLen} = Config,
    CommentIds = load_comments(Socket, CommentNum, CommentLen, ItemIds, UserIds),
    jsx:encode(#{
        <<"region_ids">> => RegionIds,
        <<"category_ids">> => CategoryIds,
        <<"user_ids">> => UserIds,
        <<"user_names">> => lists:map(fun erlang:list_to_binary/1, sets:to_list(UserNameSet)),
        <<"item_ids">> => ItemIds,
        <<"bid_ids">> => BidIds,
        <<"comment_ids">> => CommentIds
    }).

load_regions(Socket, Regions) ->
    lists:foldl(fun(Region, Acc) ->
        Msg = ppb_rubis_driver:put_region(Region),
        gen_tcp:send(Socket, Msg),
        {ok, BinReply} = gen_tcp:recv(Socket, 0),
        {ok, Id} = pvc_proto:decode_serv_reply(BinReply),
        [Id | Acc]
    end, [], Regions).

load_categories(Socket, Categories) ->
    lists:foldl(fun(Category, Acc) ->
        Msg = ppb_rubis_driver:put_category(Category),
        gen_tcp:send(Socket, Msg),
        {ok, BinReply} = gen_tcp:recv(Socket, 0),
        {ok, Id} = pvc_proto:decode_serv_reply(BinReply),
        [Id | Acc]
    end, [], Categories).

load_users(Socket, UserNum, RegionIds) ->
    UserSet = sets:new(),
    RegionLen = length(RegionIds),

    lists:foldl(fun(_, {Set, Ids}) ->
        {Username, NewSet} = unique_string(10, Set),
        Password = Username,
        N = rand:uniform(RegionLen),
        RandomId = lists:nth(N, RegionIds),
        Msg = ppb_rubis_driver:register_user(Username, Password, RandomId),
        gen_tcp:send(Socket, Msg),
        {ok, BinReply} = gen_tcp:recv(Socket, 0),
        {ok, Id} = pvc_proto:decode_serv_reply(BinReply),
        {NewSet, [Id | Ids]}
    end, {UserSet, []}, lists:seq(0, UserNum)).

load_items(Socket, ItemDescLen, UserIds, CategoryIds, CategoryMapping) ->
    CategoryMap = element(2, lists:foldl(fun(#{items := ItemNum}, Acc) ->
        {NextId, MapAcc} = Acc,
        CategoryId = lists:nth(NextId, CategoryIds),
        NewMapAcc = [{CategoryId, ItemNum} | MapAcc],
        {NextId - 1, NewMapAcc}
    end, {length(CategoryIds), []}, CategoryMapping)),

    ItemSet = sets:new(),
    element(2, lists:foldl(fun({CategoryId, ItemNum}, {Set, Ids}) ->
        {NewSet, NestedIds} = load_items_from_category(Socket,
                                                       ItemNum,
                                                       Set,
                                                       ItemDescLen,
                                                       UserIds,
                                                       CategoryId),
        {NewSet, NestedIds ++ Ids}
    end, {ItemSet, []}, CategoryMap)).

load_items_from_category(Socket, Num, NameSet, ItemDescLen, UserIds, CategoryId) ->
    UsersLen = length(UserIds),

    lists:foldl(fun(_, {Set, Ids}) ->
        {ItemName, NewSet} = unique_string(10, Set),
        Description = random_string(ItemDescLen),
        Quantity = rand:uniform(10000),
        N = rand:uniform(UsersLen),
        SellerId = lists:nth(N, UserIds),
        Msg = ppb_rubis_driver:store_item(ItemName, Description, Quantity, CategoryId, SellerId),
        gen_tcp:send(Socket, Msg),
        {ok, BinReply} = gen_tcp:recv(Socket, 0),
        {ok, Id} = pvc_proto:decode_serv_reply(BinReply),
        {NewSet, [Id | Ids]}
    end, {NameSet, []}, lists:seq(0, Num)).

load_bids(Socket, Num, UserIds, ItemIds) ->
    UsersLen = length(UserIds),
    ItemsLen = length(ItemIds),
    lists:map(fun(_) ->
        RandomUser = lists:nth(rand:uniform(UsersLen), UserIds),
        RandomItem = lists:nth(rand:uniform(ItemsLen), ItemIds),
        Price = rand:uniform(2000),
        Msg = ppb_rubis_driver:store_bid(RandomItem, RandomUser, Price),
        gen_tcp:send(Socket, Msg),
        {ok, BinReply} = gen_tcp:recv(Socket, 0),
        {ok, Id} = pvc_proto:decode_serv_reply(BinReply),
        Id
    end, lists:seq(0, Num)).

load_comments(Socket, Num, CommentLen, ItemIds, UserIds) ->
    UsersLen = length(UserIds),
    ItemsLen = length(ItemIds),
    Ratings = [-5, -3, 0, 3, 5],
    lists:map(fun(_) ->
        ToN = rand:uniform(UsersLen),
        FromN = random_choice_not(ToN, UsersLen),
        ToId = lists:nth(ToN, UserIds),
        FromId = lists:nth(FromN, UserIds),
        OnId = lists:nth(rand:uniform(ItemsLen), ItemIds),
        Rating = lists:nth(rand:uniform(length(Ratings)), Ratings),
        Body = random_string(CommentLen),
        Msg = ppb_rubis_driver:store_comment(OnId, FromId, ToId, Rating, Body),
        gen_tcp:send(Socket, Msg),
        {ok, BinReply} = gen_tcp:recv(Socket, 0),
        {ok, Id} = pvc_proto:decode_serv_reply(BinReply),
        Id
    end, lists:seq(0, Num)).

random_choice_not(N, Ceil) ->
    case rand:uniform(Ceil) of
        N ->
            random_choice_not(N, Ceil);
        M ->
            M
    end.

unique_string(N, Set) ->
    S = random_string(N),
    case sets:is_element(S, Set) of
        true ->
            unique_string(N, Set);
        false ->
            NSet = sets:add_element(S, Set),
            {S, NSet}
    end.

random_string(N) ->
    Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890",
    lists:foldl(fun(_, Acc) ->
        [lists:nth(rand:uniform(length(Chars)), Chars)] ++ Acc
    end, [], lists:seq(1, N)).



