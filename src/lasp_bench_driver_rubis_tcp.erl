-module(lasp_bench_driver_rubis_tcp).

-export([new/1,
         run/4]).

-include("lasp_bench.hrl").

-define(core, lasp_bench_driver_rubis_core).

-record(state, {
    %% Antidote connection to the remote node
    worker_id,
    socket,
    rubis_state
}).

new(Id) ->
    _ = application:ensure_all_started(rubis_proto),

    Ip = lasp_bench_config:get(rubis_ip, '127.0.0.1'),
    Port = lasp_bench_config:get(rubis_port, 7878),
    Options = [binary, {active, false}, {packet, 2}],
    {ok, Sock} = gen_tcp:connect(Ip, Port, Options),

    RubisState = lasp_bench_driver_rubis_core:new_rubis_state(),
    {ok, #state{socket=Sock,
                worker_id=Id,
                rubis_state=RubisState}}.

run(perform_operation, _, _, State = #state{rubis_state=RState}) ->
    {Operation, NewRState} = lasp_bench_driver_rubis_core:next_operation(RState),
    Resp = run_op(Operation, State#state{rubis_state=NewRState}),
    sleep_then_return(Operation, Resp).

run_op(registeruser, State = #state{socket=Sock, rubis_state=RState}) ->
    RegionId = ?core:random_region_id(RState),
    {{Username, Password}, NewState} = ?core:gen_new_user(RState),
    Msg = rubis_proto:register_user(Username, Password, RegionId),
    ok = gen_tcp:send(Sock, Msg),
    {ok, BinReply} = gen_tcp:recv(Sock, 0),
    Resp = rubis_proto:decode_reply(BinReply),
    case Resp of
        {ok, Id} ->
            FinalState = ?core:update_state({confirm_user, Id, Username}, NewState),
            {ok, State#state{rubis_state=FinalState}};

         {error, Reason} ->
             FinalState = ?core:update_state({discard_user, Username}, NewState),
             {error, Reason, State#state{rubis_state=FinalState}}
    end;

run_op(browsecategories, State = #state{socket=Sock}) ->
    Msg = rubis_proto:browse_categories(),
    ok = gen_tcp:send(Sock, Msg),
    {ok, BinReply} = gen_tcp:recv(Sock, 0),
    Resp = rubis_proto:decode_reply(BinReply),
    case Resp of
        ok ->
            {ok, State};

        {error, Reason} ->
            {error, Reason, State}
    end;

run_op(searchitemsincategory, State = #state{socket=Sock, rubis_state=RState}) ->
    CategoryId = ?core:random_category_id(RState),
    Msg = rubis_proto:search_items_by_category(CategoryId),
    ok = gen_tcp:send(Sock, Msg),
    {ok, BinReply} = gen_tcp:recv(Sock, 0),
    Resp = rubis_proto:decode_reply(BinReply),
    case Resp of
        ok ->
            {ok, State};

        {error, Reason} ->
            {error, Reason, State}
    end;

run_op(browseregions, State = #state{socket=Sock}) ->
    Msg = rubis_proto:browse_regions(),
    ok = gen_tcp:send(Sock, Msg),
    {ok, BinReply} = gen_tcp:recv(Sock, 0),
    Resp = rubis_proto:decode_reply(BinReply),
    case Resp of
        ok ->
            {ok, State};

        {error, Reason} ->
            {error, Reason, State}
    end;

run_op(searchitemsinregion, State = #state{socket=Sock, rubis_state=RState}) ->
    RegionId = ?core:random_region_id(RState),
    CategoryId = ?core:random_category_id(RState),
    Msg = rubis_proto:search_items_by_region(CategoryId, RegionId),
    ok = gen_tcp:send(Sock, Msg),
    {ok, BinReply} = gen_tcp:recv(Sock, 0),
    Resp = rubis_proto:decode_reply(BinReply),
    case Resp of
        ok ->
            {ok, State};

        {error, Reason} ->
            {error, Reason, State}
    end;

run_op(viewitem, State = #state{socket=Sock, rubis_state=RState}) ->
    ItemId = lasp_bench_driver_rubis_core:random_item_id(RState),
    Msg = rubis_proto:view_item(ItemId),
    ok = gen_tcp:send(Sock, Msg),
    {ok, BinReply} = gen_tcp:recv(Sock, 0),
    Resp = rubis_proto:decode_reply(BinReply),
    case Resp of
        ok ->
            {ok, State};

        {error, Reason} ->
            {error, Reason, State}
    end;

run_op(viewuserinfo, State = #state{socket=Sock, rubis_state=RState}) ->
    UserId = lasp_bench_driver_rubis_core:random_user_id(RState),
    Msg = rubis_proto:view_user(UserId),
    ok = gen_tcp:send(Sock, Msg),
    {ok, BinReply} = gen_tcp:recv(Sock, 0),
    Resp = rubis_proto:decode_reply(BinReply),
    case Resp of
        ok ->
            {ok, State};

        {error, Reason} ->
            {error, Reason, State}
    end;

run_op(viewbidhistory, State = #state{socket=Sock, rubis_state=RState}) ->
    ItemId = lasp_bench_driver_rubis_core:random_item_id(RState),
    Msg = rubis_proto:view_bid_history(ItemId),
    ok = gen_tcp:send(Sock, Msg),
    {ok, BinReply} = gen_tcp:recv(Sock, 0),
    Resp = rubis_proto:decode_reply(BinReply),
    case Resp of
        ok ->
            {ok, State};

        {error, Reason} ->
            {error, Reason, State}
    end;

run_op(buynowauth, State) ->
    perform_auth(State);

run_op(storebuynow, State = #state{socket=Sock,rubis_state=RState}) ->
    {ItemId, UserId, Q} = ?core:gen_buy_now(RState),
    Msg = rubis_proto:store_buy_now(ItemId, UserId, Q),
    ok = gen_tcp:send(Sock, Msg),
    {ok, BinReply} = gen_tcp:recv(Sock, 0),
    Resp = rubis_proto:decode_reply(BinReply),
    case Resp of
        {ok, _} ->
            {ok, State};

        {error, Reason} ->
            {error, Reason, State}
    end;

run_op(putbidauth, State) ->
    perform_auth(State);

run_op(storebid, State = #state{socket=Sock,rubis_state=RState}) ->
    {ItemId, UserId, Price} = ?core:gen_bid(RState),
    Msg = rubis_proto:store_bid(ItemId, UserId, Price),
    ok = gen_tcp:send(Sock, Msg),
    {ok, BinReply} = gen_tcp:recv(Sock, 0),
    Resp = rubis_proto:decode_reply(BinReply),
    case Resp of
        {ok, _} ->
            {ok, State};

        {error, Reason} ->
            {error, Reason, State}
    end;

run_op(putcommentauth, State) ->
    perform_auth(State);

run_op(storecomment, State = #state{socket=Sock,rubis_state=RState}) ->
    {ItemId, FromId, ToId, Rating, Body} = ?core:gen_comment(RState),
    Msg = rubis_proto:store_comment(ItemId, FromId, ToId, Rating, Body),
    ok = gen_tcp:send(Sock, Msg),
    {ok, BinReply} = gen_tcp:recv(Sock, 0),
    Resp = rubis_proto:decode_reply(BinReply),
    case Resp of
        {ok, _} ->
            {ok, State};

        {error, Reason} ->
            {error, Reason, State}
    end;

run_op(registeritem, State = #state{socket=Sock, rubis_state=RState}) ->
    {ItemName, Description, Quantity, CategoryId, SellerId} = ?core:gen_item(RState),
    Msg = rubis_proto:store_item(ItemName, Description, Quantity, CategoryId, SellerId),
    ok = gen_tcp:send(Sock, Msg),
    {ok, BinReply} = gen_tcp:recv(Sock, 0),
    Resp = rubis_proto:decode_reply(BinReply),
    case Resp of
        {ok, ItemId} ->
            NewState = ?core:update_state({confirm_item, ItemId}, RState),
            {ok, State#state{rubis_state=NewState}};

        {error, Reason} ->
            {error, Reason, State}
    end;

run_op(aboutme_auth, State) ->
    perform_auth(State);

run_op(aboutme, State = #state{socket=Sock, rubis_state=RState}) ->
    UserId = lasp_bench_driver_rubis_core:random_user_id(RState),
    Msg = rubis_proto:about_me(UserId),
    ok = gen_tcp:send(Sock, Msg),
    {ok, BinReply} = gen_tcp:recv(Sock, 0),
    Resp = rubis_proto:decode_reply(BinReply),
    case Resp of
        ok ->
            {ok, State};

        {error, Reason} ->
            {error, Reason, State}
    end.


%% Util functions

-spec sleep_then_return(atom(), Resp) -> Resp.
sleep_then_return(Operation, Resp) ->
    Rstate = get_state(Resp),
    ok = ?core:wait_time(Operation, Rstate),
    Resp.

get_state({ok, #state{rubis_state=RState}}) ->
    RState;

get_state({error, _, #state{rubis_state=RState}}) ->
    RState.

perform_auth(State = #state{socket=Sock, rubis_state=RState}) ->
    {Username, Pass} = ?core:random_user(RState),
    Msg = rubis_proto:auth_user(Username, Pass),
    ok = gen_tcp:send(Sock, Msg),
    {ok, BinReply} = gen_tcp:recv(Sock, 0),
    Resp = rubis_proto:decode_reply(BinReply),
    case Resp of
        {ok, Id} ->
            NewState = ?core:set_logged_in(Id, RState),
            {ok, State#state{rubis_state=NewState}};

        {error, Reason} ->
            {error, Reason, State}
    end.
