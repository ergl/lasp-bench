-module(lasp_bench_driver_rubis).

-export([new/1,
         run/4]).

-include("lasp_bench.hrl").

-define(RUBIS_CORE, lasp_bench_driver_rubis_core).

-record(state, {
    server_ip,
    server_port,
    http_connection,
    rubis_state
}).

new(_Id) ->
    _ = application:ensure_all_started(hackney),

    Ip = lasp_bench_config:get(rubis_ip, "127.0.0.1"),
    Port = lasp_bench_config:get(rubis_port, 5000),
    {ok, ConnRef} = hackney:connect(hackney_tcp, Ip, Port, []),

    {ok, #state{
        server_ip = Ip,
        server_port = integer_to_list(Port),
        http_connection = ConnRef,
        rubis_state = ?RUBIS_CORE:new_rubis_state()
    }}.

run(registeruser, _, _, State) ->
    Region = ?RUBIS_CORE:random_region(),
    {{Username, Password}, NewRubisState} = ?RUBIS_CORE:gen_new_user(State#state.rubis_state),
    Payload = jsx:encode([Username, Password, {region, Region}]),
    build_request(
        post,
        {"/users", Payload},
        fun create_user_handler/2,
        State#state{rubis_state = NewRubisState}
    );

run(browsecategories, _, _, State) ->
    build_request(get, "/categories", State);

run(browseregions, _, _, State) ->
    build_request(get, "/regions", State);

run(searchitemsincategory, _, _, State) ->
    RegionId = ?RUBIS_CORE:random_region_id(),
    URL = "/searchByCategory/" ++ integer_to_list(RegionId),
    build_request(get, URL, State);

run(searchitemsinregion, _, _, State) ->
    RegionId = ?RUBIS_CORE:random_region_id(),
    CategoryId = ?RUBIS_CORE:random_category_id(),
    Payload = jsx:encode([{regionId, RegionId}, {categoryId, CategoryId}]),
    build_request(post, {"/searchByRegion", Payload}, State);

run(viewitem, _, _, State) ->
    ItemId = ?RUBIS_CORE:random_item_id(),
    URL = "/items/" ++ integer_to_list(ItemId),
    build_request(get, URL, State);

run(viewuserinfo, _, _, State) ->
    UserId = ?RUBIS_CORE:random_user_id(State#state.rubis_state),
    URL = "/users/" ++ integer_to_list(UserId),
    build_request(get, URL, State);

run(viewbidhistory, _, _, State) ->
    ItemId = ?RUBIS_CORE:random_item_id(),
    URL = "/items/" ++ integer_to_list(ItemId) ++ "/bids",
    build_request(get, URL, State);

run(_, _, _, State) ->
    {ok, State}.

%% Networking Utils

build_request(Verb, Path, State) ->
    build_request(Verb, Path, fun simple_resp_handler/2, State).

build_request(get, Path, Handler, State=#state{
    server_ip=Ip,
    server_port=Port,
    http_connection=Conn
}) ->
    URL = generate_url(Ip, Port, Path),
    Headers = [{<<"Connection">>, <<"keep-alive">>}],
    Payload = <<>>,
    Req = {get, URL, Headers, Payload},
    perf_req(Conn, Req, State, Handler);

build_request(post, {Path, Payload}, Handler, State=#state{
    server_ip=Ip,
    server_port=Port,
    http_connection=Conn
}) ->
    URL = generate_url(Ip, Port, Path),
    Headers = [
        {<<"Connection">>, <<"keep-alive">>},
        {<<"Content-Type">>, <<"application/json">>}
    ],
    Req = {post, URL, Headers, Payload},
    perf_req(Conn, Req, State, Handler).

generate_url(Address,Port,Path) ->
    list_to_binary("http://" ++ Address ++ ":" ++ Port ++ Path).

perf_req(_Conn, Req, State, Handler) ->
    {Method, URL, Headers, Payload} = Req,
    case hackney:request(Method, URL, Headers, Payload, []) of
        {ok, 200, _Headers, HttpConn} ->
            {ok, Body} = hackney:body(HttpConn),
            Handler(Body, State);
        {ok, Status, _Headers, HttpConn} ->
            {ok, Body} = hackney:body(HttpConn),
            {error, {request_failed, Status, binary_to_list(Body)}, State};
        {error, Reason} ->
            {error, {send_request_failed, Reason, Req}, State}
    end.

simple_resp_handler(_Body, State) ->
    {ok, State}.

create_user_handler(Body, State = #state{rubis_state=RubisState}) ->
    #{ result :=
        #{generatedKeyValue :=
            #{ pkValue := NewId } } } = jsx:decode(Body, [{labels, atom}, return_maps]),
    NewRubisState = ?RUBIS_CORE:update_state({user_id, NewId}, RubisState),
    {ok, State#state{rubis_state=NewRubisState}}.
