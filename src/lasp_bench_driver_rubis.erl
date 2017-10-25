-module(lasp_bench_driver_rubis).

-export([new/1,
         run/4]).

-include("lasp_bench.hrl").

-define(TTABLE, lasp_bench_driver_rubis_transition_table).

-record(state, {
    server_ip,
    server_port,
    http_connection
}).

new(_Id) ->
    _ = application:ensure_all_started(hackney),

    Ip = lasp_bench_config:get(rubis_ip, "127.0.0.1"),
    Port = lasp_bench_config:get(rubis_port, 5000),
    {ok, ConnRef} = hackney:connect(hackney_tcp, Ip, Port, []),
    {ok, #state{
        server_ip = Ip,
        server_port = integer_to_list(Port),
        http_connection = ConnRef
    }}.

run(home, _, _, State) ->
    build_request(get, "/home", State);

run(register, _, _, State) ->
    build_request(get, "/register", State);

run(registeruser, _, _, State) ->
    %% TODO
    build_request(post, {"/register", jsx:encode([])}, State);

run(browse, _, _, State) ->
    build_request(get, "/browse", State);

run(browsecategories, _, _, State) ->
    %% TODO?
    build_request(get, "/browseCategories", State);

run(searchitemsincategory, KeyGen, _, State) ->
    %% TODO?
    Path = io_lib:format("/searchItemsInCategory/~p", [KeyGen()]),
    build_request(get, Path, State);

run(browseregions, _, _, State) ->
    %% TODO?
    build_request(get, "/browseRegions", State);

run(browsecategoriesinregion, KeyGen, _, State) ->
    %% TODO?
    Path = io_lib:format("/browseCategoriesInRegion/~p", [KeyGen()]),
    build_request(get, Path, State);

run(searchitemsinregion, KeyGen, _, State) ->
    %% TODO?
    Path = io_lib:format("/searchItemsInRegion/~p", [KeyGen()]),
    build_request(get, Path, State);

run(viewitem, KeyGen, _, State) ->
    %% TODO?
    Path = io_lib:format("/item/~p", [KeyGen()]),
    build_request(get, Path, State);

run(viewuserinfo, KeyGen, _, State) ->
    %% TODO?
    Path = io_lib:format("/item/~p", [KeyGen()]),
    build_request(get, Path, State);

run(viewbidhistory, KeyGen, _, State) ->
    %% TODO?
    Path = io_lib:format("/bidHistory/~p", [KeyGen()]),
    build_request(get, Path, State);

run(_, _, _, State) ->
    timer:sleep(1000),
    {ok, State}.

%% Networking Utils

build_request(get, Path, State=#state{
    server_ip=Ip,
    server_port=Port,
    http_connection=Conn
}) ->
    URL = generate_url(Ip, Port, Path),
    Headers = [{<<"Connection">>, <<"keep-alive">>}],
    Payload = <<>>,
    Req = {get, URL, Headers, Payload},
    perf_req(Conn, Req, State);

build_request(post, {Path, Payload}, State=#state{
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
    perf_req(Conn, Req, State).

generate_url(Address,Port,Path) ->
    list_to_binary("http://" ++ Address ++ ":" ++ Port ++ Path).

perf_req(Connection, Req, State) ->
    perf_req(Connection, Req, State, fun simple_resp_handler/2).

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
