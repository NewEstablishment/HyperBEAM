-module(hb_metrics_server).
-export([start/1, init/2]).

start(NodeMsg) ->
    case hb_opts:get(prometheus, not hb_features:test(), NodeMsg) of
        true ->
            Port = hb_opts:get(metrics_port, 8735, NodeMsg),
            cowboy:start_clear(
                hb_metrics_listener,
                #{socket_opts => [{port, Port}], max_connections => 64, num_acceptors => 2},
                #{env => #{dispatch => cowboy_router:compile([{'_', [{"/metrics", ?MODULE, []}]}])}}
            );
        false ->
            ignore
    end.

init(Req, State) ->
    {_, HeaderList, Body} =
        prometheus_http_impl:reply(
            #{path => true,
            headers => fun(_Name, Default) -> Default end,
            registry => prometheus_registry:exists(<<"default">>),
            standalone => false}
        ),
    Headers = maps:from_list(prometheus_cowboy:to_cowboy_headers(HeaderList)),
    Req2 = cowboy_req:reply(200, Headers, Body, Req),
    {ok, Req2, State}.
