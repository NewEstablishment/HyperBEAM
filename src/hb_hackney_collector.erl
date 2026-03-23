-module(hb_hackney_collector).

-export(
    [
        deregister_cleanup/1,
        collect_mf/2,
        collect_metrics/2
    ]
).
-behaviour(prometheus_collector).
%%====================================================================
%% Collector API
%%====================================================================
deregister_cleanup(_) -> ok.

collect_mf(_Registry, Callback) ->
    lists:foreach(
        fun({Name, Help, Type}) ->
            Callback(
                prometheus_model_helpers:create_mf(
                    Name,
                    Help,
                    Type,
                    ?MODULE,
                    Name
                )
            )
        end,
        metrics()
    ),
    ok.

collect_metrics(Name, _Data) ->
    case metric_type(Name) of
        histogram ->
            prometheus_model_helpers:histogram_metrics(
                hb_hackney_metrics:collect(Name)
            );
        counter ->
            prometheus_model_helpers:counter_metrics(
                hb_hackney_metrics:collect(Name)
            );
        gauge ->
            prometheus_model_helpers:gauge_metrics(
                hb_hackney_metrics:collect(Name)
            )
    end.

%%====================================================================
%% Private Functions
%%====================================================================

metrics() ->
    [
        {hackney_requests_in_flight,
            "Current number of Hackney requests in flight.", gauge},
        {hackney_host_requests_in_flight,
            "Current number of Hackney requests in flight per host.", gauge},
        {hackney_requests_total,
            "Total number of Hackney requests started.", counter},
        {hackney_requests_finished_total,
            "Total number of Hackney requests finished.", counter},
        {hackney_request_duration_seconds,
            "Hackney request duration in seconds.", histogram},
        {hackney_response_duration_seconds,
            "Hackney time to response headers in seconds.", histogram},
        {hackney_connect_duration_seconds,
            "Hackney connection establishment time in seconds.", histogram},
        {hackney_connect_timeouts_total,
            "Total number of Hackney connect timeouts.", counter},
        {hackney_connect_errors_total,
            "Total number of Hackney connect errors.", counter},
        {hackney_pool_take_total,
            "Total number of Hackney pool take operations.", counter},
        {hackney_pool_no_socket_total,
            "Total number of Hackney pool misses due to no socket.", counter},
        {hackney_pool_in_use,
            "Hackney connections currently in use.", gauge},
        {hackney_pool_free,
            "Idle Hackney connections available in the pool.", gauge},
        {hackney_pool_queue,
            "Hackney requests waiting for a pool connection.", gauge},
        {hackney_pool_connection_reuse_total,
            "Total number of reused pooled Hackney connections.", counter},
        {hackney_pool_new_connection_total,
            "Total number of new Hackney connections created by pool usage.",
            counter}
    ].

metric_type(Name) ->
    proplists:get_value(Name, [{MetricName, Type} || {MetricName, _, Type} <- metrics()]).
