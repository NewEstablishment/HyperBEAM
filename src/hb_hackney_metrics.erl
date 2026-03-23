%%% @doc Hackney metrics backend that stores state for Prometheus collection.
-module(hb_hackney_metrics).

-export([
    new/2,
    delete/1,
    increment_counter/1,
    increment_counter/2,
    decrement_counter/1,
    decrement_counter/2,
    update_histogram/2,
    update_gauge/2,
    update_meter/2,
    collect/1
]).

-include_lib("eunit/include/eunit.hrl").

-define(TABLE, ?MODULE).
-define(DURATION_BUCKETS_S, [0.01, 0.1, 0.5, 1, 5, 10, 30, 60]).
-define(DURATION_BUCKETS_US,
    [10_000, 100_000, 500_000, 1_000_000, 5_000_000, 10_000_000, 30_000_000,
        60_000_000]).

%% @doc Register a metric name with the backend.
new(_Type, Name) ->
    ensure_table(),
    case classify_name(Name) of
        {counter, Metric, Labels} ->
            init_counter(Metric, Labels);
        {gauge, Metric, Labels} ->
            init_gauge(Metric, Labels);
        {histogram, Metric, Labels} ->
            init_histogram(Metric, Labels);
        ignore ->
            ok
    end.

%% @doc Delete a metric from the backend.
delete(Name) ->
    ensure_table(),
    case classify_name(Name) of
        {counter, Metric, Labels} ->
            ets:delete(?TABLE, {counter, Metric, Labels}),
            ok;
        {gauge, Metric, Labels} ->
            ets:delete(?TABLE, {gauge, Metric, Labels}),
            ok;
        {histogram, Metric, Labels} ->
            ets:delete(?TABLE, {hist_count, Metric, Labels}),
            ets:delete(?TABLE, {hist_sum_us, Metric, Labels}),
            lists:foreach(
                fun(BoundUs) ->
                    ets:delete(?TABLE, {hist_bucket, Metric, Labels, BoundUs})
                end,
                ?DURATION_BUCKETS_US
            ),
            ok;
        ignore ->
            ok
    end.

%% @doc Increment a counter-like metric by 1.
increment_counter(Name) ->
    increment_counter(Name, 1).

%% @doc Increment a counter-like metric by Value.
increment_counter(Name, Value) ->
    ensure_table(),
    adjust(Name, Value).

%% @doc Decrement a counter-like metric by 1.
decrement_counter(Name) ->
    decrement_counter(Name, 1).

%% @doc Decrement a counter-like metric by Value.
decrement_counter(Name, Value) ->
    ensure_table(),
    adjust(Name, -Value).

%% @doc Update a histogram-like metric.
update_histogram(Name, Fun) when is_function(Fun, 0) ->
    Begin = os:timestamp(),
    Result = Fun(),
    DurationMs = timer:now_diff(os:timestamp(), Begin) / 1000,
    case update_histogram(Name, DurationMs) of
        ok -> Result;
        Error -> throw(Error)
    end;
update_histogram(Name, Value) ->
    ensure_table(),
    case classify_name(Name) of
        {histogram, Metric, Labels} ->
            observe_duration(Metric, Labels, Value);
        {gauge, Metric, Labels} ->
            set_gauge(Metric, Labels, Value);
        ignore ->
            ok
    end.

%% @doc Set a gauge-like metric directly.
update_gauge(Name, Value) ->
    ensure_table(),
    case classify_name(Name) of
        {gauge, Metric, Labels} ->
            set_gauge(Metric, Labels, Value);
        _ ->
            ok
    end.

%% @doc Update a meter-like metric.
update_meter(Name, Value) ->
    ensure_table(),
    case classify_name(Name) of
        {counter, Metric, Labels} ->
            add_counter(Metric, Labels, Value);
        _ ->
            ok
    end.

%% @doc Return collector-ready samples for the given Prometheus metric family.
collect(Name) ->
    ensure_table(),
    case metric_kind(Name) of
        counter ->
            collect_scalar(counter, Name);
        gauge ->
            collect_scalar(gauge, Name);
        histogram ->
            collect_histogram(Name);
        ignore ->
            []
    end.

%% @doc Ensure the ETS table exists before use.
ensure_table() ->
    case ets:info(?TABLE) of
        undefined ->
            try
                ets:new(
                    ?TABLE,
                    [named_table, public, set, {read_concurrency, true},
                        {write_concurrency, true}]
                ),
                ok
            catch
                error:badarg ->
                    ok
            end;
        _ ->
            ok
    end.

%% @doc Increment or decrement a tracked metric.
adjust(Name, Delta) ->
    case classify_name(Name) of
        {counter, Metric, Labels} ->
            add_counter(Metric, Labels, Delta);
        {gauge, Metric, Labels} ->
            add_gauge(Metric, Labels, Delta);
        _ ->
            ok
    end.

%% @doc Classify Hackney metric names into Prometheus metric families.
classify_name([hackney, nb_requests]) ->
    {gauge, hackney_requests_in_flight, []};
classify_name([hackney, Host, nb_requests]) ->
    {gauge, hackney_host_requests_in_flight, host_labels(Host)};
classify_name([hackney, total_requests]) ->
    {counter, hackney_requests_total, []};
classify_name([hackney, finished_requests]) ->
    {counter, hackney_requests_finished_total, []};
classify_name([hackney, Host, request_time]) ->
    {histogram, hackney_request_duration_seconds, host_labels(Host)};
classify_name([hackney, Host, response_time]) ->
    {histogram, hackney_response_duration_seconds, host_labels(Host)};
classify_name([hackney, Host, connect_time]) ->
    {histogram, hackney_connect_duration_seconds, host_labels(Host)};
classify_name([hackney, Host, connect_timeout]) ->
    {counter, hackney_connect_timeouts_total, host_labels(Host)};
classify_name([hackney, Host, connect_error]) ->
    {counter, hackney_connect_errors_total, host_labels(Host)};
classify_name([hackney_pool, Pool, take_rate]) ->
    {counter, hackney_pool_take_total, pool_labels(Pool)};
classify_name([hackney_pool, Pool, no_socket]) ->
    {counter, hackney_pool_no_socket_total, pool_labels(Pool)};
classify_name([hackney_pool, Pool, in_use_count]) ->
    {gauge, hackney_pool_in_use, pool_labels(Pool)};
classify_name([hackney_pool, Pool, free_count]) ->
    {gauge, hackney_pool_free, pool_labels(Pool)};
classify_name([hackney_pool, Pool, queue_count]) ->
    {gauge, hackney_pool_queue, pool_labels(Pool)};
classify_name([hackney_pool, Host, reuse_connection]) ->
    {counter, hackney_pool_connection_reuse_total, host_labels(Host)};
classify_name([hackney_pool, Host, new_connection]) ->
    {counter, hackney_pool_new_connection_total, host_labels(Host)};
classify_name(_) ->
    ignore.

%% @doc Return the metric kind for a Prometheus family.
metric_kind(hackney_requests_in_flight) -> gauge;
metric_kind(hackney_host_requests_in_flight) -> gauge;
metric_kind(hackney_requests_total) -> counter;
metric_kind(hackney_requests_finished_total) -> counter;
metric_kind(hackney_request_duration_seconds) -> histogram;
metric_kind(hackney_response_duration_seconds) -> histogram;
metric_kind(hackney_connect_duration_seconds) -> histogram;
metric_kind(hackney_connect_timeouts_total) -> counter;
metric_kind(hackney_connect_errors_total) -> counter;
metric_kind(hackney_pool_take_total) -> counter;
metric_kind(hackney_pool_no_socket_total) -> counter;
metric_kind(hackney_pool_in_use) -> gauge;
metric_kind(hackney_pool_free) -> gauge;
metric_kind(hackney_pool_queue) -> gauge;
metric_kind(hackney_pool_connection_reuse_total) -> counter;
metric_kind(hackney_pool_new_connection_total) -> counter;
metric_kind(_) -> ignore.

%% @doc Normalize host labels.
host_labels(Host) ->
    [{host, normalize_value(Host)}].

%% @doc Normalize pool labels.
pool_labels(Pool) ->
    [{pool, normalize_value(Pool)}].

%% @doc Normalize a label value for Prometheus.
normalize_value(Value) when is_binary(Value) ->
    Value;
normalize_value(Value) when is_atom(Value) ->
    atom_to_binary(Value, utf8);
normalize_value(Value) when is_integer(Value) ->
    integer_to_binary(Value);
normalize_value(Value) when is_list(Value) ->
    iolist_to_binary(Value);
normalize_value(Value) ->
    iolist_to_binary(io_lib:format("~p", [Value])).

%% @doc Create a default counter row.
init_counter(Metric, Labels) ->
    ets:insert_new(?TABLE, {{counter, Metric, Labels}, 0}),
    ok.

%% @doc Create a default gauge row.
init_gauge(Metric, Labels) ->
    ets:insert_new(?TABLE, {{gauge, Metric, Labels}, 0}),
    ok.

%% @doc Create default histogram rows.
init_histogram(Metric, Labels) ->
    ets:insert_new(?TABLE, {{hist_count, Metric, Labels}, 0}),
    ets:insert_new(?TABLE, {{hist_sum_us, Metric, Labels}, 0}),
    ok.

%% @doc Add Delta to a counter row.
add_counter(Metric, Labels, Delta) ->
    ets:update_counter(
        ?TABLE,
        {counter, Metric, Labels},
        {2, Delta},
        {{counter, Metric, Labels}, 0}
    ),
    ok.

%% @doc Add Delta to a gauge row.
add_gauge(Metric, Labels, Delta) ->
    ets:update_counter(
        ?TABLE,
        {gauge, Metric, Labels},
        {2, Delta},
        {{gauge, Metric, Labels}, 0}
    ),
    ok.

%% @doc Set a gauge row to the latest observed value.
set_gauge(Metric, Labels, Value) ->
    ets:insert(?TABLE, {{gauge, Metric, Labels}, Value}),
    ok.

%% @doc Observe a duration metric using fixed Prometheus-ready buckets.
observe_duration(Metric, Labels, ValueMs) ->
    init_histogram(Metric, Labels),
    ValueUs = round(ValueMs * 1000),
    ets:update_counter(
        ?TABLE,
        {hist_count, Metric, Labels},
        {2, 1},
        {{hist_count, Metric, Labels}, 0}
    ),
    ets:update_counter(
        ?TABLE,
        {hist_sum_us, Metric, Labels},
        {2, ValueUs},
        {{hist_sum_us, Metric, Labels}, 0}
    ),
    lists:foreach(
        fun(BoundUs) ->
            case ValueUs =< BoundUs of
                true ->
                    ets:update_counter(
                        ?TABLE,
                        {hist_bucket, Metric, Labels, BoundUs},
                        {2, 1},
                        {{hist_bucket, Metric, Labels, BoundUs}, 0}
                    );
                false ->
                    ok
            end
        end,
        ?DURATION_BUCKETS_US
    ),
    ok.

%% @doc Collect all scalar metrics for a family.
collect_scalar(Type, Name) ->
    Rows = ets:match_object(?TABLE, {{Type, Name, '_'}, '_'}),
    [
        {Labels, Value}
    ||
        {{_, _, Labels}, Value} <- Rows
    ].

%% @doc Collect all histogram metrics for a family.
collect_histogram(Name) ->
    Rows = ets:match_object(?TABLE, {{hist_count, Name, '_'}, '_'}),
    [
        {Labels, histogram_buckets(Name, Labels), Count, SumUs / 1_000_000}
    ||
        {{hist_count, _, Labels}, Count} <- Rows,
        SumUs <- [lookup_value({hist_sum_us, Name, Labels})]
    ].

%% @doc Read histogram buckets for a series.
histogram_buckets(Name, Labels) ->
    [
        {BoundS, lookup_value({hist_bucket, Name, Labels, BoundUs})}
    ||
        {BoundS, BoundUs} <- lists:zip(?DURATION_BUCKETS_S, ?DURATION_BUCKETS_US)
    ].

%% @doc Lookup a single scalar row, returning 0 when absent.
lookup_value(Key) ->
    case ets:lookup(?TABLE, Key) of
        [{Key, Value}] -> Value;
        [] -> 0
    end.

cleanup_test_table() ->
    catch ets:delete(?TABLE),
    ok.

histogram_bucket_value(Buckets, Bound) ->
    proplists:get_value(Bound, Buckets, 0).

collector_smoke_test() ->
    cleanup_test_table(),
    new(counter, [hackney, total_requests]),
    increment_counter([hackney, total_requests], 2),
    MFs = collect_metric_families(),
    ?assert(
        lists:any(
            fun(MF) when element(2, MF) =:= <<"hackney_requests_total">> -> true;
                (_) -> false
            end,
            MFs
        )
    ),
    cleanup_test_table().

counter_and_gauge_test() ->
    cleanup_test_table(),
    new(counter, [hackney, total_requests]),
    new(counter, [hackney, nb_requests]),
    increment_counter([hackney, total_requests], 3),
    increment_counter([hackney, nb_requests], 4),
    decrement_counter([hackney, nb_requests], 1),
    ?assertEqual([{[], 3}], collect(hackney_requests_total)),
    ?assertEqual([{[], 3}], collect(hackney_requests_in_flight)),
    cleanup_test_table().

duration_histogram_test() ->
    cleanup_test_table(),
    new(histogram, [hackney, <<"example.org">>, request_time]),
    update_histogram([hackney, <<"example.org">>, request_time], 250),
    update_histogram([hackney, <<"example.org">>, request_time], 750),
    [{[{host, <<"example.org">>}], Buckets, Count, Sum}] =
        collect(hackney_request_duration_seconds),
    ?assertEqual(2, Count),
    ?assertEqual(1.0, Sum),
    ?assertEqual(0, histogram_bucket_value(Buckets, 0.1)),
    ?assertEqual(1, histogram_bucket_value(Buckets, 0.5)),
    ?assertEqual(2, histogram_bucket_value(Buckets, 1)),
    cleanup_test_table().

collect_metric_families() ->
    Self = self(),
    ok =
        hb_hackney_collector:collect_mf(
            default,
            fun(MF) -> Self ! {metric_family, MF} end
        ),
    drain_metric_families([]).

drain_metric_families(Acc) ->
    receive
        {metric_family, MF} ->
            drain_metric_families([MF | Acc])
    after 0 ->
        lists:reverse(Acc)
    end.
