%%% @doc Performance metrics and monitoring for storage-aware data access.
%%% This module tracks storage vs HTTP performance, cache hit rates,
%%% and access patterns to optimize the storage hierarchy.
-module(hb_storage_metrics).
-export([
    start_timer/1,
    stop_timer/2,
    record_access/4,
    record_fallback/3,
    get_stats/0,
    reset_stats/0
]).
-include("include/hb.hrl").

%% @doc Start a performance timer for an operation.
start_timer(Operation) ->
    erlang:system_time(microsecond).

%% @doc Stop timer and record the elapsed time.
stop_timer(Operation, StartTime) ->
    EndTime = erlang:system_time(microsecond),
    ElapsedMicros = EndTime - StartTime,
    ElapsedMs = ElapsedMicros / 1000,
    ?event({performance_timing, {operation, Operation}, {elapsed_ms, ElapsedMs}}),
    ElapsedMs.

%% @doc Record a storage access attempt.
record_access(Operation, Store, Size, Success) ->
    StoreModule = case is_map(Store) of
        true -> maps:get(<<"store-module">>, Store, unknown);
        false -> Store
    end,
    ?event({storage_access,
        {operation, Operation},
        {store, StoreModule},
        {size, Size},
        {success, Success}
    }).

%% @doc Record when storage fails and falls back to HTTP.
record_fallback(Operation, Reason, FallbackSuccess) ->
    ?event({storage_fallback,
        {operation, Operation},
        {reason, Reason},
        {fallback_success, FallbackSuccess}
    }).

%% @doc Get performance statistics (placeholder for future implementation).
get_stats() ->
    #{
        message => <<"Statistics collection not yet implemented">>,
        note => <<"Use debug events for now - search logs for performance_timing, storage_access, storage_fallback">>
    }.

%% @doc Reset performance statistics (placeholder).
reset_stats() ->
    ?event({stats_reset, {timestamp, erlang:system_time(second)}}),
    ok.