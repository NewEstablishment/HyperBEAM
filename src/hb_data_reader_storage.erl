%%% @doc Integration layer that bridges HTTP and storage-based data readers.
%%% This module provides the same API as hb_data_reader but intelligently
%%% chooses between storage-hierarchy access and HTTP requests based on
%%% configuration and data characteristics.
%%%
%%% Usage:
%%%   - Drop-in replacement for hb_data_reader
%%%   - Controlled by `use_storage_for_range` option
%%%   - Automatic fallback to HTTP when storage fails
%%%   - Maintains full compatibility with existing code
-module(hb_data_reader_storage).
-export([metadata/2, read_range/4, fetch_full/3, stream/4, stream_from/5, next_chunk/5, chunk_size/1]).
-include("include/hb.hrl").

%% @doc Pass-through chunk size function.
chunk_size(Opts) ->
    hb_data_reader:chunk_size(Opts).

%% @doc Retrieve metadata with intelligent storage vs HTTP selection.
metadata(ID, Opts) when is_binary(ID), is_map(Opts) ->
    case should_use_storage(ID, Opts) of
        true ->
            ?event({trying_storage_metadata, {id, ID}}),
            StartTime = hb_storage_metrics:start_timer(metadata),
            case hb_storage_data_reader:metadata(ID, Opts) of
                {ok, Meta} ->
                    hb_storage_metrics:stop_timer(storage_metadata, StartTime),
                    Size = maps:get(size, Meta, 0),
                    hb_cache_optimizer:record_access_pattern(ID, metadata, Size, Opts),
                    ?event({storage_metadata_success, {id, ID}}),

                    % Consider prefetching related content
                    case hb_cache_optimizer:should_prefetch(ID, metadata, Opts) of
                        true ->
                            spawn(fun() ->
                                Candidates = hb_cache_optimizer:get_prefetch_candidates(ID, Opts),
                                hb_cache_optimizer:warm_cache(Candidates, Opts, Opts)
                            end);
                        false -> ok
                    end,

                    {ok, Meta};
                Error ->
                    hb_storage_metrics:stop_timer(storage_metadata_failed, StartTime),
                    ?event({storage_metadata_failed, {id, ID}, {error, Error}}),
                    hb_storage_metrics:record_fallback(metadata, Error, unknown),
                    fallback_to_http_metadata(ID, Opts)
            end;
        false ->
            ?event({using_http_metadata, {id, ID}}),
            StartTime = hb_storage_metrics:start_timer(http_metadata),
            Result = hb_data_reader:metadata(ID, Opts),
            hb_storage_metrics:stop_timer(http_metadata, StartTime),
            Result
    end.

%% @doc Read range with storage hierarchy preference.
read_range(ID, RangeHeader, Meta, Opts) ->
    Size = maps:get(size, Meta, 0),
    case should_use_storage_for_range(ID, Meta, Opts) of
        true ->
            ?event({trying_storage_range, {id, ID}}),
            StartTime = hb_storage_metrics:start_timer(range_read),
            case hb_storage_data_reader:read_range(ID, RangeHeader, Meta, Opts) of
                {ok, Result} ->
                    hb_storage_metrics:stop_timer(storage_range, StartTime),
                    Store = maps:get(store, Meta, unknown),
                    hb_storage_metrics:record_access(range_read, Store, Size, true),
                    hb_cache_optimizer:record_access_pattern(ID, range_read, Size, Opts),
                    ?event({storage_range_success, {id, ID}}),
                    {ok, Result};
                Error ->
                    hb_storage_metrics:stop_timer(storage_range_failed, StartTime),
                    Store = maps:get(store, Meta, unknown),
                    hb_storage_metrics:record_access(range_read, Store, Size, false),
                    ?event({storage_range_failed, {id, ID}, {error, Error}}),
                    fallback_to_http_range(ID, RangeHeader, Meta, Opts)
            end;
        false ->
            ?event({using_http_range, {id, ID}}),
            StartTime = hb_storage_metrics:start_timer(http_range),
            Result = hb_data_reader:read_range(ID, RangeHeader, Meta, Opts),
            hb_storage_metrics:stop_timer(http_range, StartTime),
            Result
    end.

%% @doc Fetch full data with storage preference.
fetch_full(ID, Meta, Opts) ->
    case should_use_storage_for_full(ID, Meta, Opts) of
        true ->
            ?event({trying_storage_full, {id, ID}}),
            case hb_storage_data_reader:fetch_full(ID, Meta, Opts) of
                {ok, Result} ->
                    ?event({storage_full_success, {id, ID}}),
                    {ok, Result};
                Error ->
                    ?event({storage_full_failed, {id, ID}, {error, Error}}),
                    fallback_to_http_full(ID, Meta, Opts)
            end;
        false ->
            ?event({using_http_full, {id, ID}}),
            hb_data_reader:fetch_full(ID, Meta, Opts)
    end.

%% @doc Stream with storage-aware chunking.
stream(ID, Meta, ChunkFun, Opts) when is_function(ChunkFun, 2) ->
    case should_use_storage_for_stream(ID, Meta, Opts) of
        true ->
            ?event({trying_storage_stream, {id, ID}}),
            case hb_storage_data_reader:stream(ID, Meta, ChunkFun, Opts) of
                {ok, Result} ->
                    ?event({storage_stream_success, {id, ID}}),
                    {ok, Result};
                Error ->
                    ?event({storage_stream_failed, {id, ID}, {error, Error}}),
                    fallback_to_http_stream(ID, Meta, ChunkFun, Opts)
            end;
        false ->
            ?event({using_http_stream, {id, ID}}),
            hb_data_reader:stream(ID, Meta, ChunkFun, Opts)
    end.

%% @doc Stream from offset with storage support.
stream_from(ID, Meta, Offset, ChunkFun, Opts) when is_function(ChunkFun, 2) ->
    case should_use_storage_for_stream(ID, Meta, Opts) of
        true ->
            ?event({trying_storage_stream_from, {id, ID}, {offset, Offset}}),
            case hb_storage_data_reader:stream_from(ID, Meta, Offset, ChunkFun, Opts) of
                {ok, Result} ->
                    ?event({storage_stream_from_success, {id, ID}}),
                    {ok, Result};
                Error ->
                    ?event({storage_stream_from_failed, {id, ID}, {error, Error}}),
                    hb_data_reader:stream_from(ID, Meta, Offset, ChunkFun, Opts)
            end;
        false ->
            ?event({using_http_stream_from, {id, ID}}),
            hb_data_reader:stream_from(ID, Meta, Offset, ChunkFun, Opts)
    end.

%% @doc Get next chunk with storage preference.
next_chunk(ID, Meta, Offset, ChunkSize, Opts) ->
    case should_use_storage_for_range(ID, Meta, Opts) of
        true ->
            case hb_storage_data_reader:next_chunk(ID, Meta, Offset, ChunkSize, Opts) of
                {ok, Result} -> {ok, Result};
                Error ->
                    ?event({storage_next_chunk_failed, {id, ID}, {error, Error}}),
                    hb_data_reader:next_chunk(ID, Meta, Offset, ChunkSize, Opts)
            end;
        false ->
            hb_data_reader:next_chunk(ID, Meta, Offset, ChunkSize, Opts)
    end.

%% Internal decision logic

%% @doc Decide whether to use storage hierarchy for initial metadata.
should_use_storage(ID, Opts) ->
    BaseDecision = hb_opts:get(use_storage_for_range, true, Opts),
    case BaseDecision of
        false -> false;
        true ->
            % Additional heuristics can be added here
            case is_likely_cached(ID, Opts) of
                true -> true;
                false ->
                    % Still try storage first for smaller files
                    case is_small_file_hint(ID) of
                        true -> true;
                        false -> hb_opts:get(storage_try_large_files, false, Opts)
                    end
            end
    end.

%% @doc Decide whether to use storage for range requests.
should_use_storage_for_range(ID, Meta, Opts) ->
    case hb_opts:get(use_intelligent_storage, true, Opts) of
        true ->
            % Use intelligent decision making
            intelligent_storage_decision(ID, Meta, Opts);
        false ->
            % Use original logic
            case should_use_storage(ID, Opts) of
                false -> false;
                true ->
                    % Don't use storage for very large ranges unless it supports native ranges
                    case maps:get(store, Meta, undefined) of
                        Store when Store =/= undefined ->
                            % We know which store has it, check if it supports ranges
                            case hb_store:supports_range(Store) of
                                true -> true;  % Native range support, always good
                                false ->
                                    % Fallback implementation - check if file is reasonably sized
                                    Size = maps:get(size, Meta, 0),
                                    MaxFallbackSize = hb_opts:get(storage_max_fallback_size, 50 * 1024 * 1024, Opts),  % 50MB default
                                    Size =< MaxFallbackSize
                            end;
                        _ -> true  % No specific store info, let hierarchy decide
                    end
            end
    end.

%% @doc Decide whether to use storage for full data fetch.
should_use_storage_for_full(ID, Meta, Opts) ->
    case should_use_storage(ID, Opts) of
        false -> false;
        true ->
            Size = maps:get(size, Meta, 0),
            MaxFullSize = hb_opts:get(storage_max_full_size, 100 * 1024 * 1024, Opts),  % 100MB default
            Size =< MaxFullSize
    end.

%% @doc Decide whether to use storage for streaming.
should_use_storage_for_stream(ID, Meta, Opts) ->
    case should_use_storage(ID, Opts) of
        false -> false;
        true ->
            % Streaming is generally good for storage since we can chunk efficiently
            case maps:get(store, Meta, undefined) of
                Store when Store =/= undefined ->
                    % Check if store supports native ranges for efficient streaming
                    case hb_store:supports_range(Store) of
                        true -> true;  % Excellent for streaming
                        false ->
                            % Still okay for moderately sized files
                            Size = maps:get(size, Meta, 0),
                            MaxStreamSize = hb_opts:get(storage_max_stream_size, 200 * 1024 * 1024, Opts),  % 200MB default
                            Size =< MaxStreamSize
                    end;
                _ -> true  % Let hierarchy decide
            end
    end.

%% Fallback functions

%% @doc Fallback to HTTP metadata when storage fails.
fallback_to_http_metadata(ID, Opts) ->
    ?event({falling_back_to_http_metadata, {id, ID}}),
    hb_data_reader:metadata(ID, Opts).

%% @doc Fallback to HTTP range when storage fails.
fallback_to_http_range(ID, RangeHeader, Meta, Opts) ->
    ?event({falling_back_to_http_range, {id, ID}}),
    hb_data_reader:read_range(ID, RangeHeader, Meta, Opts).

%% @doc Fallback to HTTP full fetch when storage fails.
fallback_to_http_full(ID, Meta, Opts) ->
    ?event({falling_back_to_http_full, {id, ID}}),
    hb_data_reader:fetch_full(ID, Meta, Opts).

%% @doc Fallback to HTTP streaming when storage fails.
fallback_to_http_stream(ID, Meta, ChunkFun, Opts) ->
    ?event({falling_back_to_http_stream, {id, ID}}),
    hb_data_reader:stream(ID, Meta, ChunkFun, Opts).

%% Heuristic helpers

%% @doc Guess if data is likely to be cached locally.
is_likely_cached(ID, Opts) ->
    % Enhanced heuristics based on multiple factors
    Score = cache_likelihood_score(ID, Opts),
    Score >= 0.5.  % Threshold for considering "likely cached"

%% @doc Calculate cache likelihood score (0.0 to 1.0).
cache_likelihood_score(ID, _Opts) ->
    BaseScore = 0.0,

    % Factor 1: Explicit cache keywords
    Score1 = case binary:match(ID, [<<"cache">>, <<"local">>, <<"temp">>]) of
        nomatch -> BaseScore;
        _ -> BaseScore + 0.4
    end,

    % Factor 2: ID length heuristic (shorter = more recent = more likely cached)
    IDSize = byte_size(ID),
    Score2 = case IDSize of
        Size when Size =< 32 -> Score1 + 0.3;   % Very short, likely recent
        Size when Size =< 43 -> Score1 + 0.2;   % Standard AO ID length
        Size when Size =< 64 -> Score1 + 0.1;   % Longer, less likely recent
        _ -> Score1
    end,

    % Factor 3: File type hints
    Score3 = case is_small_file_hint(ID) of
        true -> Score2 + 0.2;   % Small files more likely to be cached
        false -> Score2
    end,

    % Factor 4: Common patterns that indicate metadata/config
    Score4 = case binary:match(ID, [<<"process">>, <<"scheduler">>, <<"cron">>, <<"state">>]) of
        nomatch -> Score3;
        _ -> Score3 + 0.3  % Process-related data often cached
    end,

    % Cap at 1.0
    min(Score4, 1.0).

%% @doc Guess if file is likely to be small based on ID patterns.
is_small_file_hint(ID) ->
    % Enhanced patterns for small file detection
    SmallFilePatterns = [
        <<".json">>, <<".txt">>, <<".md">>, <<".yml">>, <<".yaml">>,
        <<".toml">>, <<".ini">>, <<".cfg">>, <<".conf">>,
        <<"manifest">>, <<"config">>, <<"metadata">>, <<"index">>,
        <<"readme">>, <<"license">>, <<"changelog">>
    ],
    case binary:match(ID, SmallFilePatterns) of
        nomatch -> false;
        _ -> true
    end.

%% @doc Enhanced decision making for storage vs HTTP based on context.
intelligent_storage_decision(ID, Meta, Opts) ->
    % Base decision from configuration
    BaseDecision = hb_opts:get(use_storage_for_range, true, Opts),

    case BaseDecision of
        false -> false;
        true ->
            % Apply intelligent heuristics
            Size = maps:get(size, Meta, 0),
            Store = maps:get(store, Meta, undefined),

            % Factor 1: Cache likelihood
            CacheLikelihood = cache_likelihood_score(ID, Opts),

            % Factor 2: Store capability and size
            StoreScore = case Store of
                undefined -> 0.3;  % Unknown store, moderate confidence
                StoreMap when is_map(StoreMap) ->
                    case maps:get(<<"store-module">>, StoreMap, undefined) of
                        <<"hb_store_lmdb">> -> 0.9;      % LMDB is fast even for large reads
                        <<"hb_store_fs">> when Size =< 10*1024*1024 -> 0.8;  % FS good for small files
                        <<"hb_store_fs">> -> 0.6;       % FS okay for larger files
                        <<"hb_store_gateway">> -> 0.4;  % Gateway similar to HTTP
                        _ -> 0.5
                    end;
                _ -> 0.5
            end,

            % Factor 3: Size penalties
            SizeScore = case Size of
                SizeVal when SizeVal =< 1024*1024 -> 1.0;           % Small files: always prefer storage
                SizeVal when SizeVal =< 10*1024*1024 -> 0.8;        % Medium files: usually prefer storage
                SizeVal when SizeVal =< 100*1024*1024 -> 0.6;       % Large files: conditionally prefer storage
                _ -> 0.3                                 % Very large files: usually prefer HTTP
            end,

            % Combined score
            CombinedScore = (CacheLikelihood * 0.4) + (StoreScore * 0.4) + (SizeScore * 0.2),

            % Decision threshold
            Decision = CombinedScore >= 0.6,

            ?event({intelligent_storage_decision,
                {id, ID},
                {cache_likelihood, CacheLikelihood},
                {store_score, StoreScore},
                {size_score, SizeScore},
                {combined_score, CombinedScore},
                {decision, Decision}
            }),

            Decision
    end.