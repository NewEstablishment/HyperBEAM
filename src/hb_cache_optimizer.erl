%%% @doc Cache optimization and warming for storage-aware data access.
%%% Implements predictive caching, access pattern tracking, and cache warming.
-module(hb_cache_optimizer).
-export([
    record_access_pattern/4,
    should_prefetch/3,
    get_prefetch_candidates/2,
    warm_cache/3,
    optimize_storage_hierarchy/2
]).
-include("include/hb.hrl").

-define(ACCESS_PATTERN_TTL, 3600). % 1 hour in seconds
-define(MIN_ACCESS_COUNT, 3).      % Minimum accesses before considering prefetch

%% @doc Record access pattern for future optimization.
record_access_pattern(ID, AccessType, Size, Opts) ->
    case hb_opts:get(range_cache_enabled, true, Opts) of
        true ->
            ?event({access_pattern,
                {id, ID},
                {type, AccessType},
                {size, Size},
                {timestamp, erlang:system_time(second)}
            }),
            % In a full implementation, this would store to a persistent cache
            ok;
        false ->
            ok
    end.

%% @doc Determine if we should prefetch related data.
should_prefetch(ID, AccessType, Opts) ->
    case hb_opts:get(range_prefetch_enabled, false, Opts) of
        false -> false;
        true ->
            % Simple heuristics for prefetch decisions
            case AccessType of
                metadata -> true;   % Always prefetch after metadata access
                range_read when byte_size(ID) =< 43 -> true;  % Prefetch for recent IDs
                full_read -> false;  % Don't prefetch after full reads
                _ -> false
            end
    end.

%% @doc Get candidates for prefetching based on access patterns.
get_prefetch_candidates(ID, Opts) ->
    case should_prefetch(ID, metadata, Opts) of
        true ->
            % Generate predictive candidates
            Candidates = generate_related_ids(ID),
            ?event({prefetch_candidates, {base_id, ID}, {candidates, Candidates}}),
            Candidates;
        false ->
            []
    end.

%% @doc Generate related IDs that might be accessed together.
generate_related_ids(ID) ->
    % Simple heuristics for related content
    IDStr = binary_to_list(ID),
    case length(IDStr) of
        Len when Len >= 10 ->
            % Generate variations based on common patterns
            Prefix = list_to_binary(lists:sublist(IDStr, Len - 2)),
            Suffix = list_to_binary(lists:nthtail(Len - 2, IDStr)),

            % Common related patterns
            [
                <<Prefix/binary, "01">>,  % Sequential IDs
                <<Prefix/binary, "02">>,
                <<ID/binary, ".json">>,   % Metadata files
                <<ID/binary, ".meta">>,
                <<"index-", ID/binary>>   % Index files
            ];
        _ ->
            []
    end.

%% @doc Warm cache with frequently accessed data.
warm_cache(IDs, StoreOpts, Opts) when is_list(IDs) ->
    case hb_opts:get(range_cache_enabled, true, Opts) of
        true ->
            ?event({cache_warming, {count, length(IDs)}}),
            lists:foreach(fun(ID) ->
                warm_single_item(ID, StoreOpts, Opts)
            end, IDs),
            ok;
        false ->
            ok
    end.

%% @doc Warm a single cache item.
warm_single_item(ID, StoreOpts, Opts) ->
    try
        case hb_storage_data_reader:metadata(ID, Opts) of
            {ok, Meta} ->
                Size = maps:get(size, Meta, 0),
                case Size =< 1024*1024 of  % Only warm small files
                    true ->
                        ?event({warming_cache, {id, ID}, {size, Size}}),
                        _ = hb_storage_data_reader:fetch_full(ID, Meta, Opts),
                        ok;
                    false ->
                        ok
                end;
            _ ->
                ok
        end
    catch
        _:_ ->
            ?event({cache_warm_failed, {id, ID}}),
            ok
    end.

%% @doc Optimize storage hierarchy based on access patterns.
optimize_storage_hierarchy(AccessPatterns, Opts) ->
    case hb_opts:get(range_cache_enabled, true, Opts) of
        true ->
            % Analyze patterns and suggest optimizations
            Suggestions = analyze_access_patterns(AccessPatterns),
            ?event({storage_optimization_suggestions, {suggestions, Suggestions}}),
            Suggestions;
        false ->
            []
    end.

%% @doc Analyze access patterns to generate optimization suggestions.
analyze_access_patterns(Patterns) ->
    % Simple pattern analysis - could be much more sophisticated
    FrequentIDs = find_frequent_accesses(Patterns),
    LargeFiles = find_large_file_accesses(Patterns),
    RangeHeavy = find_range_heavy_accesses(Patterns),

    Suggestions = [],

    % Suggest moving frequently accessed items to faster storage
    Suggestions1 = case length(FrequentIDs) > 0 of
        true ->
            [#{
                type => move_to_lmdb,
                reason => <<"Frequently accessed items should be in LMDB">>,
                items => FrequentIDs
            } | Suggestions];
        false -> Suggestions
    end,

    % Suggest range optimization for large files
    Suggestions2 = case length(LargeFiles) > 0 of
        true ->
            [#{
                type => optimize_large_files,
                reason => <<"Large files should use filesystem with range support">>,
                items => LargeFiles
            } | Suggestions1];
        false -> Suggestions1
    end,

    % Suggest chunking strategy for range-heavy access
    case length(RangeHeavy) > 0 of
        true ->
            [#{
                type => chunking_strategy,
                reason => <<"Range-heavy files should be pre-chunked">>,
                items => RangeHeavy
            } | Suggestions2];
        false -> Suggestions2
    end.

%% Helper functions for pattern analysis

find_frequent_accesses(_Patterns) ->
    % Placeholder - would analyze actual access frequency
    [].

find_large_file_accesses(_Patterns) ->
    % Placeholder - would identify large file patterns
    [].

find_range_heavy_accesses(_Patterns) ->
    % Placeholder - would identify files with many range requests
    [].