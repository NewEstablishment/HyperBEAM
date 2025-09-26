%%% @doc A storage-hierarchy-aware data reader that can efficiently serve
%%% range requests directly from local stores (LMDB, filesystem) before
%%% falling back to HTTP requests. This module mirrors the API of hb_data_reader
%%% but leverages the storage hierarchy for improved performance.
%%%
%%% Key benefits:
%%% - Serves hot data from LMDB with sub-millisecond latency
%%% - Uses efficient file seeking for filesystem-cached data
%%% - Falls back gracefully to HTTP when data is not locally cached
%%% - Maintains streaming support across all storage backends
%%% - Preserves all existing hb_data_reader functionality
-module(hb_storage_data_reader).
-export([metadata/2, read_range/4, fetch_full/3, stream/4, stream_from/5, next_chunk/5, chunk_size/1]).
-include("include/hb.hrl").

-define(DEFAULT_CHUNK_SIZE, 1024 * 1024).

%% @doc Get chunk size from configuration, same as original data reader.
chunk_size(Opts) ->
    case hb_opts:get(stream_chunk_size, ?DEFAULT_CHUNK_SIZE, Opts) of
        Size when is_integer(Size), Size > 0 -> Size;
        _ -> ?DEFAULT_CHUNK_SIZE
    end.

%% @doc Retrieve dataset metadata using storage hierarchy first, then HTTP fallback.
metadata(ID, Opts) when is_binary(ID), is_map(Opts) ->
    case metadata_from_stores(ID, Opts) of
        {ok, Meta} ->
            ?event({storage_metadata_hit, {id, ID}, {meta, Meta}}),
            {ok, Meta};
        not_found ->
            ?event({storage_metadata_miss, {id, ID}}),
            % Fallback to HTTP-based metadata
            hb_data_reader:metadata(ID, Opts)
    end.

%% @doc Try to get metadata from storage hierarchy.
metadata_from_stores(ID, Opts) ->
    Stores = hb_opts:get(store, [], Opts),
    metadata_from_stores_list(ID, Stores, Opts).

metadata_from_stores_list(_, [], _) -> not_found;
metadata_from_stores_list(ID, [Store | Rest], Opts) ->
    case try_store_metadata(Store, ID, Opts) of
        {ok, Meta} -> {ok, Meta};
        _ -> metadata_from_stores_list(ID, Rest, Opts)
    end.

try_store_metadata(Store, ID, Opts) ->
    Module = maps:get(<<"store-module">>, Store, undefined),
    case Module of
        undefined -> not_found;
        _ ->
            case hb_store:get_size(Store, ID) of
                {ok, Size} ->
                    {ok, #{
                        size => Size,
                        content_type => detect_content_type(ID, Opts),
                        store => Store  % Remember which store has it
                    }};
                _ -> not_found
            end
    end.

%% @doc Detect content type from ID or use default.
detect_content_type(ID, Opts) ->
    % Simple content type detection based on common patterns
    case binary:match(ID, [<<".jpg">>, <<".jpeg">>, <<".png">>, <<".gif">>]) of
        nomatch ->
            case binary:match(ID, [<<".json">>, <<".txt">>, <<".md">>]) of
                nomatch -> hb_opts:get(range_default_content_type, <<"application/octet-stream">>, Opts);
                _ -> <<"text/plain">>
            end;
        _ -> <<"image/jpeg">>  % Default for images
    end.

%% @doc Materialize bytes defined by a Range header using storage hierarchy.
read_range(ID, RangeHeader, Meta = #{size := Total}, Opts) when is_binary(RangeHeader) ->
    case hb_http_range:parse(RangeHeader, Total) of
        {ok, {Start, End}} ->
            case maps:get(store, Meta, undefined) of
                undefined ->
                    % No specific store found, try hierarchy
                    read_range_from_hierarchy(ID, Start, End, Meta, Opts);
                Store ->
                    % Use the store that had the metadata
                    read_range_from_store(Store, ID, Start, End, Meta, Opts)
            end;
        {error, {range_not_satisfiable, _}} ->
            {error, {range_not_satisfiable, Total}};
        {error, invalid_range} ->
            {error, {invalid_range, Total}}
    end;
read_range(_, _, _, _) -> {error, invalid_arguments}.

%% @doc Read range from a specific store.
read_range_from_store(Store, ID, Start, End, Meta, Opts) ->
    Module = maps:get(<<"store-module">>, Store),
    case hb_store:supports_range(Store) of
        true ->
            ?event({using_native_range, {store, Module}, {id, ID}, {start, Start}, {'end', End}}),
            case hb_store:read_range(Store, ID, Start, End) of
                {ok, Data} ->
                    {ok, #{
                        body => Data,
                        start => Start,
                        range_end => End,
                        total => maps:get(size, Meta),
                        content_type => maps:get(content_type, Meta),
                        final => End >= (maps:get(size, Meta) - 1)
                    }};
                Error -> Error
            end;
        false ->
            ?event({using_fallback_range, {store, Module}, {id, ID}}),
            fallback_range_read(Store, ID, Start, End, Meta, Opts)
    end.

%% @doc Read range from storage hierarchy when no specific store is known.
read_range_from_hierarchy(ID, Start, End, Meta, Opts) ->
    Stores = hb_opts:get(store, [], Opts),
    read_range_from_stores_list(ID, Start, End, Meta, Stores, Opts).

read_range_from_stores_list(_, _, _, _, [], _) -> not_found;
read_range_from_stores_list(ID, Start, End, Meta, [Store | Rest], Opts) ->
    case read_range_from_store(Store, ID, Start, End, Meta, Opts) of
        {ok, Result} -> {ok, Result};
        _ -> read_range_from_stores_list(ID, Start, End, Meta, Rest, Opts)
    end.

%% @doc Fallback range read for stores that don't support native ranges.
fallback_range_read(Store, ID, Start, End, Meta, _Opts) ->
    case hb_store:read(Store, ID) of
        {ok, FullData} ->
            Size = byte_size(FullData),
            ActualEnd = min(End, Size - 1),
            Length = ActualEnd - Start + 1,
            case Start + Length =< Size of
                true ->
                    RangeData = binary:part(FullData, Start, Length),
                    {ok, #{
                        body => RangeData,
                        start => Start,
                        range_end => ActualEnd,
                        total => Size,
                        content_type => maps:get(content_type, Meta),
                        final => ActualEnd >= Size - 1
                    }};
                false ->
                    {error, range_not_satisfiable}
            end;
        not_found -> not_found;
        Error -> Error
    end.

%% @doc Fetch the entire body using storage hierarchy when possible.
fetch_full(_ID, #{size := 0, content_type := CType}, _Opts) ->
    {ok, #{ data => <<>>, content_type => CType }};
fetch_full(ID, Meta = #{size := Total}, Opts) when Total > 0 ->
    case maps:get(store, Meta, undefined) of
        Store when Store =/= undefined ->
            % Try to read full data from the identified store
            case hb_store:read(Store, ID) of
                {ok, Data} ->
                    {ok, #{
                        data => Data,
                        content_type => maps:get(content_type, Meta)
                    }};
                _ ->
                    % Fallback to HTTP if store read fails
                    hb_data_reader:fetch_full(ID, Meta, Opts)
            end;
        _ ->
            % Try storage hierarchy
            Stores = hb_opts:get(store, [], Opts),
            case fetch_full_from_stores(ID, Stores, Meta, Opts) of
                {ok, Result} -> {ok, Result};
                _ -> hb_data_reader:fetch_full(ID, Meta, Opts)
            end
    end.

%% @doc Try to fetch full data from storage hierarchy.
fetch_full_from_stores(_, [], _, _) -> not_found;
fetch_full_from_stores(ID, [Store | Rest], Meta, Opts) ->
    case hb_store:read(Store, ID) of
        {ok, Data} ->
            {ok, #{
                data => Data,
                content_type => maps:get(content_type, Meta)
            }};
        _ -> fetch_full_from_stores(ID, Rest, Meta, Opts)
    end.

%% @doc Stream from the beginning using storage-aware chunking.
stream(ID, Meta = #{size := Total}, ChunkFun, Opts) when is_function(ChunkFun, 2) ->
    case Total of
        0 ->
            ChunkFun(<<>>, true),
            {ok, Meta};
        _ ->
            ChunkSize = chunk_size(Opts),
            case maps:get(store, Meta, undefined) of
                Store when Store =/= undefined ->
                    stream_with_store(Store, ID, Total, ChunkSize, ChunkFun, Opts);
                _ ->
                    % Try storage hierarchy for streaming
                    Stores = hb_opts:get(store, [], Opts),
                    case stream_from_stores(ID, Total, ChunkSize, ChunkFun, Stores, Opts) of
                        {ok, Result} -> {ok, Result};
                        _ -> hb_data_reader:stream(ID, Meta, ChunkFun, Opts)
                    end
            end
    end.

%% @doc Continue streaming from a specific byte offset.
stream_from(ID, Meta, Offset, ChunkFun, Opts) when is_function(ChunkFun, 2) ->
    ChunkSize = chunk_size(Opts),
    Total = maps:get(size, Meta),
    case maps:get(store, Meta, undefined) of
        Store when Store =/= undefined ->
            stream_loop_with_store(Store, ID, Offset, Total, ChunkSize, ChunkFun, Opts);
        _ ->
            hb_data_reader:stream_from(ID, Meta, Offset, ChunkFun, Opts)
    end.

%% @doc Retrieve next chunk for preflight checks.
next_chunk(_ID, _Meta = #{size := Total}, Offset, _ChunkSize, _Opts) when Offset >= Total ->
    {error, done};
next_chunk(ID, Meta = #{size := Total}, Offset, ChunkSize, Opts) ->
    Normalized = case ChunkSize > 0 of true -> ChunkSize; false -> ?DEFAULT_CHUNK_SIZE end,
    {Start, End, _} = compute_next_range(Offset, Total, Normalized),
    case maps:get(store, Meta, undefined) of
        Store when Store =/= undefined ->
            read_range_from_store(Store, ID, Start, End, Meta, Opts);
        _ ->
            read_range_from_hierarchy(ID, Start, End, Meta, Opts)
    end.

%% Internal streaming helpers

%% @doc Stream from a specific store.
stream_with_store(Store, ID, Total, ChunkSize, ChunkFun, Opts) ->
    Module = maps:get(<<"store-module">>, Store),
    case hb_store:supports_range(Store) of
        true ->
            ?event({streaming_with_native_range, {store, Module}, {id, ID}}),
            stream_loop_with_store(Store, ID, 0, Total, ChunkSize, ChunkFun, Opts);
        false ->
            ?event({streaming_with_full_read, {store, Module}, {id, ID}}),
            stream_with_full_read(Store, ID, Total, ChunkSize, ChunkFun, Opts)
    end.

%% @doc Stream by reading the full data and chunking it.
stream_with_full_read(Store, ID, Total, ChunkSize, ChunkFun, _Opts) ->
    case hb_store:read(Store, ID) of
        {ok, FullData} ->
            stream_chunks_from_binary(FullData, ChunkSize, ChunkFun),
            {ok, #{size => Total}};
        Error -> Error
    end.

%% @doc Stream chunks from a binary in memory.
stream_chunks_from_binary(Data, ChunkSize, ChunkFun) ->
    stream_chunks_from_binary(Data, 0, byte_size(Data), ChunkSize, ChunkFun).

stream_chunks_from_binary(Data, Offset, Total, ChunkSize, ChunkFun) when Offset < Total ->
    End = min(Offset + ChunkSize, Total),
    Length = End - Offset,
    Chunk = binary:part(Data, Offset, Length),
    IsFinal = End >= Total,
    ChunkFun(Chunk, IsFinal),
    case IsFinal of
        true -> ok;
        false -> stream_chunks_from_binary(Data, End, Total, ChunkSize, ChunkFun)
    end;
stream_chunks_from_binary(_, _, _, _, _) -> ok.

%% @doc Stream using range reads from a store.
stream_loop_with_store(_, _, Offset, Total, _, _, _) when Offset >= Total ->
    {ok, done};
stream_loop_with_store(Store, ID, Offset, Total, ChunkSize, ChunkFun, Opts) ->
    {Start, End, IsFinal} = compute_next_range(Offset, Total, ChunkSize),
    case hb_store:read_range(Store, ID, Start, End) of
        {ok, Chunk} ->
            ChunkFun(Chunk, IsFinal),
            case IsFinal of
                true -> {ok, #{size => Total}};
                false -> stream_loop_with_store(Store, ID, End + 1, Total, ChunkSize, ChunkFun, Opts)
            end;
        Error -> Error
    end.

%% @doc Try streaming from storage hierarchy.
stream_from_stores(_, _, _, _, [], _) -> not_found;
stream_from_stores(ID, Total, ChunkSize, ChunkFun, [Store | Rest], Opts) ->
    case stream_with_store(Store, ID, Total, ChunkSize, ChunkFun, Opts) of
        {ok, Result} -> {ok, Result};
        _ -> stream_from_stores(ID, Total, ChunkSize, ChunkFun, Rest, Opts)
    end.

%% @doc Compute the next range for streaming (same as original).
compute_next_range(Offset, Total, ChunkSize) when Offset < Total, ChunkSize > 0 ->
    Start = Offset,
    End = erlang:min(Start + ChunkSize - 1, Total - 1),
    {Start, End, End >= Total - 1};
compute_next_range(_, Total, _) when Total =< 0 -> {0, -1, true};
compute_next_range(_, _, _) -> {0, -1, true}.