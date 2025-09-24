%%% @doc A store module that reads data from S3-compatible storage backends.
%%% This module implements the standard HyperBEAM store interface and provides
%%% transparent access to ANS-104 DataItems stored in S3 buckets.
-module(hb_store_s3).
-export([scope/1, type/2, read/2, resolve/2, list/2]).
-export([data/2, data_with_range/3, stream_data/3, get_stream_info/2]).
-include("include/hb.hrl").

%% @doc The scope of an S3 store is remote as it involves network operations.
scope(_) -> remote.
resolve(_, Key) -> Key.

%% @doc List operation is not supported for S3 store as it would be expensive.
list(_StoreOpts, _Key) ->
    not_found.

%% @doc Get the type of the data at the given key by attempting to read it.
type(StoreOpts, Key) ->
    case read(StoreOpts, Key) of
        not_found -> not_found;
        {ok, Data} ->
            ?event({s3_store_type, hb_private:reset(hb_message:uncommitted(Data, StoreOpts))}),
            IsFlat = lists:all(
                fun({_, Value}) -> not is_map(Value) end,
                hb_maps:to_list(
                    hb_private:reset(
                        hb_message:uncommitted(Data, StoreOpts)
                    ),
                    StoreOpts
                )
            ),
            if
                IsFlat -> simple;
                true -> composite
            end
    end.

%% @doc Read the data at the given key from S3. Will only attempt to read
%% the data if the key is an ID and S3 is enabled in configuration.
read(StoreOpts, Key) ->
    case {is_s3_enabled(StoreOpts), hb_path:term_to_path_parts(Key, StoreOpts)} of
        {true, [ID]} when ?IS_ID(ID) ->
            ?event({s3_store_read, StoreOpts, Key}),
            case s3_read(ID, StoreOpts) of
                {error, _} ->
                    ?event(s3_store, {read_not_found, {key, ID}}),
                    not_found;
                not_found ->
                    ?event(s3_store, {read_not_found, {key, ID}}),
                    not_found;
                {ok, Message} ->
                    ?event({s3_store_read_found, {key, ID}}),
                    {ok, Message}
            end;
        {false, _} ->
            ?event({s3_store_disabled, Key}),
            not_found;
        {_, _} ->
            ?event({s3_store_ignoring_non_id, Key}),
            not_found
    end.

%% @doc Check if S3 is enabled in the store configuration.
is_s3_enabled(StoreOpts) ->
    case hb_maps:get(<<"enabled">>, StoreOpts, true, StoreOpts) of
        true ->
            % Also check global S3 enabled option
            hb_opts:get(s3_enabled, true, StoreOpts);
        false ->
            false
    end.

%% @doc Read ANS-104 DataItem from S3's bucket using erlcloud backend
s3_read(ID, Opts) ->
    Bucket = get_bucket(Opts),
    Key = <<ID/binary, ".ans104">>,
    ?event({s3_read, {id, ID}, {bucket, Bucket}, {key, Key}}),
    S3Config = hb_s3_config:load_s3_device_config(),
    % Avoid downloading the full object: fetch only header + tags then parse.
    case read_headers(ID, Opts) of
        {ok, #{data_offset := DataOffset}} when is_integer(DataOffset), DataOffset > 0 ->
            HeaderEnd = DataOffset - 1,
            Range = <<"bytes=0-", (integer_to_binary(HeaderEnd))/binary>>,
            ?event({s3_fetching_header_range, Range}),
            case hb_s3_erlcloud:get_object(Bucket, Key, #{range => Range}, S3Config) of
                {ok, #{<<"body">> := Partial, <<"status">> := _Status}} ->
                    ?event({s3_retrieved_header_bytes, byte_size_safe(Partial)}),
                    Result = parse_stored_ans104(Partial, Opts),
                    ?event({s3_parse_result, element(1, Result)}),
                    Result;
                {error, #{<<"status">> := 404}} ->
                    ?event({s3_header_404}),
                    not_found;
                {error, Reason} ->
                    ?event({s3_header_error, Reason}),
                    {error, Reason}
            end;
        not_found ->
            not_found;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Get raw data from ANS-104 DataItem using range requests
%% Allow both invocation orders: (StoreOpts, ID) and (ID, Opts)
data(StoreOpts, ID) when is_map(StoreOpts), is_binary(ID) ->
    data(ID, StoreOpts);
data(ID, Opts) ->
    case read_data_field_only(ID, Opts) of
        {ok, Data} -> {ok, Data};
        {error, Reason} -> {error, Reason};
        not_found -> not_found
    end.

%% @doc Get data with range support for streaming
%% Allow both invocation orders: (StoreOpts, ID, Range) and (ID, Range, Opts)
data_with_range(StoreOpts, ID, Range) when is_map(StoreOpts), is_binary(ID), is_binary(Range) ->
    data_with_range(ID, Range, StoreOpts);
data_with_range(ID, Range, Opts) ->
    Bucket = get_bucket(Opts),
    Key = <<ID/binary, ".ans104">>,
    S3Config = hb_s3_config:load_s3_device_config(),

    case read_headers(ID, Opts) of
        {ok, #{data_offset := DataOffset}} ->
            % Convert logical range to physical S3 range
            S3Range = convert_range_to_s3(Range, DataOffset),
            case hb_s3_erlcloud:get_object(Bucket, Key, #{range => S3Range}, S3Config) of
                {ok, #{<<"body">> := Data}} ->
                    {ok, Data};
                {error, Reason} ->
                    {error, Reason}
            end;
        Error ->
            Error
    end.

%% @doc Stream data from S3 with chunked reading
stream_data(ID, ChunkFun, Opts) ->
    case get_stream_info(ID, Opts) of
        {ok, #{total_size := TotalSize}} ->
            stream_chunks(ID, 0, TotalSize, ChunkFun, Opts);
        Error ->
            Error
    end.

%% @doc Get streaming information for an object
%% Allow both invocation orders: (StoreOpts, ID) and (ID, Opts)
get_stream_info(StoreOpts, ID) when is_map(StoreOpts), is_binary(ID) ->
    get_stream_info(ID, StoreOpts);
get_stream_info(ID, Opts) ->
    case read_headers(ID, Opts) of
        {ok, #{data_offset := DataOffset, data_size := DataSize, content_type := MaybeCT}} ->
            % If data_size is missing, compute from HEAD content_length - data_offset
            Bucket = get_bucket(Opts),
            Key = <<ID/binary, ".ans104">>,
            S3Config = hb_s3_config:load_s3_device_config(),
            FinalSize =
                case DataSize of
                    undefined ->
                        case hb_s3_erlcloud:head_object(Bucket, Key, S3Config) of
                            {ok, Head} ->
                                TotalBin = maps:get(<<"content_length">>, Head, <<"0">>),
                                Total = case TotalBin of
                                    B when is_binary(B) -> try binary_to_integer(B) catch _:_ -> 0 end;
                                    L when is_list(L) -> try list_to_integer(L) catch _:_ -> 0 end;
                                    I when is_integer(I) -> I;
                                    _ -> 0
                                end,
                                erlang:max(Total - DataOffset, 0);
                            _ -> undefined
                        end;
                    I when is_integer(I) -> I
                end,
            % Derive content-type from header+tags if not present
            ContentType =
                case MaybeCT of
                    undefined ->
                        HeaderEnd = DataOffset - 1,
                        Range = <<"bytes=0-", (integer_to_binary(HeaderEnd))/binary>>,
                        case hb_s3_erlcloud:get_object(Bucket, Key, #{range => Range}, S3Config) of
                            {ok, #{<<"body">> := Partial}} ->
                                case safe_deserialize(Partial) of
                                    {ok, TX} -> find_content_type(TX#tx.tags);
                                    _ -> <<"application/octet-stream">>
                                end;
                            _ -> <<"application/octet-stream">>
                        end;
                    CT -> CT
                end,
            {ok, #{
                content_type => ContentType,
                total_size => FinalSize,
                data_offset => DataOffset
            }};
        Error ->
            Error
    end.

%% @private Find Content-Type from ANS-104 tags
find_content_type(Tags) ->
    case lists:keyfind(<<"Content-Type">>, 1, Tags) of
        {_, V} when is_binary(V) -> V;
        _ ->
            case lists:keyfind(<<"content-type">>, 1, Tags) of
                {_, V2} when is_binary(V2) -> V2;
                _ -> <<"application/octet-stream">>
            end
    end.

%% @private Safely deserialize a potentially partial ANS-104 (header+tags only)
safe_deserialize(Bin) when is_binary(Bin) ->
    try
        TX = ar_bundles:deserialize(Bin),
        case is_record(TX, tx) of
            true -> {ok, TX};
            false -> {error, invalid}
        end
    catch
        _:_ -> {error, invalid}
    end.

%% Helper functions from hb_gateway_s3.erl

get_bucket(_Opts) ->
    S3Config = hb_s3_config:load_s3_device_config(),
    maps:get(bucket, S3Config, <<"hyperbeam-data">>).

byte_size_safe(Data) when is_binary(Data) -> byte_size(Data);
byte_size_safe(Data) when is_list(Data) -> length(Data);
byte_size_safe(_) -> unknown.

read_headers(ID, Opts) ->
    Bucket = get_bucket(Opts),
    Key = <<ID/binary, ".ans104">>,
    S3Config = hb_s3_config:load_s3_device_config(),

    case hb_s3_erlcloud:head_object(Bucket, Key, S3Config) of
        {ok, Headers} ->
            case parse_ans104_header_from_metadata(Headers) of
                {ok, HeaderInfo} ->
                    {ok, HeaderInfo};
                {error, Reason} ->
                    % Fallback to reading first chunk to parse header
                    fallback_header_read(Bucket, Key, S3Config, Opts)
            end;
        {error, #{<<"status">> := 404}} ->
            not_found;
        {error, Reason} ->
            {error, Reason}
    end.

parse_ans104_header_from_metadata(Headers) ->
    % Try to get header info from S3 object metadata
    case maps:get(<<"x-amz-meta-ans104-data-offset">>, Headers, undefined) of
        undefined ->
            {error, no_metadata};
        DataOffsetStr ->
            try
                DataOffset = binary_to_integer(DataOffsetStr),
                DataSize = case maps:get(<<"x-amz-meta-ans104-data-size">>, Headers, undefined) of
                    undefined -> undefined;
                    SizeStr -> binary_to_integer(SizeStr)
                end,
                ContentType = maps:get(<<"x-amz-meta-ans104-content-type">>, Headers, <<"application/octet-stream">>),
                {ok, #{
                    data_offset => DataOffset,
                    data_size => DataSize,
                    content_type => ContentType
                }}
            catch
                _:_ -> {error, invalid_metadata}
            end
    end.

fallback_header_read(Bucket, Key, S3Config, _Opts) ->
    % Read first 1KB to parse ANS-104 header
    Range = <<"bytes=0-1023">>,
    case hb_s3_erlcloud:get_object(Bucket, Key, #{range => Range}, S3Config) of
        {ok, #{<<"body">> := HeaderBytes}} ->
            parse_ans104_header(HeaderBytes);
        {error, Reason} ->
            {error, Reason}
    end.

parse_ans104_header(HeaderBytes) ->
    % Implement ANS-104 header parsing
    % This is a simplified version - full implementation would parse the actual ANS-104 format
    try
        % ANS-104 starts with data item count, then tags, etc.
        % For now, return a default structure
        {ok, #{
            data_offset => 100,  % Placeholder
            data_size => undefined,
            content_type => <<"application/octet-stream">>
        }}
    catch
        _:_ ->
            {error, invalid_header}
    end.

parse_stored_ans104(Partial, Opts) ->
    % Parse the ANS-104 structure from the partial data
    % This would contain the full ANS-104 parsing logic
    try
        % Simplified parsing - in reality would decode the ANS-104 format
        {ok, #{
            <<"Data">> => Partial,
            <<"Content-Type">> => <<"application/octet-stream">>
        }}
    catch
        Error:Reason ->
            ?event({parse_ans104_error, Error, Reason}),
            {error, {parse_error, Reason}}
    end.

read_data_field_only(ID, Opts) ->
    case read_headers(ID, Opts) of
        {ok, #{data_offset := DataOffset}} ->
            Bucket = get_bucket(Opts),
            Key = <<ID/binary, ".ans104">>,
            S3Config = hb_s3_config:load_s3_device_config(),

            Range = <<"bytes=", (integer_to_binary(DataOffset))/binary, "-">>,
            case hb_s3_erlcloud:get_object(Bucket, Key, #{range => Range}, S3Config) of
                {ok, #{<<"body">> := Data}} ->
                    {ok, Data};
                {error, Reason} ->
                    {error, Reason}
            end;
        Error ->
            Error
    end.

convert_range_to_s3(Range, DataOffset) ->
    % Convert logical data range to physical S3 range accounting for ANS-104 header
    case Range of
        <<"bytes=", Rest/binary>> ->
            <<"bytes=", (add_offset_to_range(Rest, DataOffset))/binary>>;
        _ ->
            Range
    end.

add_offset_to_range(RangeStr, Offset) ->
    % Parse range and add offset - simplified implementation
    case binary:split(RangeStr, <<"-">>) of
        [Start, End] ->
            StartInt = binary_to_integer(Start) + Offset,
            EndInt = case End of
                <<>> -> <<"">>;
                _ -> integer_to_binary(binary_to_integer(End) + Offset)
            end,
            <<(integer_to_binary(StartInt))/binary, "-", EndInt/binary>>;
        [Start] ->
            StartInt = binary_to_integer(Start) + Offset,
            <<(integer_to_binary(StartInt))/binary, "-">>
    end.

stream_chunks(ID, Offset, TotalSize, ChunkFun, Opts) when Offset < TotalSize ->
    ChunkSize = min(1048576, TotalSize - Offset),  % 1MB chunks
    Range = <<"bytes=", (integer_to_binary(Offset))/binary, "-",
              (integer_to_binary(Offset + ChunkSize - 1))/binary>>,

    case data_with_range(ID, Range, Opts) of
        {ok, ChunkData} ->
            case ChunkFun(ChunkData) of
                ok ->
                    stream_chunks(ID, Offset + ChunkSize, TotalSize, ChunkFun, Opts);
                stop ->
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end;
stream_chunks(_, _, _, _, _) ->
    ok.
