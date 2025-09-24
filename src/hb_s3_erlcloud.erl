%%% @doc erlcloud-based S3 backend implementation
%%% This module implements the hb_s3_backend behavior using erlcloud library
%%% to provide memory-efficient S3 operations with streaming support for large files.
-module(hb_s3_erlcloud).
-behaviour(hb_s3_backend).

-export([
    get_object/4,
    put_object/5,
    head_object/3,
    list_objects/4,
    delete_object/3,
    delete_objects/3,
    stream_object/5
]).

%% Internal exports for testing
-export([
    create_erlcloud_config/1,
    map_erlcloud_error/1,
    should_use_multipart/1,
    ensure_httpc_started/0
]).

-include_lib("erlcloud/include/erlcloud_aws.hrl").

%% Constants for multipart upload and streaming
-define(MULTIPART_THRESHOLD, 104857600). % 100MB
-define(STREAMING_THRESHOLD, 10485760).  % 10MB
-define(DEFAULT_CHUNK_SIZE, 5242880).    % 5MB
-define(MIN_PART_SIZE, 5242880).         % 5MB minimum per AWS

%%% @doc Get an object from S3
-spec get_object(binary(), binary(), hb_s3_backend:s3_options(), hb_s3_backend:s3_config()) ->
    {ok, hb_s3_backend:s3_response()} | {error, hb_s3_backend:error_response()}.
get_object(Bucket, Key, Options, Config) ->
    % Ensure lhttpc is started
    ensure_httpc_started(),
    try
        case should_stream(Bucket, Key, Options, Config) of
            {true, _Size} ->
                io:format("S3 DEBUG: Using streaming path for ~p/~p~n", [Bucket, Key]),
                Ref = make_ref(),
                put({s3_stream_acc, Ref}, []),
                AccumCb = fun(Chunk, _Offset, _Total) ->
                    Chunks = case get({s3_stream_acc, Ref}) of undefined -> []; L -> L end,
                    put({s3_stream_acc, Ref}, [Chunk | Chunks]),
                    continue
                end,
                case stream_object(Bucket, Key, Options, Config, AccumCb) of
                    {ok, _Complete} ->
                        Collected = lists:reverse(case get({s3_stream_acc, Ref}) of undefined -> []; L -> L end),
                        erase({s3_stream_acc, Ref}),
                        Body = iolist_to_binary(Collected),
                        Meta = case head_object(Bucket, Key, Config) of
                            {ok, M} -> M;
                            _ -> #{}
                        end,
                        % Merge metadata first, then override with actual body/length
                        {ok, maps:merge(
                            Meta,
                            #{
                                <<"status">> => 200,
                                <<"body">> => Body,
                                <<"content_length">> => list_to_binary(integer_to_list(byte_size(Body)))
                            }
                        )};
                    {error, Reason} -> {error, Reason}
                end;
            false ->
                ErlcloudConfig = create_erlcloud_config(Config),
                ErlcloudOptions = build_get_options(Options),

                BucketStr = binary_to_list(Bucket),
                KeyStr = binary_to_list(Key),
                io:format("erlcloud adapter: Calling erlcloud_s3:get_object(~p, ~p, Options: ~p)~n",
                         [BucketStr, KeyStr, ErlcloudOptions]),
                
                case erlcloud_s3:get_object(
                    BucketStr,
                    KeyStr,
                    ErlcloudOptions,
                    ErlcloudConfig
                ) of
                    Response when is_list(Response) ->
                        {ok, format_get_response(Response)};
                    {error, Reason} ->
                        {error, map_erlcloud_error(Reason)}
                end
        end
    catch
        _Error:_Reason:_Stacktrace ->
            io:format("erlcloud get_object error: ~p:~p~n", [_Error, _Reason]),
            io:format("Stacktrace: ~p~n", [_Stacktrace]),
            {error, #{<<"status">> => 500, <<"body">> => <<"Internal server error">>}}
    end.

%%% @doc Put an object to S3 with automatic multipart upload for large files
-spec put_object(binary(), binary(), binary(), hb_s3_backend:s3_options(), hb_s3_backend:s3_config()) ->
    {ok, hb_s3_backend:s3_response()} | {error, hb_s3_backend:error_response()}.
put_object(Bucket, Key, Body, Options, Config) ->
    try
        ErlcloudConfig = create_erlcloud_config(Config),
        
        case should_use_multipart(Body) of
            true ->
                multipart_upload(Bucket, Key, Body, Options, ErlcloudConfig);
            false ->
                simple_upload(Bucket, Key, Body, Options, ErlcloudConfig)
        end
    catch
        _Error:_Reason ->
            io:format("erlcloud put_object error: ~p:~p~n", [_Error, _Reason]),
            {error, #{<<"status">> => 500, <<"body">> => <<"Internal server error">>}}
    end.

%%% @doc Get object metadata without downloading the body
-spec head_object(binary(), binary(), hb_s3_backend:s3_config()) ->
    {ok, hb_s3_backend:s3_response()} | {error, hb_s3_backend:error_response()}.
head_object(Bucket, Key, Config) ->
    % Ensure lhttpc is started
    ensure_httpc_started(),
    try
        ErlcloudConfig = create_erlcloud_config(Config),
        
        case erlcloud_s3:get_object_metadata(
            binary_to_list(Bucket),
            binary_to_list(Key),
            [],
            ErlcloudConfig
        ) of
            Response when is_list(Response) ->
                {ok, format_head_response(Response)};
            {error, Reason} ->
                {error, map_erlcloud_error(Reason)}
        end
    catch
        _Error:_Reason ->
            io:format("erlcloud head_object error: ~p:~p~n", [_Error, _Reason]),
            {error, #{<<"status">> => 500, <<"body">> => <<"Internal server error">>}}
    end.

%%% @doc List objects in a bucket with optional prefix
-spec list_objects(binary(), binary(), hb_s3_backend:s3_options(), hb_s3_backend:s3_config()) ->
    {ok, hb_s3_backend:s3_response()} | {error, hb_s3_backend:error_response()}.
list_objects(Bucket, Prefix, Options, Config) ->
    try
        ErlcloudConfig = create_erlcloud_config(Config),
        ErlcloudOptions = build_list_options(Prefix, Options),
        
        case erlcloud_s3:list_objects(
            binary_to_list(Bucket),
            ErlcloudOptions,
            ErlcloudConfig
        ) of
            Response when is_list(Response) ->
                {ok, format_list_response(Response)};
            {error, Reason} ->
                {error, map_erlcloud_error(Reason)}
        end
    catch
        _Error:_Reason ->
            io:format("erlcloud list_objects error: ~p:~p~n", [_Error, _Reason]),
            {error, #{<<"status">> => 500, <<"body">> => <<"Internal server error">>}}
    end.

%%% @doc Delete a single object from S3
-spec delete_object(binary(), binary(), hb_s3_backend:s3_config()) ->
    {ok, hb_s3_backend:s3_response()} | {error, hb_s3_backend:error_response()}.
delete_object(Bucket, Key, Config) ->
    try
        ErlcloudConfig = create_erlcloud_config(Config),
        
        case erlcloud_s3:delete_object(
            binary_to_list(Bucket),
            binary_to_list(Key),
            ErlcloudConfig
        ) of
            ok ->
                {ok, #{<<"status">> => 200, <<"body">> => <<>>}};
            {error, Reason} ->
                {error, map_erlcloud_error(Reason)}
        end
    catch
        _Error:_Reason ->
            io:format("erlcloud delete_object error: ~p:~p~n", [_Error, _Reason]),
            {error, #{<<"status">> => 500, <<"body">> => <<"Internal server error">>}}
    end.

%%% @doc Delete multiple objects from S3
-spec delete_objects(binary(), [binary()], hb_s3_backend:s3_config()) ->
    {ok, hb_s3_backend:s3_response()} | {error, hb_s3_backend:error_response()}.
delete_objects(Bucket, Keys, Config) ->
    try
        ErlcloudConfig = create_erlcloud_config(Config),
        KeyList = [binary_to_list(Key) || Key <- Keys],
        
        case erlcloud_s3:delete_objects(
            binary_to_list(Bucket),
            KeyList,
            ErlcloudConfig
        ) of
            Response when is_list(Response) ->
                {ok, format_delete_response(Response)};
            {error, Reason} ->
                {error, map_erlcloud_error(Reason)}
        end
    catch
        _Error:_Reason ->
            io:format("erlcloud delete_objects error: ~p:~p~n", [_Error, _Reason]),
            {error, #{<<"status">> => 500, <<"body">> => <<"Internal server error">>}}
    end.

%%% Internal Functions

%%% @doc Create erlcloud AWS configuration from HyperBeam S3 config
-spec create_erlcloud_config(hb_s3_backend:s3_config()) -> #aws_config{}.
create_erlcloud_config(S3Config) ->
    Endpoint = maps:get(endpoint, S3Config),
    AccessKeyId = maps:get(access_key_id, S3Config),
    SecretAccessKey = maps:get(secret_access_key, S3Config),
    
    % Handle custom endpoints (e.g., MinIO)
    {Scheme, Host, Port} = parse_endpoint(Endpoint),
    
    % Create config using erlcloud_s3:new/4 and set scheme separately
    Config = erlcloud_s3:new(
        binary_to_list(AccessKeyId),
        binary_to_list(SecretAccessKey),
        Host,
        Port
    ),
    
    % Set the scheme and configure for path-style access (required for MinIO)
    Config#aws_config{
        s3_scheme = Scheme,
        s3_bucket_after_host = true,        % Use path-style: host:port/bucket/key
        s3_bucket_access_method = path,      % Force path-style instead of vhost
        http_client = httpc                 % Use built-in httpc instead of lhttpc
    }.

%%% @doc Parse endpoint URL into scheme, host, and port
parse_endpoint(Endpoint) ->
    EndpointStr = binary_to_list(Endpoint),
    case string:split(EndpointStr, "://", leading) of
        [Scheme, HostPort] ->
            case string:split(HostPort, ":", trailing) of
                [Host, PortStr] ->
                    Port = try list_to_integer(PortStr) catch _:_ -> 80 end,
                    {Scheme ++ "://", Host, Port};
                [Host] ->
                    DefaultPort = case Scheme of
                        "https" -> 443;
                        _ -> 80
                    end,
                    {Scheme ++ "://", Host, DefaultPort}
            end;
        [HostPort] ->
            % No scheme provided, assume http
            {"http://", HostPort, 80}
    end.

%%% @doc Determine if file should use multipart upload
-spec should_use_multipart(binary()) -> boolean().
should_use_multipart(Body) when byte_size(Body) > ?MULTIPART_THRESHOLD ->
    true;
should_use_multipart(_Body) ->
    false.

%%% @doc Simple upload for smaller files
simple_upload(Bucket, Key, Body, Options, Config) ->
    ErlcloudOptions = build_put_options(Options),
    
    case erlcloud_s3:put_object(
        binary_to_list(Bucket),
        binary_to_list(Key),
        Body,
        ErlcloudOptions,
        Config
    ) of
        Response when is_list(Response) ->
            {ok, format_put_response(Response)};
        {error, Reason} ->
            {error, map_erlcloud_error(Reason)}
    end.

%%% @doc Multipart upload for large files
multipart_upload(Bucket, Key, Body, _Options, Config) ->
    BucketStr = binary_to_list(Bucket),
    KeyStr = binary_to_list(Key),
    
    % Start multipart upload
    case erlcloud_s3:start_multipart(BucketStr, KeyStr, [], [], Config) of
        {ok, Response} ->
            UploadId = proplists:get_value(upload_id, Response),
            
            try
                % Upload parts in chunks
                ETags = upload_parts_chunked(BucketStr, KeyStr, UploadId, Body, Config),
                
                % Complete multipart upload
                case erlcloud_s3:complete_multipart(BucketStr, KeyStr, UploadId, ETags, [], Config) of
                    ok ->
                        {ok, #{
                            <<"status">> => 200,
                            <<"body">> => <<"">>,
                            <<"upload_id">> => list_to_binary(UploadId)
                        }};
                    {error, Reason} ->
                        {error, map_erlcloud_error(Reason)}
                end
            catch
                _Error:_Reason ->
                    % Cleanup: abort multipart upload on failure
                    erlcloud_s3:abort_multipart(BucketStr, KeyStr, UploadId, [], Config),
                    io:format("Multipart upload failed: ~p:~p~n", [_Error, _Reason]),
                    {error, #{<<"status">> => 500, <<"body">> => <<"Multipart upload failed">>}}
            end;
        {error, Reason} ->
            {error, map_erlcloud_error(Reason)}
    end.

%%% @doc Upload file data in chunks using multipart upload
upload_parts_chunked(Bucket, Key, UploadId, Data, Config) ->
    upload_parts_chunked(Bucket, Key, UploadId, Data, ?DEFAULT_CHUNK_SIZE, 1, [], Config).

upload_parts_chunked(_Bucket, _Key, _UploadId, <<>>, _ChunkSize, _PartNum, ETags, _Config) ->
    lists:reverse(ETags);
upload_parts_chunked(Bucket, Key, UploadId, Data, ChunkSize, PartNum, ETags, Config) ->
    {Chunk, Remaining} = extract_chunk(Data, ChunkSize),
    
    % Upload this part
    case erlcloud_s3:upload_part(Bucket, Key, UploadId, PartNum, Chunk, [], Config) of
        {ok, Response} ->
            ETag = proplists:get_value(etag, Response),
            
            % Continue with remaining data
            upload_parts_chunked(Bucket, Key, UploadId, Remaining, ChunkSize, PartNum + 1, 
                               [{PartNum, ETag} | ETags], Config);
        {error, Reason} ->
            throw({upload_part_failed, Reason})
    end.

%%% @doc Extract a chunk of data from binary
extract_chunk(Data, ChunkSize) when byte_size(Data) > ChunkSize ->
    <<Chunk:ChunkSize/binary, Remaining/binary>> = Data,
    {Chunk, Remaining};
extract_chunk(Data, _ChunkSize) ->
    {Data, <<>>}.

%%% @doc Build options for get_object call
build_get_options(Options) ->
    BaseOpts = [],
    case maps:get(range, Options, undefined) of
        undefined -> BaseOpts;
        <<>> -> BaseOpts;
        Range when is_binary(Range) -> [{range, binary_to_list(Range)} | BaseOpts];
        Range when is_list(Range) -> [{range, Range} | BaseOpts];
        _ -> BaseOpts
    end.

%%% @doc Build options for put_object call
build_put_options(Options) ->
    BaseOpts = [],
    % Add metadata if provided
    case maps:get(metadata, Options, undefined) of
        undefined -> BaseOpts;
        Metadata when is_map(Metadata) ->
            MetadataList = maps:to_list(Metadata),
            [{meta, MetadataList} | BaseOpts];
        _ -> BaseOpts
    end.

%%% @doc Build options for list_objects call
build_list_options(Prefix, Options) ->
    BaseOpts = [{prefix, binary_to_list(Prefix)}],
    % Add delimiter if provided
    case maps:get(delimiter, Options, undefined) of
        undefined -> BaseOpts;
        Delimiter -> [{delimiter, binary_to_list(Delimiter)} | BaseOpts]
    end.

%%% @doc Format get_object response to standard format
format_get_response(Response) ->
    ContentRange = case proplists:get_value(content_range, Response, undefined) of
        undefined -> proplists:get_value('content-range', Response, undefined);
        V -> V
    end,
    Status = case ContentRange of undefined -> 200; _ -> 206 end,
    Base = #{
        <<"status">> => Status,
        <<"body">> => proplists:get_value(content, Response, <<>>),
        <<"etag">> => list_to_binary(proplists:get_value(etag, Response, "")),
        <<"last_modified">> => list_to_binary(proplists:get_value(last_modified, Response, "")),
        <<"content_type">> => list_to_binary(proplists:get_value(content_type, Response, "binary/octet-stream")),
        <<"content_length">> => case proplists:get_value(content_length, Response, "0") of
            Len when is_integer(Len) -> list_to_binary(integer_to_list(Len));
            Len when is_list(Len) -> list_to_binary(Len);
            Len when is_binary(Len) -> Len
        end,
        <<"accept_ranges">> => <<"bytes">>
    },
    case ContentRange of
        undefined -> Base;
        CR -> Base#{ <<"content-range">> => list_to_binary(CR) }
    end.

%%% @doc Format head_object response to standard format
format_head_response(Response) ->
    #{
        <<"status">> => 200,
        <<"body">> => <<>>,
        <<"etag">> => list_to_binary(proplists:get_value(etag, Response, "")),
        <<"last_modified">> => list_to_binary(proplists:get_value(last_modified, Response, "")),
        <<"content_type">> => list_to_binary(proplists:get_value(content_type, Response, "binary/octet-stream")),
        <<"content_length">> => case proplists:get_value(content_length, Response, "0") of
            Len when is_integer(Len) -> list_to_binary(integer_to_list(Len));
            Len when is_list(Len) -> list_to_binary(Len);
            Len when is_binary(Len) -> Len
        end
    }.

%%% @doc Format list_objects response to standard format
format_list_response(Response) ->
    Contents = proplists:get_value(contents, Response, []),
    ObjectCount = length(Contents),
    
    % Convert contents to the expected format
    FormattedContents = lists:foldl(fun(Object, {Index, Acc}) ->
        Key = proplists:get_value(key, Object, ""),
        Size = proplists:get_value(size, Object, 0),
        ETag = proplists:get_value(etag, Object, ""),
        LastModified = proplists:get_value(last_modified, Object, ""),
        
        NewAcc = maps:merge(Acc, #{
            list_to_binary(io_lib:format("object_~p_key", [Index])) => list_to_binary(Key),
            list_to_binary(io_lib:format("object_~p_size", [Index])) => list_to_binary(integer_to_list(Size)),
            list_to_binary(io_lib:format("object_~p_etag", [Index])) => list_to_binary(ETag),
            list_to_binary(io_lib:format("object_~p_last_modified", [Index])) => list_to_binary(LastModified)
        }),
        
        {Index + 1, NewAcc}
    end, {0, #{}}, Contents),
    
    {_FinalIndex, ContentMap} = FormattedContents,
    
    maps:merge(#{
        <<"status">> => 200,
        <<"object_count">> => list_to_binary(integer_to_list(ObjectCount)),
        <<"is_truncated">> => list_to_binary(atom_to_list(proplists:get_value(is_truncated, Response, false))),
        <<"marker">> => list_to_binary(proplists:get_value(marker, Response, "")),
        <<"next_marker">> => list_to_binary(proplists:get_value(next_marker, Response, ""))
    }, ContentMap).

%%% @doc Format put_object response to standard format
format_put_response(Response) ->
    #{
        <<"status">> => 200,
        <<"body">> => <<>>,
        <<"etag">> => list_to_binary(proplists:get_value(etag, Response, "")),
        <<"version_id">> => list_to_binary(proplists:get_value(version_id, Response, ""))
    }.

%%% @doc Format delete_objects response to standard format
format_delete_response(_Response) ->
    #{
        <<"status">> => 200,
        <<"body">> => <<"Objects deleted successfully">>
    }.

%%% @doc Map erlcloud errors to standard HTTP error responses
-spec map_erlcloud_error(term()) -> hb_s3_backend:error_response().
map_erlcloud_error({aws_error, {http_error, 404, _, _}}) ->
    #{<<"status">> => 404, <<"body">> => <<"Not Found">>};
map_erlcloud_error({aws_error, {http_error, 403, _, _}}) ->
    #{<<"status">> => 403, <<"body">> => <<"Forbidden">>};
map_erlcloud_error({aws_error, {http_error, 400, _, Body}}) ->
    #{<<"status">> => 400, <<"body">> => hb_util:bin(Body)};
map_erlcloud_error({aws_error, {http_error, Status, _, Body}}) ->
    #{<<"status">> => Status, <<"body">> => hb_util:bin(Body)};
map_erlcloud_error({aws_error, {socket_error, Reason}}) ->
    #{<<"status">> => 503, <<"body">> => list_to_binary(io_lib:format("Connection error: ~p", [Reason]))};
map_erlcloud_error(no_such_key) ->
    #{<<"status">> => 404, <<"body">> => <<"NoSuchKey">>};
map_erlcloud_error(Error) ->
    #{<<"status">> => 500, <<"body">> => list_to_binary(io_lib:format("~p", [Error]))}.

%%% @doc Check if we should stream based on file size
-spec should_stream(binary(), binary(), hb_s3_backend:s3_options(), hb_s3_backend:s3_config()) ->
    {true, non_neg_integer()} | false.
should_stream(Bucket, Key, Options, Config) ->
    case maps:get(range, Options, undefined) of
        undefined ->
            Threshold = hb_opts:get(s3_streaming_threshold, ?STREAMING_THRESHOLD, #{}),
            case head_object(Bucket, Key, Config) of
                {ok, #{ <<"content_length">> := SizeBin }} ->
                    Size = case SizeBin of
                        Bin when is_binary(Bin) ->
                            try binary_to_integer(Bin) catch _:_ -> 0 end;
                        List when is_list(List) ->
                            try list_to_integer(List) catch _:_ -> 0 end;
                        Int when is_integer(Int) -> Int;
                        _ -> 0
                    end,
                    case Size > Threshold of
                        true -> {true, Size};
                        false -> false
                    end;
                _ -> false
            end;
        RangeValue ->
            io:format("S3 DEBUG: Skipping streaming due to explicit Range option: ~p~n", [RangeValue]),
            false
    end.

%%% @doc Stream object in chunks for memory efficiency
-spec stream_object(binary(), binary(), hb_s3_backend:s3_options(), hb_s3_backend:s3_config(), 
                    fun((binary(), integer(), integer()) -> continue | {stop, term()})) ->
    {ok, term()} | {error, hb_s3_backend:error_response()}.
stream_object(Bucket, Key, Options, Config, CallbackFun) ->
    ErlcloudConfig = create_erlcloud_config(Config),
    
    % First get object size
    case head_object(Bucket, Key, Config) of
        {ok, #{<<"content_length">> := SizeBin}} ->
            Size = binary_to_integer(SizeBin),
            io:format("erlcloud: Starting streaming for ~p bytes~n", [Size]),
            stream_chunks(Bucket, Key, Size, 0, CallbackFun, ErlcloudConfig, Options);
        Error -> 
            io:format("erlcloud: Failed to get object size: ~p~n", [Error]),
            Error
    end.

%%% @doc Stream chunks of the object
stream_chunks(Bucket, Key, TotalSize, Offset, CallbackFun, Config, Options) 
    when Offset < TotalSize ->
    ChunkSize = erlang:min(?DEFAULT_CHUNK_SIZE, TotalSize - Offset),
    EndByte = Offset + ChunkSize - 1,
    Range = io_lib:format("bytes=~B-~B", [Offset, EndByte]),
    
    io:format("erlcloud: Fetching chunk ~p-~p of ~p~n", [Offset, EndByte, TotalSize]),
    
    case erlcloud_s3:get_object(
        binary_to_list(Bucket),
        binary_to_list(Key),
        [{range, Range}],
        Config
    ) of
        Response when is_list(Response) ->
            Content = proplists:get_value(content, Response),
            case CallbackFun(Content, Offset, TotalSize) of
                continue ->
                    stream_chunks(Bucket, Key, TotalSize, 
                                  Offset + ChunkSize, CallbackFun, Config, Options);
                {stop, Result} ->
                    {ok, Result}
            end;
        {error, Reason} ->
            {error, map_erlcloud_error(Reason)}
    end;
stream_chunks(_, _, _, _, _, _, _) ->
    {ok, complete}.

%% @doc Ensure the HTTP client is started
ensure_httpc_started() ->
    % Start dependencies first
    application:start(crypto),
    application:start(ssl),
    case application:start(inets) of
        ok -> ok;
        {error, {already_started, _}} -> ok;
        {error, Reason} ->
            io:format("Warning: Could not start inets: ~p~n", [Reason]),
            ok
    end.
