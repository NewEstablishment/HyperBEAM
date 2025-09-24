%%% @doc S3 protocol adapter for HyperBEAM HTTP interface.
%%% This module implements the hb_http interface for S3 storage, allowing
%%% transparent access to ANS-104 dataitems stored in S3 buckets with automatic
%%% fallback to HTTP gateways through the routing system.
-module(hb_http_s3).
-export([request/2, request/5]).
-include("include/hb.hrl").

%% @doc Main entry point matching hb_http interface
request(Req, Opts) when is_map(Req) ->
    Method = maps:get(<<"method">>, Req),
    URL = maps:get(<<"url">>, Req),
    Path = maps:get(<<"path">>, Req, <<>>),
    Body = maps:get(<<"body">>, Req, <<>>),
    Headers = maps:get(<<"headers">>, Req, #{}),
    request(Method, URL, Path, #{<<"body">> => Body, <<"headers">> => Headers}, Opts).

%% @doc Handle S3 requests with HTTP-like interface
request(Method, <<"s3://", Rest/binary>>, Path, Message, Opts) ->
    ?event({s3_http_request, {method, Method}, {url, <<"s3://", Rest/binary>>}, {path, Path}}),

    % Parse S3 URL: s3://bucket/optional-prefix
    {Bucket, Prefix} = parse_s3_url(Rest),

    % Extract ID from path (e.g., /raw/ID or /ID/data)
    ID = extract_id_from_path(Path),

    % Merge S3 configuration
    S3Opts = merge_s3_opts(Opts, Bucket, Prefix),

    case Method of
        <<"HEAD">> ->
            handle_head_request(ID, S3Opts);
        <<"GET">> ->
            handle_get_request(ID, Message, S3Opts);
        _ ->
            ?event({s3_http_error, {unsupported_method, Method}}),
            {error, #{<<"status">> => 405, <<"body">> => <<"Method not allowed">>}}
    end;
request(Method, URL, Path, Message, Opts) ->
    ?event({s3_http_error, {invalid_s3_url, URL}}),
    {error, #{<<"status">> => 400, <<"body">> => <<"Invalid S3 URL format">>}}.

%% @doc Handle HEAD requests for metadata
handle_head_request(ID, Opts) ->
    ?event({s3_http_head, {id, ID}}),
    case hb_store_s3:get_stream_info(ID, Opts) of
        {ok, #{total_size := Size, content_type := ContentType}} ->
            ?event({s3_http_head_success, {id, ID}, {size, Size}}),
            {ok, #{
                <<"status">> => 200,
                <<"headers">> => #{
                    <<"content-length">> => integer_to_binary(Size),
                    <<"content-type">> => ContentType,
                    <<"accept-ranges">> => <<"bytes">>,
                    <<"cache-control">> => <<"public, max-age=3600">>
                }
            }};
        not_found ->
            ?event({s3_http_head_not_found, {id, ID}}),
            {error, #{<<"status">> => 404, <<"body">> => <<"Not found">>}};
        {error, Reason} ->
            ?event({s3_http_head_error, {id, ID}, {reason, Reason}}),
            {error, #{<<"status">> => 500, <<"body">> => <<"Internal server error">>}}
    end.

%% @doc Handle GET requests with optional range support
handle_get_request(ID, Message, Opts) ->
    Headers = maps:get(<<"headers">>, Message, #{}),
    case maps:get(<<"range">>, Headers, undefined) of
        undefined ->
            % Full object request
            handle_full_get(ID, Opts);
        RangeHeader ->
            % Range request
            handle_range_get(ID, RangeHeader, Opts)
    end.

%% @doc Handle full object GET
handle_full_get(ID, Opts) ->
    ?event({s3_http_get_full, {id, ID}}),
    case hb_store_s3:data(ID, Opts) of
        {ok, Data} ->
            ContentType = hb_opts:get(range_default_content_type, <<"application/octet-stream">>, Opts),
            ?event({s3_http_get_success, {id, ID}, {size, byte_size(Data)}}),
            {ok, #{
                <<"status">> => 200,
                <<"body">> => Data,
                <<"headers">> => #{
                    <<"content-type">> => ContentType,
                    <<"content-length">> => integer_to_binary(byte_size(Data)),
                    <<"cache-control">> => <<"public, max-age=3600">>
                }
            }};
        not_found ->
            ?event({s3_http_get_not_found, {id, ID}}),
            {error, #{<<"status">> => 404, <<"body">> => <<"Not found">>}};
        {error, Reason} ->
            ?event({s3_http_get_error, {id, ID}, {reason, Reason}}),
            {error, #{<<"status">> => 500, <<"body">> => <<"Internal server error">>}}
    end.

%% @doc Handle range GET requests
handle_range_get(ID, RangeHeader, Opts) ->
    ?event({s3_http_get_range, {id, ID}, {range, RangeHeader}}),
    case hb_store_s3:get_stream_info(ID, Opts) of
        {ok, #{total_size := Total, content_type := ContentType}} ->
            case hb_http_range:parse(RangeHeader, Total) of
                {ok, {Start, End}} ->
                    Range = <<"bytes=", (integer_to_binary(Start))/binary, "-",
                              (integer_to_binary(End))/binary>>,
                    case hb_store_s3:data_with_range(ID, Range, Opts) of
                        {ok, Data} ->
                            ContentRange = <<"bytes ", (integer_to_binary(Start))/binary, "-",
                                           (integer_to_binary(End))/binary, "/",
                                           (integer_to_binary(Total))/binary>>,
                            ?event({s3_http_range_success, {id, ID}, {range, ContentRange}}),
                            {ok, #{
                                <<"status">> => 206,
                                <<"body">> => Data,
                                <<"headers">> => #{
                                    <<"content-range">> => ContentRange,
                                    <<"content-length">> => integer_to_binary(byte_size(Data)),
                                    <<"content-type">> => ContentType
                                }
                            }};
                        {error, Reason} ->
                            ?event({s3_http_range_error, {id, ID}, {reason, Reason}}),
                            {error, #{<<"status">> => 500, <<"body">> => <<"Range request failed">>}}
                    end;
                {error, _} ->
                    ?event({s3_http_range_invalid, {id, ID}, {range, RangeHeader}}),
                    {error, #{
                        <<"status">> => 416,
                        <<"body">> => <<"Range not satisfiable">>,
                        <<"headers">> => #{
                            <<"content-range">> => <<"bytes */", (integer_to_binary(Total))/binary>>
                        }
                    }}
            end;
        not_found ->
            ?event({s3_http_range_not_found, {id, ID}}),
            {error, #{<<"status">> => 404, <<"body">> => <<"Not found">>}};
        {error, Reason} ->
            ?event({s3_http_range_meta_error, {id, ID}, {reason, Reason}}),
            {error, #{<<"status">> => 500, <<"body">> => <<"Failed to get metadata">>}}
    end.

%% @doc Parse S3 URL into bucket and optional prefix
parse_s3_url(URL) ->
    case binary:split(URL, <<"/">>, [global]) of
        [Bucket] ->
            {Bucket, <<>>};
        [Bucket | Rest] ->
            Prefix = iolist_to_binary(lists:join(<<"/">>, Rest)),
            {Bucket, Prefix}
    end.

%% @doc Extract ID from various path formats
extract_id_from_path(<<"/raw/", ID/binary>>) ->
    % Remove any query parameters or additional path components
    case binary:split(ID, <<"?">>) of
        [CleanID, _] -> CleanID;
        [CleanID] -> CleanID
    end;
extract_id_from_path(<<"/", ID:43/binary, "/data", _/binary>>) -> ID;
extract_id_from_path(<<"/", ID:43/binary, _/binary>>) -> ID;
extract_id_from_path(ID) when byte_size(ID) >= 43 ->
    % Extract first 43 characters as ID
    <<ExtractedID:43/binary, _/binary>> = ID,
    ExtractedID;
extract_id_from_path(ID) -> ID.

%% @doc Merge S3-specific options with request options
merge_s3_opts(Opts, Bucket, Prefix) ->
    S3Opts = #{
        bucket => Bucket,
        prefix => Prefix,
        endpoint => hb_opts:get(s3_endpoint, <<"http://localhost:9000">>, Opts),
        region => hb_opts:get(s3_region, <<"us-east-1">>, Opts),
        access_key => hb_opts:get(s3_access_key, <<"minioadmin">>, Opts),
        secret_key => hb_opts:get(s3_secret_key, <<"minioadmin">>, Opts)
    },

    % Merge route-specific S3 options if present
    RouteOpts = maps:get(<<"opts">>, Opts, #{}),
    S3RouteOpts = maps:with([
        s3_endpoint, s3_region, s3_access_key, s3_secret_key,
        <<"s3_endpoint">>, <<"s3_region">>, <<"s3_access_key">>, <<"s3_secret_key">>
    ], RouteOpts),

    % Convert binary keys to atom keys for consistency
    NormalizedRouteOpts = maps:fold(
        fun(K, V, Acc) when is_binary(K) ->
            AtomKey = case K of
                <<"s3_endpoint">> -> s3_endpoint;
                <<"s3_region">> -> s3_region;
                <<"s3_access_key">> -> s3_access_key;
                <<"s3_secret_key">> -> s3_secret_key;
                _ -> K
            end,
            Acc#{AtomKey => V};
           (K, V, Acc) ->
            Acc#{K => V}
        end,
        #{},
        S3RouteOpts
    ),

    FinalS3Opts = maps:merge(S3Opts, NormalizedRouteOpts),
    maps:merge(Opts, FinalS3Opts).

%% @doc Check if S3 is enabled in the current configuration
is_s3_enabled(Opts) ->
    hb_opts:get(s3_enabled, true, Opts).

%% @doc Generate cache key for S3 responses
cache_key(ID, Bucket, Prefix) ->
    PrefixPart = case Prefix of
        <<>> -> <<"">>;
        _ -> <<"/", Prefix/binary>>
    end,
    <<"s3://", Bucket/binary, PrefixPart/binary, "/", ID/binary>>.

%% @doc Validate S3 configuration
validate_s3_config(Opts) ->
    RequiredKeys = [bucket, endpoint, region],
    Missing = lists:filter(
        fun(Key) ->
            not maps:is_key(Key, Opts) orelse maps:get(Key, Opts) == <<>>
        end,
        RequiredKeys
    ),
    case Missing of
        [] -> ok;
        _ -> {error, {missing_s3_config, Missing}}
    end.