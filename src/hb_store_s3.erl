%%% @doc An S3-compatible storage backend implementing the `hb_store' behavior.
%%% Minimal Phase 1 implementation: simple read/write/list/type operations,
%%% no streaming/range support, and conservative reset semantics.
-module(hb_store_s3).
-behavior(hb_store).

-export([start/1, stop/1, reset/1]).
-export([scope/0, scope/1]).
-export([read/2, write/3, list/2, type/2]).
-export([make_group/2, make_link/3, resolve/2]).

-include("include/hb.hrl").
-include_lib("erlcloud/include/erlcloud_aws.hrl").

%% =====================
%% Public API
%% =====================

%% @doc Initialize the S3 store by validating configuration.
start(Opts) ->
    case validate_config(Opts) of
        ok -> ok;
        {error, Reason} -> {error, Reason}
    end.

%% @doc Stop the S3 store. No-op.
stop(_Opts) -> ok.

%% @doc The S3 store is remote by default.
scope() -> remote.
scope(_Opts) -> scope().

%% @doc Reset the store by deleting all objects under the configured `prefix`.
%% For safety, requires a non-empty `prefix` and `<<"dangerous_reset">> => true`.
reset(Opts) ->
    maybe
        true ?= has_nonempty_prefix(Opts) orelse {error, no_prefix},
        true ?= maps:get(<<"dangerous_reset">>, Opts, false) orelse {error, not_confirmed},
        ok ?= delete_all_with_prefix(Opts),
        ok
    else
        _ -> not_found
    end.

%% @doc Read a key from S3, following links if necessary. Returns `{ok, Binary}` or `not_found`.
read(Opts, Key) when is_list(Key) ->
    read(Opts, hb_store:join(Key));
read(Opts, Key) ->
    io:format("S3DBG read_attempt key=~p~n", [Key]),
    ?event(store_s3, {read_attempt, {key, Key}}),
    read_with_links(Opts, Key, 0).

%% @doc Internal read that handles both direct links and path segment resolution
read_with_links(_Opts, _Key, Depth) when Depth > 1000 ->
    ?event(error, {too_many_link_redirects, {depth, Depth}}),
    not_found;
read_with_links(Opts, Key, Depth) ->
    io:format("S3DBG read_with_links key=~p depth=~p~n", [Key, Depth]),
    ?event(store_s3, {read_with_links, {key, Key}, {depth, Depth}}),
    try
        PathParts = binary:split(Key, <<"/">>, [global]),
        case length(PathParts) > 1 of
            true ->
                % Multi-segment path: resolve via link-following first
                io:format("S3DBG resolve_path parts=~p~n", [PathParts]),
                ?event(store_s3, {resolve_path, {parts, PathParts}}),
                case resolve_path_links(Opts, PathParts) of
                    {ok, ResolvedPathParts} ->
                        ResolvedPath = hb_store:join(ResolvedPathParts),
                        io:format("S3DBG resolved_path from=~p to=~p~n", [Key, ResolvedPath]),
                        ?event(store_s3, {resolved_path, {from, Key}, {to, ResolvedPath}}),
                        case ResolvedPath =:= Key of
                            true ->
                                % Path resolved to itself, attempt direct read
                                io:format("S3DBG resolved_to_self key=~p, trying direct read~n", [Key]),
                                case read_direct(Opts, Key) of
                                    {ok, Body} ->
                                        case is_link(Body) of
                                            {true, Target} ->
                                                io:format("S3DBG follow_link from=~p to=~p~n", [Key, Target]),
                                                read_with_links(Opts, Target, Depth + 1);
                                            false ->
                                                io:format("S3DBG read_hit key=~p size=~p~n", [Key, byte_size(Body)]),
                                                {ok, Body}
                                        end;
                                    _ -> not_found
                                end;
                            false ->
                                % Path changed, continue resolution
                                read_with_links(Opts, ResolvedPath, Depth + 1)
                        end;
                    {error, _} ->
                        io:format("S3DBG resolve_failed key=~p~n", [Key]),
                        ?event(store_s3, {resolve_failed, {key, Key}}),
                        not_found
                end;
            false ->
                % Single segment: attempt direct read
                case read_direct(Opts, Key) of
                    {ok, Body} ->
                        case is_link(Body) of
                            {true, Target} ->
                                io:format("S3DBG follow_link from=~p to=~p~n", [Key, Target]),
                                read_with_links(Opts, Target, Depth + 1);
                            false ->
                                io:format("S3DBG read_hit key=~p size=~p~n", [Key, byte_size(Body)]),
                                {ok, Body}
                        end;
                    _ -> not_found
                end
        end
    catch
        _:_ ->
            io:format("S3DBG resolve_exception key=~p~n", [Key]),
            not_found
    end.

%% @doc Check only for links (used during path segment resolution)
%% Does NOT attempt to read the object itself, only checks lnk/Key
check_link_only(Opts, Key) ->
    Bucket = maps:get(<<"bucket">>, Opts),
    LinkFullKey = apply_prefix(Opts, link_path(Key)),
    Config = make_erlcloud_config(Opts),
    io:format("S3DBG check_link_only key=~p link_key=~p~n", [Key, LinkFullKey]),
    case s3_get_object(Bucket, LinkFullKey, Config) of
        {ok, Body} ->
            io:format("S3DBG check_link_hit key=~p size=~p~n", [LinkFullKey, byte_size(Body)]),
            {ok, Body};
        _ ->
            io:format("S3DBG check_link_miss key=~p~n", [Key]),
            not_found
    end.

%% @doc Direct read without path resolution
read_direct(Opts, Key) ->
    Bucket = maps:get(<<"bucket">>, Opts),
    FullKey = apply_prefix(Opts, Key),
    Config = make_erlcloud_config(Opts),
    % Skip link check for data blobs (content-addressed, never have links)
    case binary:match(Key, <<"data/">>) of
        {0, _} ->
            % Key starts with "data/", read directly without checking for links
            io:format("S3DBG read_direct_data key=~p~n", [FullKey]),
            ?event(store_s3, {read_direct_data, {key, FullKey}}),
            case s3_get_object(Bucket, FullKey, Config) of
                {ok, Body} ->
                    io:format("S3DBG read_direct_hit key=~p size=~p~n", [FullKey, byte_size(Body)]),
                    ?event(store_s3, {read_direct_hit, {key, FullKey}, {size, byte_size(Body)}}),
                    {ok, Body};
                not_found ->
                    io:format("S3DBG read_direct_not_found key=~p~n", [FullKey]),
                    ?event(store_s3, {read_direct_not_found, {key, FullKey}}),
                    not_found;
                {error, Reason} ->
                    io:format("S3DBG read_direct_error key=~p reason=~p~n", [FullKey, Reason]),
                    ?event(store_s3, {read_direct_error, {key, FullKey}, {reason, Reason}}),
                    not_found
            end;
        _ ->
            % Not a data blob, check for links first
            LinkFullKey = apply_prefix(Opts, link_path(Key)),
            io:format("S3DBG read_direct bucket=~p key=~p link_key=~p~n", [Bucket, FullKey, LinkFullKey]),
            ?event(store_s3, {read_direct, {bucket, Bucket}, {key, FullKey}}),
            case s3_get_object(Bucket, LinkFullKey, Config) of
                {ok, BodyL} ->
                    io:format("S3DBG read_direct_hit LINK key=~p size=~p~n", [LinkFullKey, byte_size(BodyL)]),
                    {ok, BodyL};
                _ ->
                    case s3_get_object(Bucket, FullKey, Config) of
                        {ok, Body} ->
                            io:format("S3DBG read_direct_hit key=~p size=~p~n", [FullKey, byte_size(Body)]),
                            ?event(store_s3, {read_direct_hit, {key, FullKey}, {size, byte_size(Body)}}),
                            {ok, Body};
                        not_found ->
                            io:format("S3DBG read_direct_not_found key=~p~n", [FullKey]),
                            ?event(store_s3, {read_direct_not_found, {key, FullKey}}),
                            not_found;
                        {error, Reason} ->
                            io:format("S3DBG read_direct_error key=~p reason=~p~n", [FullKey, Reason]),
                            ?event(store_s3, {read_direct_error, {key, FullKey}, {reason, Reason}}),
                            not_found
                    end
            end
    end.

%% @doc Resolve path segment by segment, following links
resolve_path_links(Opts, Path) ->
    resolve_path_links(Opts, Path, 0).

resolve_path_links(_Opts, _Path, Depth) when Depth > 1000 ->
    {error, too_many_redirects};
resolve_path_links(_Opts, [LastSegment], _Depth) ->
    {ok, [LastSegment]};
resolve_path_links(Opts, Path, Depth) ->
    resolve_path_links_acc(Opts, Path, [], Depth).

resolve_path_links_acc(_Opts, [], AccPath, _Depth) ->
    {ok, lists:reverse(AccPath)};
resolve_path_links_acc(_, FullPath = [<<"data">>|_], [], _Depth) ->
    {ok, FullPath};
resolve_path_links_acc(Opts, [Head | Tail], AccPath, Depth) ->
    CurrentPath = lists:reverse([Head | AccPath]),
    CurrentPathBin = hb_store:join(CurrentPath),
    io:format("S3DBG resolve_segment current=~p remaining=~p~n", [CurrentPathBin, Tail]),
    ?event(store_s3, {resolve_segment, {current, CurrentPathBin}, {remaining, Tail}}),
    % For intermediate segments, only check for links (don't read the object itself)
    case check_link_only(Opts, CurrentPathBin) of
        {ok, Value} ->
            case is_link(Value) of
                {true, Link} ->
                    io:format("S3DBG segment_link from=~p to=~p~n", [CurrentPathBin, Link]),
                    ?event(store_s3, {segment_link, {from, CurrentPathBin}, {to, Link}}),
                    LinkSegments = binary:split(Link, <<"/">>, [global]),
                    NewPath = LinkSegments ++ Tail,
                    resolve_path_links(Opts, NewPath, Depth + 1);
                false ->
                    io:format("S3DBG segment_no_link current=~p~n", [CurrentPathBin]),
                    ?event(store_s3, {segment_no_link, {current, CurrentPathBin}}),
                    resolve_path_links_acc(Opts, Tail, [Head | AccPath], Depth)
            end;
        not_found ->
            io:format("S3DBG segment_not_found current=~p~n", [CurrentPathBin]),
            ?event(store_s3, {segment_not_found, {current, CurrentPathBin}}),
            % If the current path is a group and the next segment may be a
            % key-level link (e.g., lnk/<P>/<key>), attempt that directly.
            case Tail of
                [Next | Rest] ->
                    NextBin = hb_store:join(Next),
                    NextPathBin = <<CurrentPathBin/binary, "/", NextBin/binary>>,
                    io:format("S3DBG try_key_level_link path=~p~n", [NextPathBin]),
                    case check_link_only(Opts, NextPathBin) of
                        {ok, V2} ->
                            case is_link(V2) of
                                {true, L2} ->
                                    io:format("S3DBG key_level_link from=~p to=~p~n", [NextPathBin, L2]),
                                    Segs2 = binary:split(L2, <<"/">>, [global]),
                                    resolve_path_links(Opts, Segs2 ++ Rest, Depth + 1);
                                false ->
                                    resolve_path_links_acc(Opts, Tail, [Head | AccPath], Depth)
                            end;
                        _ ->
                            resolve_path_links_acc(Opts, Tail, [Head | AccPath], Depth)
                    end;
                [] ->
                    resolve_path_links_acc(Opts, Tail, [Head | AccPath], Depth)
            end
    end.

%% @doc Check if a value is a link and extract the target.
is_link(Value) ->
    LinkPrefixSize = byte_size(<<"link:">>),
    case byte_size(Value) > LinkPrefixSize andalso
        binary:part(Value, 0, LinkPrefixSize) =:= <<"link:">> of
        true ->
            Target = binary:part(Value, LinkPrefixSize, byte_size(Value) - LinkPrefixSize),
            {true, Target};
        false ->
            false
    end.

%% @doc Write a key to S3. Returns `ok` or `not_found` on errors to allow chaining.
write(Opts, Key, Value) ->
    Bucket = maps:get(<<"bucket">>, Opts),
    FullKey = apply_prefix(Opts, Key),
    Config = make_erlcloud_config(Opts),
    Result = s3_put_object(Bucket, FullKey, Value, Config),
    case Result of
        ok -> ok;
        {error, Reason} ->
            ?event(error, {s3_write_failed, {bucket, Bucket}, {key, FullKey}, {reason, Reason}}),
            not_found
    end.

%% @doc Determine if a key is `simple' (object), `composite' (has children), or `not_found`.
%% Follows links transparently.
type(Opts, Key) ->
    Bucket = maps:get(<<"bucket">>, Opts),
    FullKey = apply_prefix(Opts, Key),
    LinkFullKey = apply_prefix(Opts, link_path(Key)),
    Config = make_erlcloud_config(Opts),
    io:format("S3DBG type_check key=~p~n", [FullKey]),
    ?event(store_s3, {type_check, {key, FullKey}}),
    case s3_get_object(Bucket, LinkFullKey, Config) of
        {ok, LBody} ->
            case is_link(LBody) of
                {true, Target} ->
                    io:format("S3DBG type_link from=~p to=~p~n", [FullKey, Target]),
                    ?event(store_s3, {type_link, {from, FullKey}, {to, Target}}),
                    type(Opts, Target);
                false ->
                    case s3_get_object(Bucket, FullKey, Config) of
                        {ok, _} -> simple;
                        not_found ->
                            case s3_has_children(Bucket, FullKey, Config) of
                                true -> composite;
                                false -> not_found
                            end;
                        {error, _} -> not_found
                    end
            end;
        _ ->
            % No link found. Only try direct read for data blobs.
            % Transaction IDs are never stored directly, only their fields at lnk/<id>/*
            case binary:match(Key, <<"data/">>) of
                {0, _} ->
                    % Key starts with "data/", try direct read
                    case s3_get_object(Bucket, FullKey, Config) of
                        {ok, Body} ->
                            case is_link(Body) of
                                {true, Target} ->
                                    io:format("S3DBG type_link from=~p to=~p~n", [FullKey, Target]),
                                    ?event(store_s3, {type_link, {from, FullKey}, {to, Target}}),
                                    type(Opts, Target);
                                false ->
                                    io:format("S3DBG type_simple key=~p~n", [FullKey]),
                                    ?event(store_s3, {type_simple, {key, FullKey}}),
                                    simple
                            end;
                        not_found ->
                            io:format("S3DBG type_not_found key=~p~n", [FullKey]),
                            ?event(store_s3, {type_not_found, {key, FullKey}}),
                            not_found;
                        {error, _} ->
                            io:format("S3DBG type_error key=~p~n", [FullKey]),
                            ?event(store_s3, {type_error, {key, FullKey}}),
                            not_found
                    end;
                _ ->
                    % Not a data blob, skip direct read and check for children
                    io:format("S3DBG type_check_children key=~p~n", [FullKey]),
                    ?event(store_s3, {type_check_children, {key, FullKey}}),
                    case s3_has_children(Bucket, FullKey, Config) of
                        true ->
                            io:format("S3DBG type_composite key=~p~n", [FullKey]),
                            ?event(store_s3, {type_composite, {key, FullKey}}),
                            composite;
                        false ->
                            io:format("S3DBG type_not_found key=~p~n", [FullKey]),
                            ?event(store_s3, {type_not_found, {key, FullKey}}),
                            not_found
                    end
            end
    end.

%% @doc List immediate children under the given key (treating key as a group).
%% Follows links transparently.
list(Opts, Key) ->
    Bucket = maps:get(<<"bucket">>, Opts),
    FullKey = apply_prefix(Opts, Key),
    LinkFullKey = apply_prefix(Opts, link_path(Key)),
    Config = make_erlcloud_config(Opts),
    % Check if Key is a link and resolve it if necessary
    io:format("S3DBG list_request key=~p~n", [FullKey]),
    ?event(store_s3, {list_request, {key, FullKey}}),
    ResolvedKey = case s3_get_object(Bucket, LinkFullKey, Config) of
        {ok, LBody} ->
            case is_link(LBody) of
                {true, Target} ->
                    io:format("S3DBG list_follow_link from=~p to=~p~n", [FullKey, Target]),
                    ?event(store_s3, {list_follow_link, {from, FullKey}, {to, Target}}),
                    apply_prefix(Opts, Target);
                false -> FullKey
            end;
        _ -> case s3_get_object(Bucket, FullKey, Config) of
        {ok, Body} ->
            case is_link(Body) of
                {true, Target} ->
                    io:format("S3DBG list_follow_link from=~p to=~p~n", [FullKey, Target]),
                    ?event(store_s3, {list_follow_link, {from, FullKey}, {to, Target}}),
                    apply_prefix(Opts, Target);
                false -> FullKey
            end;
        _ -> FullKey end
    end,
    GroupPrefix = ensure_trailing_slash(ResolvedKey),
    io:format("S3DBG list_s3 prefix=~p~n", [GroupPrefix]),
    ?event(store_s3, {list_s3, {prefix, GroupPrefix}}),
    case erlcloud_s3:list_objects(binary_to_list(Bucket), [{prefix, binary_to_list(GroupPrefix)}, {delimiter, "/"}], Config) of
        Resp when is_list(Resp) ->
            io:format("S3DBG list_raw resp=~p\n", [Resp]),
            Children = format_list_children(GroupPrefix, Resp),
            io:format("S3DBG list_result count=~p\n", [length(Children)]),
            ?event(store_s3, {list_result, {count, length(Children)}}),
            {ok, Children};
        {error, {aws_error, {http_error, 404, _, _}}} -> not_found;
        {error, Reason} ->
            ?event(error, {s3_list_failed, {bucket, Bucket}, {prefix, GroupPrefix}, {reason, Reason}}),
            not_found
    end.

%% @doc Create a group marker so we can detect composite types.
%% Store a special ".group" marker to indicate this is a directory/composite.
make_group(_Opts, _Path) ->
    % Avoid writing an object at the group path to prevent conflicts with
    % children under the same prefix. Groups are detected via listing.
    ok.

%% @doc Create a symbolic link by storing a "link:" prefixed reference.
%% This matches the LMDB implementation for compatibility.
make_link(Opts, Existing, New) ->
    ExistingBin = hb_util:bin(Existing),
    LinkValue = <<"link:", ExistingBin/binary>>,
    write(Opts, link_path(New), LinkValue).

%% @doc No link resolution for S3 in Phase 1; passthrough.
resolve(Opts, Path) when is_list(Path) ->
    try
        Parts = lists:map(fun hb_util:bin/1, Path),
        case resolve_path_links(Opts, Parts) of
            {ok, ResolvedParts} ->
                Resolved = hb_store:join(ResolvedParts),
                io:format("S3DBG resolve list from=~p to=~p~n", [Path, Resolved]),
                Resolved;
            {error, _} ->
                hb_store:join(Path)
        end
    catch _:_ -> hb_store:join(Path) end;
resolve(Opts, Path) ->
    try
        Bin = hb_store:join(Path),
        Parts = binary:split(Bin, <<"/">>, [global]),
        case resolve_path_links(Opts, Parts) of
            {ok, ResolvedParts} ->
                Resolved = hb_store:join(ResolvedParts),
                io:format("S3DBG resolve bin from=~p to=~p~n", [Bin, Resolved]),
                Resolved;
            {error, _} -> Bin
        end
    catch _:_ -> hb_store:join(Path) end.

%% Attempt a fast path for ID/data: follow ID link to P, then read P/file-hash
%% to compute data/<sha>. If any step fails, return not_found.
quick_data_path(Opts, ID) ->
    Bucket = maps:get(<<"bucket">>, Opts),
    Config = make_erlcloud_config(Opts),
    case s3_get_object(Bucket, apply_prefix(Opts, ID), Config) of
        {ok, Body} ->
            case is_link(Body) of
                {true, P} ->
                    FHKey = <<P/binary, "/file-hash">>,
                    case s3_get_object(Bucket, apply_prefix(Opts, FHKey), Config) of
                        {ok, FH} ->
                            Hash = hb_util:bin(FH),
                            {ok, <<"data/", Hash/binary>>};
                        _ -> not_found
                    end;
                false -> not_found
            end;
        _ -> not_found
    end.

%% =====================
%% Internal helpers
%% =====================

validate_config(Opts) ->
    Required = [<<"bucket">>, <<"access-key-id">>, <<"secret-access-key">>],
    case lists:all(fun(K) -> maps:is_key(K, Opts) andalso hb_util:bin(maps:get(K, Opts)) =/= <<>> end, Required) of
        true -> ok;
        false -> {error, missing_required_keys}
    end.

has_nonempty_prefix(Opts) ->
    case maps:get(<<"prefix">>, Opts, <<>>) of
        <<>> -> false;
        P when is_binary(P) -> true;
        _ -> false
    end.

make_erlcloud_config(Opts) ->
    Endpoint = maps:get(<<"endpoint">>, Opts, <<"https://s3.amazonaws.com">>),
    ForcePathStyle = maps:get(<<"force_path_style">>, Opts, false),
    BucketAccessMethod = case ForcePathStyle of
        true -> path;
        <<"true">> -> path;
        _ -> vhost
    end,
    AccessKey = maps:get(<<"access-key-id">>, Opts),
    SecretKey = maps:get(<<"secret-access-key">>, Opts),
    Region = maps:get(<<"region">>, Opts, <<"us-east-1">>),
    {Scheme, Host, Port} = parse_endpoint(Endpoint),
    Base = erlcloud_s3:new(
        binary_to_list(AccessKey),
        binary_to_list(SecretKey),
        Host,
        Port
    ),
    RegionStr = case Region of B when is_binary(B) -> binary_to_list(B); L when is_list(L) -> L; _ -> "us-east-1" end,
    Base#aws_config{
        s3_scheme = Scheme,
        s3_bucket_after_host = false,  % Changed from true for path-style
        s3_bucket_access_method = BucketAccessMethod,
        aws_region = RegionStr,
        http_client = httpc
    }.

parse_endpoint(Endpoint) ->
    EndpointStr = binary_to_list(Endpoint),
    case string:split(EndpointStr, "://", leading) of
        [Scheme, HostPort] ->
            case string:split(HostPort, ":", trailing) of
                [Host, PortStr] ->
                    {Scheme ++ "://", Host, to_int(PortStr)};
                [Host] ->
                    DefaultPort = case Scheme of "https" -> 443; _ -> 80 end,
                    {Scheme ++ "://", Host, DefaultPort}
            end;
        [HostOnly] -> {"http://", HostOnly, 80}
    end.

to_int(Str) ->
    try list_to_integer(Str) catch _:_ -> 80 end.

apply_prefix(Opts, Key) ->
    Prefix = maps:get(<<"prefix">>, Opts, <<>>),
    Path = hb_store:join(Key),
    case Prefix of
        <<>> -> Path;
        _ -> hb_store:join([Prefix, Path])
    end.

ensure_trailing_slash(Bin) ->
    case Bin of
        <<>> -> <<>>;
        _ ->
            case binary:last(Bin) of
                $/ -> Bin;
                _ -> <<Bin/binary, "/">>
            end
    end.

delete_all_with_prefix(Opts) ->
    Bucket = maps:get(<<"bucket">>, Opts),
    Prefix = ensure_trailing_slash(maps:get(<<"prefix">>, Opts, <<>>)),
    Config = make_erlcloud_config(Opts),
    case erlcloud_s3:list_objects(binary_to_list(Bucket), [{prefix, binary_to_list(Prefix)}], Config) of
        Resp when is_list(Resp) ->
            Keys = [ list_to_binary(proplists:get_value(key, Obj, "")) || Obj <- proplists:get_value(contents, Resp, []) ],
            case Keys of
                [] -> ok;
                _ ->
                    case erlcloud_s3:delete_objects(binary_to_list(Bucket), [ binary_to_list(K) || K <- Keys ], Config) of
                        _ -> ok
                    end
            end;
        {error, _} -> not_found
    end.

%% Map erlcloud get to {ok, Body} | not_found | {error, Reason}
s3_get_object(Bucket, Key, Config) ->
    try erlcloud_s3:get_object(binary_to_list(Bucket), binary_to_list(Key), [], Config) of
        Resp when is_list(Resp) ->
            Body = hb_util:bin(proplists:get_value(content, Resp, <<>>)),
            io:format("S3DBG s3_get_ok key=~p size=~p~n", [Key, byte_size(Body)]),
            ?event(store_s3, {s3_get_ok, {key, Key}, {size, byte_size(Body)}}),
            {ok, Body};
        {error, {aws_error, {http_error, 404, _, _}}} ->
            io:format("S3DBG s3_get_not_found key=~p~n", [Key]),
            ?event(store_s3, {s3_get_not_found, {key, Key}}),
            not_found;
        {error, Reason} ->
            io:format("S3DBG s3_get_error key=~p reason=~p~n", [Key, Reason]),
            ?event(store_s3, {s3_get_error, {key, Key}, {reason, Reason}}),
            {error, Reason}
    catch
        Class:Reason:Stack ->
            io:format("S3DBG s3_get_exception key=~p class=~p reason=~p stack=~p~n", [Key, Class, Reason, Stack]),
            ?event(store_s3, {s3_get_exception, {key, Key}, {class, Class}, {reason, Reason}}),
            not_found
    end.

%% Map erlcloud put to ok | {error, Reason}
s3_put_object(Bucket, Key, Body, Config) ->
    BucketStr = binary_to_list(Bucket),
    KeyStr = binary_to_list(Key),
    io:format("S3DBG s3_put key=~p size=~p~n", [Key, byte_size(Body)]),
    ?event(store_s3, {s3_put, {key, Key}, {size, byte_size(Body)}}),
    try erlcloud_s3:put_object(BucketStr, KeyStr, Body, [], Config) of
        Resp when is_list(Resp) ->
            io:format("S3DBG s3_put_ok key=~p~n", [Key]),
            ?event(store_s3, {s3_put_ok, {key, Key}}),
            ok;
        {error, Reason} ->
            io:format("S3DBG s3_put_error key=~p reason=~p~n", [Key, Reason]),
            ?event(store_s3, {s3_put_error, {key, Key}, {reason, Reason}}),
            {error, Reason}
    catch
        Class:Reason:_Stack ->
            io:format("S3DBG s3_put_exception key=~p class=~p reason=~p~n", [Key, Class, Reason]),
            ?event(store_s3, {s3_put_exception, {key, Key}, {class, Class}, {reason, Reason}}),
            {error, {exception, Class, Reason}}
    end.

%% Return simple | error
s3_head_type(Bucket, Key, Config) ->
    case erlcloud_s3:get_object_metadata(binary_to_list(Bucket), binary_to_list(Key), [], Config) of
        Resp when is_list(Resp) -> simple;
        {error, _Reason} -> {error, not_found}
    end.

%% Check if there are any children under Key/
s3_has_children(Bucket, Key, Config) ->
    Prefix = ensure_trailing_slash(Key),
    io:format("S3DBG has_children prefix=~p~n", [Prefix]),
    ?event(store_s3, {has_children, {prefix, Prefix}}),
    case erlcloud_s3:list_objects(binary_to_list(Bucket), [{prefix, binary_to_list(Prefix)}, {delimiter, "/"}], Config) of
        Resp when is_list(Resp) ->
            Contents = proplists:get_value(contents, Resp, []),
            Common = proplists:get_value(common_prefixes, Resp, []),
            io:format("S3DBG has_children_result contents=~p dirs=~p~n", [length(Contents), length(Common)]),
            ?event(store_s3, {has_children_result, {contents, length(Contents)}, {dirs, length(Common)}}),
            (length(Contents) > 0) orelse (length(Common) > 0);
        {error, _} -> false
    end.

format_list_children(GroupPrefix, Resp) ->
    Contents = proplists:get_value(contents, Resp, []),
    Common = proplists:get_value(common_prefixes, Resp, []),
    Files =
        lists:foldl(
            fun(Obj, Acc) ->
                Key = list_to_binary(proplists:get_value(key, Obj, "")),
                case strip_prefix(GroupPrefix, Key) of
                    <<>> -> Acc;
                    Remainder ->
                        case binary:split(Remainder, <<"/">>, [global]) of
                            [Name] -> [Name | Acc];
                            _ -> Acc
                        end
                end
            end,
            [],
            Contents
        ),
    Dirs =
        lists:foldl(
            fun(P, Acc) ->
                Prefix = list_to_binary(proplists:get_value(prefix, P, "")),
                case strip_prefix(GroupPrefix, Prefix) of
                    <<>> -> Acc;
                    Remainder ->
                        Name =
                            case binary:split(Remainder, <<"/">>, [global]) of
                                [DirName, <<>>] -> DirName;
                                [DirName] -> DirName;
                                [DirName | _] -> DirName
                            end,
                        [Name | Acc]
                end
            end,
            [],
            Common
        ),
    lists:usort([ hb_util:bin(N) || N <- (Files ++ Dirs) ]).

%% Fallback child extraction: derive immediate child names from a full listing
%% (no delimiter) by stripping the group prefix and taking the first path
%% segment. Returns de-duplicated binary names.
%% No fallback listing logic; rely on delimiter listings only.

strip_prefix(Prefix, Bin) ->
    PL = byte_size(Prefix),
    case Bin of
        <<Prefix:PL/binary, Rest/binary>> -> Rest;
        _ -> Bin
    end.

%% Build link-object path under dedicated namespace to avoid conflicts with
%% group/object prefixes (e.g., lnk/<key>). Accepts binaries or path lists.
link_path(Key) ->
    <<"lnk/", (hb_store:join(Key))/binary>>.
