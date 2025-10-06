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

%% @doc Read a key from S3. Returns `{ok, Binary}` or `not_found`.
read(Opts, Key) ->
    Bucket = maps:get(<<"bucket">>, Opts),
    FullKey = apply_prefix(Opts, Key),
    Config = make_erlcloud_config(Opts),
    maybe
        {ok, Body} ?= s3_get_object(Bucket, FullKey, Config),
        {ok, Body}
    else
        not_found -> not_found;
        {error, Reason} ->
            ?event(error, {s3_read_failed, {bucket, Bucket}, {key, FullKey}, {reason, Reason}}),
            not_found
    end.

%% @doc Write a key to S3. Returns `ok` or `not_found` on errors to allow chaining.
write(Opts, Key, Value) ->
    Bucket = maps:get(<<"bucket">>, Opts),
    FullKey = apply_prefix(Opts, Key),
    Config = make_erlcloud_config(Opts),
    maybe
        ok ?= s3_put_object(Bucket, FullKey, Value, Config),
        ok
    else
        {error, Reason} ->
            ?event(error, {s3_write_failed, {bucket, Bucket}, {key, FullKey}, {reason, Reason}}),
            not_found
    end.

%% @doc Determine if a key is `simple' (object), `composite' (has children), or `not_found`.
type(Opts, Key) ->
    Bucket = maps:get(<<"bucket">>, Opts),
    FullKey = apply_prefix(Opts, Key),
    Config = make_erlcloud_config(Opts),
    maybe
        simple ?= s3_head_type(Bucket, FullKey, Config) orelse {error, maybe_list},
        simple
    else
        {error, maybe_list} ->
            case s3_has_children(Bucket, FullKey, Config) of
                true -> composite;
                false -> not_found
            end;
        _ -> not_found
    end.

%% @doc List immediate children under the given key (treating key as a group).
list(Opts, Key) ->
    Bucket = maps:get(<<"bucket">>, Opts),
    GroupPrefix = ensure_trailing_slash(apply_prefix(Opts, Key)),
    Config = make_erlcloud_config(Opts),
    case erlcloud_s3:list_objects(binary_to_list(Bucket), [{prefix, binary_to_list(GroupPrefix)}, {delimiter, "/"}], Config) of
        Resp when is_list(Resp) ->
            {ok, format_list_children(GroupPrefix, Resp)};
        {error, {aws_error, {http_error, 404, _, _}}} -> not_found;
        {error, Reason} ->
            ?event(error, {s3_list_failed, {bucket, Bucket}, {prefix, GroupPrefix}, {reason, Reason}}),
            not_found
    end.

%% @doc S3 has no directories. We accept and return ok for compatibility.
make_group(_Opts, _Path) -> ok.

%% @doc S3 has no symlinks. Not supported in Phase 1.
make_link(_Opts, _Existing, _New) -> not_found.

%% @doc No link resolution for S3 in Phase 1; passthrough.
resolve(_Opts, Path) -> hb_store:join(Path).

%% =====================
%% Internal helpers
%% =====================

validate_config(Opts) ->
    Required = [<<"bucket">>, <<"priv_access_key_id">>, <<"priv_secret_access_key">>],
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
    BucketAccessMethod = case maps:get(<<"force_path_style">>, Opts, false) of true -> path; _ -> vhost end,
    AccessKey = maps:get(<<"priv_access_key_id">>, Opts),
    SecretKey = maps:get(<<"priv_secret_access_key">>, Opts),
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
        s3_bucket_after_host = true,
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
    case erlcloud_s3:get_object(binary_to_list(Bucket), binary_to_list(Key), [], Config) of
        Resp when is_list(Resp) ->
            {ok, hb_util:bin(proplists:get_value(content, Resp, <<>>))};
        {error, {aws_error, {http_error, 404, _, _}}} -> not_found;
        {error, Reason} -> {error, Reason}
    end.

%% Map erlcloud put to ok | {error, Reason}
s3_put_object(Bucket, Key, Body, Config) ->
    case erlcloud_s3:put_object(binary_to_list(Bucket), binary_to_list(Key), Body, [], Config) of
        Resp when is_list(Resp) -> ok;
        {error, Reason} -> {error, Reason}
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
    case erlcloud_s3:list_objects(binary_to_list(Bucket), [{prefix, binary_to_list(Prefix)}, {delimiter, "/"}], Config) of
        Resp when is_list(Resp) ->
            Contents = proplists:get_value(contents, Resp, []),
            Common = proplists:get_value(common_prefixes, Resp, []),
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

strip_prefix(Prefix, Bin) ->
    PL = byte_size(Prefix),
    case Bin of
        <<Prefix:PL/binary, Rest/binary>> -> Rest;
        _ -> Bin
    end.

