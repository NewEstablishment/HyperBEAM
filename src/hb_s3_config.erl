%%% @doc S3 configuration management
%%% Handles configuration for S3 backend selection and settings
-module(hb_s3_config).

-export([
    get_backend/0,
    get_backend/1,
    get_multipart_threshold/0,
    get_chunk_size/0,
    load_s3_device_config/0,
    get_config_value/2,
    get_config_value/3
]).

-compile(export_all).

%%% @doc Get the configured S3 backend (always erlcloud now)
-spec get_backend() -> erlcloud.
get_backend() ->
    erlcloud.

-spec get_backend(list()) -> erlcloud.
get_backend(_Opts) ->
    erlcloud.

%%% @doc Get multipart upload threshold
-spec get_multipart_threshold() -> pos_integer().
get_multipart_threshold() ->
    DeviceConfig = load_s3_device_config(),
    case maps:get(s3_multipart_threshold, DeviceConfig, undefined) of
        undefined ->
            case application:get_env(hb, s3_multipart_threshold) of
                {ok, Threshold} when is_integer(Threshold), Threshold > 0 ->
                    Threshold;
                _ ->
                    104857600 % 100MB default
            end;
        Threshold when is_integer(Threshold), Threshold > 0 ->
            Threshold;
        _ ->
            104857600 % 100MB default
    end.

%%% @doc Get chunk size for multipart uploads
-spec get_chunk_size() -> pos_integer().
get_chunk_size() ->
    DeviceConfig = load_s3_device_config(),
    case maps:get(s3_chunk_size, DeviceConfig, undefined) of
        undefined ->
            case application:get_env(hb, s3_chunk_size) of
                {ok, ChunkSize} when is_integer(ChunkSize), ChunkSize >= 5242880 ->
                    ChunkSize;
                _ ->
                    5242880 % 5MB default (minimum for AWS multipart)
            end;
        ChunkSize when is_integer(ChunkSize), ChunkSize >= 5242880 ->
            ChunkSize;
        _ ->
            5242880 % 5MB default
    end.

%%% @doc Load S3 device configuration from file
-spec load_s3_device_config() -> map().
load_s3_device_config() ->
    case file:consult("s3_module.config") of
        {ok, Terms} -> maps:from_list(Terms);
        {error, enoent} ->
            io:format("Warning: s3_module.config not found, using defaults~n"),
            #{};
        {error, Reason} ->
            io:format("Warning: Could not load s3_module.config: ~p~n", [Reason]),
            #{}
    end.

%%% @doc Get a configuration value with fallback hierarchy
-spec get_config_value(atom(), map()) -> term().
get_config_value(Key, DeviceConfig) ->
    get_config_value(Key, DeviceConfig, undefined).

-spec get_config_value(atom(), map(), term()) -> term().
get_config_value(Key, DeviceConfig, Default) ->
    % Priority: 1. Device config, 2. App env, 3. Default
    case maps:get(Key, DeviceConfig, undefined) of
        undefined ->
            case application:get_env(hb, Key) of
                {ok, Value} -> Value;
                undefined -> Default
            end;
        Value ->
            Value
    end.
