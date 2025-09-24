%%% @doc Behavior for S3 backend implementations
%%% This behavior defines the interface for S3 operations, allowing
%%% different implementations (NIF, erlcloud, etc.) to be used interchangeably.
-module(hb_s3_backend).

-type s3_config() :: #{
    endpoint => binary(),
    access_key_id => binary(),
    secret_access_key => binary(),
    region => binary()
}.

-type s3_options() :: #{
    range => binary(),
    headers => map(),
    metadata => map()
}.

-type s3_response() :: #{
    status => integer(),
    body => binary(),
    etag => binary(),
    last_modified => binary(),
    content_type => binary(),
    content_length => binary()
}.

-type error_response() :: #{
    status => integer(),
    body => binary()
}.

%% Callback definitions
-callback get_object(
    Bucket :: binary(),
    Key :: binary(),
    Options :: s3_options(),
    Config :: s3_config()
) -> 
    {ok, s3_response()} | {error, error_response()}.

-callback put_object(
    Bucket :: binary(),
    Key :: binary(),
    Body :: binary(),
    Options :: s3_options(),
    Config :: s3_config()
) ->
    {ok, s3_response()} | {error, error_response()}.

-callback head_object(
    Bucket :: binary(),
    Key :: binary(),
    Config :: s3_config()
) ->
    {ok, s3_response()} | {error, error_response()}.

-callback list_objects(
    Bucket :: binary(),
    Prefix :: binary(),
    Options :: s3_options(),
    Config :: s3_config()
) ->
    {ok, s3_response()} | {error, error_response()}.

-callback delete_object(
    Bucket :: binary(),
    Key :: binary(),
    Config :: s3_config()
) ->
    {ok, s3_response()} | {error, error_response()}.

-callback delete_objects(
    Bucket :: binary(),
    Keys :: [binary()],
    Config :: s3_config()
) ->
    {ok, s3_response()} | {error, error_response()}.

%% Export types for use by implementations
-export_type([s3_config/0, s3_options/0, s3_response/0, error_response/0]).