-module(test_s3_http_integration).
-export([test_s3_routing/0, test_s3_url_parsing/0, test_complete_integration/0]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

%% @doc Test that S3 URLs are properly routed to the S3 adapter
test_s3_routing() ->
    % Test that S3 URLs are recognized
    S3URL = <<"s3://test-bucket/prefix">>,
    ?assert(binary:match(S3URL, <<"s3://">>) /= nomatch),

    % Test URL parsing
    <<"s3://", Rest/binary>> = S3URL,
    {Bucket, Prefix} = parse_s3_url_simple(Rest),
    ?assertEqual(<<"test-bucket">>, Bucket),
    ?assertEqual(<<"prefix">>, Prefix),

    io:format("S3 URL routing test passed~n").

%% @doc Test S3 URL parsing functionality
test_s3_url_parsing() ->
    % Test bucket only
    {Bucket1, Prefix1} = parse_s3_url_simple(<<"hyperbeam-data">>),
    ?assertEqual(<<"hyperbeam-data">>, Bucket1),
    ?assertEqual(<<>>, Prefix1),

    % Test bucket with prefix
    {Bucket2, Prefix2} = parse_s3_url_simple(<<"hyperbeam-data/ans104">>),
    ?assertEqual(<<"hyperbeam-data">>, Bucket2),
    ?assertEqual(<<"ans104">>, Prefix2),

    % Test bucket with nested prefix
    {Bucket3, Prefix3} = parse_s3_url_simple(<<"hyperbeam-data/ans104/v1">>),
    ?assertEqual(<<"hyperbeam-data">>, Bucket3),
    ?assertEqual(<<"ans104/v1">>, Prefix3),

    io:format("S3 URL parsing test passed~n").

%% @doc Test the complete S3 integration
test_complete_integration() ->
    io:format("Testing S3 integration...~n"),

    % Test S3 options loading
    Opts = hb_opts:default_message_with_env(),
    S3Enabled = hb_opts:get(s3_enabled, false, Opts),
    S3Endpoint = hb_opts:get(s3_endpoint, undefined, Opts),
    S3Bucket = hb_opts:get(s3_bucket, undefined, Opts),

    io:format("S3 Enabled: ~p~n", [S3Enabled]),
    io:format("S3 Endpoint: ~p~n", [S3Endpoint]),
    io:format("S3 Bucket: ~p~n", [S3Bucket]),

    ?assertEqual(true, S3Enabled),
    % Environment variables come back as strings, so convert for comparison
    ExpectedEndpoint = case S3Endpoint of
        Bin when is_binary(Bin) -> Bin;
        Str when is_list(Str) -> list_to_binary(Str);
        _ -> S3Endpoint
    end,
    ExpectedBucket = case S3Bucket of
        Bin2 when is_binary(Bin2) -> Bin2;
        Str2 when is_list(Str2) -> list_to_binary(Str2);
        _ -> S3Bucket
    end,
    ?assertEqual(<<"http://not-tee.zephyrdev.xyz:9001">>, ExpectedEndpoint),
    ?assertEqual(<<"migrated">>, ExpectedBucket),

    % Test store configuration
    StoreOpts = hb_opts:get(store, [], Opts),
    S3Store = lists:keyfind(hb_store_s3, 1,
        lists:map(fun(Store) ->
            {maps:get(<<"store-module">>, Store, undefined), Store}
        end, StoreOpts)
    ),

    case S3Store of
        {hb_store_s3, S3Config} ->
            io:format("Found S3 store configuration: ~p~n", [S3Config]),
            ?assertEqual(true, maps:get(<<"enabled">>, S3Config)),
            ?assertEqual(<<"migrated">>, maps:get(<<"bucket">>, S3Config));
        false ->
            ?assert(false)  % S3 store not found
    end,

    % Test route configuration
    Routes = hb_opts:get(routes, [], Opts),
    RawRoute = lists:keyfind(<<"/raw">>, 2,
        lists:map(fun(Route) ->
            {Route, maps:get(<<"template">>, Route, undefined)}
        end, Routes)
    ),

    case RawRoute of
        {RouteConfig, <<"/raw">>} ->
            io:format("Found raw route configuration~n"),
            Nodes = maps:get(<<"nodes">>, RouteConfig, []),
            ?assert(length(Nodes) > 0),

            % Check if first node is S3
            [FirstNode|_] = Nodes,
            S3Prefix = maps:get(<<"prefix">>, FirstNode, <<>>),
            ?assert(binary:match(S3Prefix, <<"s3://">>) /= nomatch),
            io:format("S3 route found: ~p~n", [S3Prefix]);
        false ->
            ?assert(false)  % Raw route not found
    end,

    % Test store defaults
    StoreDefaults = hb_opts:get(store_defaults, #{}, Opts),
    S3Defaults = maps:get(<<"s3">>, StoreDefaults, undefined),
    ?assertNotEqual(undefined, S3Defaults),
    ?assertEqual(3600, maps:get(<<"cache_ttl">>, S3Defaults)),
    ?assertEqual(3, maps:get(<<"max_retries">>, S3Defaults)),

    io:format("S3 integration test passed~n").

%% Helper function for URL parsing (simplified version)
parse_s3_url_simple(URL) ->
    case binary:split(URL, <<"/">>, [global]) of
        [Bucket] ->
            {Bucket, <<>>};
        [Bucket | Rest] ->
            Prefix = iolist_to_binary(lists:join(<<"/">>, Rest)),
            {Bucket, Prefix}
    end.

%% EUnit test wrappers
s3_routing_test() ->
    test_s3_routing().

s3_url_parsing_test() ->
    test_s3_url_parsing().

complete_integration_test() ->
    test_complete_integration().