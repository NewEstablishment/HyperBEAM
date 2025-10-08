%%% @doc Integration test for hb_store_s3: verifies that an ID/data path
%%% resolves to the underlying data/<sha> object via S3-only link resolution.
-module(s3_store_integration_test).
-include_lib("eunit/include/eunit.hrl").

s3_id_data_roundtrip_test_() ->
    {timeout, 30, fun s3_id_data_roundtrip/0}.

s3_id_data_roundtrip() ->
    S3 = setup_s3_store(),
    try
        %% Create the link structure:
        %% ID -> link:P
        %% P/file-hash -> <<sha>>
        %% P/data -> link:data/<sha>
        %% data/<sha> -> <<Content>>
        Content = <<"hello-from-eunit">>,
        DataHP = hb_path:hashpath(Content, #{}),
        ID = <<"test-id-", (rand_id())/binary>>,
        P = <<"test-uncommitted-", (rand_id())/binary>>,
        ok = hb_store:make_group(S3, P),
        ok = hb_store:write(S3, <<"data/", DataHP/binary>>, Content),
        ok = hb_store:write(S3, <<P/binary, "/file-hash">>, DataHP),
        ok = hb_store:write(S3, <<P/binary, "/data">>, <<"link:data/", DataHP/binary>>),
        ok = hb_store:make_link(S3, P, ID),
        %% Resolve ID/data via S3-only resolution and verify we get the original content back.
        ?assertEqual({ok, Content}, hb_store:read(S3, [ID, <<"data">>])),
        ok
    after
        cleanup_s3_store(S3)
    end.

s3_type_checking_test_() ->
    {timeout, 30, fun s3_type_checking/0}.

s3_type_checking() ->
    S3 = setup_s3_store(),
    try
        Content = <<"type-test-content">>,
        DataHP = hb_path:hashpath(Content, #{}),
        ID = <<"type-id-", (rand_id())/binary>>,
        P = <<"type-uncommitted-", (rand_id())/binary>>,

        %% Write a composite structure (P with multiple fields)
        ok = hb_store:make_group(S3, P),
        ok = hb_store:write(S3, <<"data/", DataHP/binary>>, Content),
        ok = hb_store:write(S3, <<P/binary, "/file-hash">>, DataHP),
        ok = hb_store:write(S3, <<P/binary, "/data">>, <<"link:data/", DataHP/binary>>),
        ok = hb_store:write(S3, <<P/binary, "/type">>, <<"file">>),
        ok = hb_store:make_link(S3, P, ID),

        %% Verify P is composite (has children)
        ?assertEqual(composite, hb_store:type(S3, P)),

        %% Verify data blob is simple
        ?assertEqual(simple, hb_store:type(S3, <<"data/", DataHP/binary>>)),

        %% Verify ID resolves to P's type (composite)
        ?assertEqual(composite, hb_store:type(S3, ID)),

        %% Verify non-existent key returns not_found
        ?assertEqual(not_found, hb_store:type(S3, <<"nonexistent-key-", (rand_id())/binary>>)),

        ok
    after
        cleanup_s3_store(S3)
    end.

s3_list_children_test_() ->
    {timeout, 30, fun s3_list_children/0}.

s3_list_children() ->
    S3 = setup_s3_store(),
    try
        Content = <<"list-test-content">>,
        DataHP = hb_path:hashpath(Content, #{}),
        P = <<"list-uncommitted-", (rand_id())/binary>>,

        %% Write multiple fields under P
        ok = hb_store:make_group(S3, P),
        ok = hb_store:write(S3, <<"data/", DataHP/binary>>, Content),
        ok = hb_store:write(S3, <<P/binary, "/file-hash">>, DataHP),
        ok = hb_store:write(S3, <<P/binary, "/data">>, <<"link:data/", DataHP/binary>>),
        ok = hb_store:write(S3, <<P/binary, "/type">>, <<"file">>),
        ok = hb_store:write(S3, <<P/binary, "/content-type">>, <<"application/octet-stream">>),

        %% List P's children
        {ok, Children} = hb_store:list(S3, P),

        %% Verify expected fields are present
        ?assert(lists:member(<<"file-hash">>, Children)),
        ?assert(lists:member(<<"data">>, Children)),
        ?assert(lists:member(<<"type">>, Children)),
        ?assert(lists:member(<<"content-type">>, Children)),
        ?assertEqual(4, length(Children)),

        ok
    after
        cleanup_s3_store(S3)
    end.

s3_not_found_paths_test_() ->
    {timeout, 30, fun s3_not_found_paths/0}.

s3_not_found_paths() ->
    S3 = setup_s3_store(),
    try
        %% Verify reading non-existent paths returns not_found
        ?assertEqual(not_found, hb_store:read(S3, <<"nonexistent-", (rand_id())/binary>>)),
        ?assertEqual(not_found, hb_store:read(S3, [<<"nonexistent-", (rand_id())/binary>>, <<"data">>])),

        %% Create a valid ID->P link, but don't write P/data
        ID = <<"notfound-id-", (rand_id())/binary>>,
        P = <<"notfound-uncommitted-", (rand_id())/binary>>,
        ok = hb_store:make_link(S3, P, ID),

        %% Verify ID/data returns not_found (P/data doesn't exist)
        ?assertEqual(not_found, hb_store:read(S3, [ID, <<"data">>])),

        ok
    after
        cleanup_s3_store(S3)
    end.

%% Helper functions

setup_s3_store() ->
    %% Check for required environment variables
    Required = [
        {<<"HB_S3_BUCKET">>, "S3 bucket name"},
        {<<"HB_S3_ACCESS_KEY">>, "S3 access key"},
        {<<"HB_S3_SECRET_KEY">>, "S3 secret key"},
        {<<"HB_S3_ENDPOINT">>, "S3 endpoint URL"}
    ],
    case check_required_env(Required) of
        ok -> ok;
        {missing, Missing} ->
            error(io_lib:format(
                "S3 integration tests require environment variables to be set:~n~s",
                [Missing]
            ))
    end,
    TestPrefix = <<"eunit-s3/", (rand_id())/binary, "/">>,
    BasePrefix = env_bin(<<"HB_S3_PREFIX">>, <<>>),
    Prefix = case BasePrefix of
        <<>> -> TestPrefix;
        _ -> <<BasePrefix/binary, "/", TestPrefix/binary>>
    end,
    S3 = #{
        <<"store-module">> => hb_store_s3,
        <<"name">> => <<"eunit-s3">>,
        <<"bucket">> => env_bin_required(<<"HB_S3_BUCKET">>),
        <<"access-key-id">> => env_bin_required(<<"HB_S3_ACCESS_KEY">>),
        <<"secret-access-key">> => env_bin_required(<<"HB_S3_SECRET_KEY">>),
        <<"endpoint">> => env_bin_required(<<"HB_S3_ENDPOINT">>),
        <<"region">> => env_bin(<<"HB_S3_REGION">>, <<"us-east-1">>),
        <<"force_path_style">> => true,
        <<"prefix">> => Prefix
    },
    hb_store:start(S3),
    %% Probe S3 availability. If writes fail, skip the test.
    case hb_store:write(S3, <<"__probe__">>, <<"ok">>) of
        ok -> S3;
        _ -> error("S3 not available; check endpoint, credentials, and bucket configuration.")
    end.

cleanup_s3_store(S3) ->
    _ = hb_store:reset(S3#{ <<"dangerous_reset">> => true }),
    ok.

rand_id() -> hb_util:human_id(crypto:strong_rand_bytes(32)).

check_required_env(Required) ->
    Missing = lists:filter(
        fun({Key, Desc}) ->
            case os:getenv(hb_util:list(Key)) of
                false -> true;
                "" -> true;
                _ -> false
            end
        end,
        Required
    ),
    case Missing of
        [] -> ok;
        _ ->
            MissingStr = lists:map(
                fun({Key, Desc}) ->
                    io_lib:format("  ~s: ~s", [Key, Desc])
                end,
                Missing
            ),
            {missing, string:join(MissingStr, "\n")}
    end.

env_bin_required(Key) ->
    case os:getenv(hb_util:list(Key)) of
        false -> error({missing_env, Key});
        "" -> error({missing_env, Key});
        Str -> hb_util:bin(Str)
    end.

env_bin(Key, Default) ->
    case os:getenv(hb_util:list(Key)) of
        false -> Default;
        "" -> Default;
        Str -> hb_util:bin(Str)
    end.
