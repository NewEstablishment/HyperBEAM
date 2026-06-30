%%% @doc End-to-end test for the `odysee-id-route@1.0' request hook: a bare
%%% `GET /<lbry-id>' resolves LBRY content. A node is started with the hook on
%%% `on.request' (a node-config fixture -- no shared default is changed) and
%%% seeded with native, store-backed LBRY evidence (a transaction, a claim
%%% output / outpoint, and a blob) under their LBRY-id store keys.
%%%
%%% What is proven, and at which layer:
%%%   - Routing takes effect: a bare `GET /<txid>', `GET /<txid>:<nout>', and
%%%     `GET /<blob-hash>' over real HTTP each come back as the native LBRY
%%%     `source' object -- carrying the expected native commitment device and the
%%%     identifying field (`txid'/`nout'/`blob-hash') -- rather than `not_found'.
%%%     This is the load-bearing claim: the hook rewrote the bare-id request path
%%%     to `~odysee@1.0/source' and the rewrite drove resolution.
%%%   - The routed content is genuinely verified: resolving the hook-rewritten
%%%     sequence in-process re-verifies the native commitment with
%%%     `hb_message:verify(..., #{<<"commitment-ids">> => <<"all">>})' = true for
%%%     every shape. Re-verifying the wire-re-encoded HTTP read-back is a codec
%%%     round-trip concern (see `hb_odysee_e2e_test'), so full verification is
%%%     asserted in-process; the blob shape additionally re-verifies over HTTP.
%%%
%%% Pass-through is asserted both via a `?IS_ID' singleton (a published HB-native
%%% object) and `/~meta@1.0/info': both resolve normally with the hook
%%% installed, confirming non-LBRY-id requests are untouched.
%%%
%%% A bare 40-hex claim-id IS routed to `source' but answered with a structured
%%% 400 `unsupported_native_source_id': mapping a claim-id to an outpoint needs
%%% the network SDK and has no verifiable store-backed path, so the honest
%%% layer-correct outcome is a 400, not a silent unverified network fallback.
%%%
%%% Offline: ephemeral port, real wallet, fresh volatile store, no network.
-module(hb_odysee_id_route_test).
-include_lib("eunit/include/eunit.hrl").
-include("include/hb.hrl").

%% @doc The `on.request' hook wiring the id-route device. This is the documented
%% node-config recipe a deployment uses to make `GET /<lbry-id>' resolve LBRY
%% content; it is supplied here as a test fixture, never a default change.
hook() ->
    #{ <<"device">> => <<"odysee-id-route@1.0">>, <<"path">> => <<"request">> }.

%% @doc A bare `GET /<txid>' resolves to the verified transaction evidence.
bare_txid_resolves_content_test() ->
    Raw = binary:decode_hex(hb_lbry_tx:task0_tx_hex()),
    {ok, Msg} = hb_lbry_commitment:transaction_message(Raw),
    TxID = maps:get(<<"txid">>, Msg),
    Store = seed_store(TxID, Msg, <<"odysee-id-route-txid">>),
    Resp = http_get_bare_id(Store, TxID),
    assert_native_object(Resp, <<"lbry-transaction@1.0">>),
    ?assertEqual(TxID, hb_maps:get(<<"txid">>, Resp, undefined, #{})),
    assert_routed_content_verifies(Store, TxID).

%% @doc The discriminating test that the hook is the cause: with the SAME seeded
%% store but NO hook installed, a bare `GET /<txid>' returns `not_found' (a bare
%% non-`?IS_ID' segment does not trigger a store read). Installing the hook is
%% the only difference that makes the bare id resolve to content.
hook_is_required_for_bare_id_test() ->
    Raw = binary:decode_hex(hb_lbry_tx:task0_tx_hex()),
    {ok, Msg} = hb_lbry_commitment:transaction_message(Raw),
    TxID = maps:get(<<"txid">>, Msg),
    Store = seed_store(TxID, Msg, <<"id-route-nohook">>),
    %% No `on.request' hook: the bare id is not interpreted as a store key, so
    %% the request does not resolve to the native LBRY `source' object.
    NoHookNode =
        hb_http_server:start_node(#{ <<"port">> => 0, <<"store">> => [Store] }),
    WithoutHook = hb_http:get(NoHookNode, <<"/", TxID/binary>>, #{}),
    ?assertMatch({error, _}, WithoutHook),
    ?assertNot(response_has_commitment(WithoutHook, <<"lbry-transaction@1.0">>)),
    %% Same store, hook installed: the bare id resolves to the native object.
    WithHook = http_get_bare_id(Store, TxID),
    assert_native_object(WithHook, <<"lbry-transaction@1.0">>),
    ?assertEqual(TxID, hb_maps:get(<<"txid">>, WithHook, undefined, #{})).

%% @doc A bare `GET /<txid>:<nout>' resolves to the verified claim-output evidence.
bare_outpoint_resolves_content_test() ->
    Raw = binary:decode_hex(hb_lbry_tx:task0_tx_hex()),
    {ok, TxMsg} = hb_lbry_commitment:transaction_message(Raw),
    TxID = maps:get(<<"txid">>, TxMsg),
    Key = <<TxID/binary, ":0">>,
    {ok, Msg} = hb_lbry_commitment:claim_output_message(Raw, 0),
    Store = seed_store(Key, Msg, <<"odysee-id-route-outpoint">>),
    Resp = http_get_bare_id(Store, Key),
    assert_native_object(Resp, <<"lbry-claim@1.0">>),
    ?assertEqual(TxID, hb_maps:get(<<"txid">>, Resp, undefined, #{})),
    ?assertEqual(0, hb_maps:get(<<"nout">>, Resp, undefined, #{})),
    assert_routed_content_verifies(Store, Key).

%% @doc A bare `GET /<blob-hash>' (96-hex) resolves to the verified blob
%% evidence, and re-verifies over the HTTP read-back as well.
bare_blob_resolves_content_test() ->
    Body = <<"encrypted bytes">>,
    Hash = hb_lbry_stream_descriptor:blob_hash(Body),
    Msg = hb_lbry_commitment:blob_message(Hash, Body),
    Store = seed_store(Hash, Msg, <<"odysee-id-route-blob">>),
    Resp = http_get_bare_id(Store, Hash),
    assert_native_object(Resp, <<"lbry-blob@1.0">>),
    ?assertEqual(Hash, hb_maps:get(<<"blob-hash">>, Resp, undefined, #{})),
    ?assertEqual(
        true,
        hb_message:verify(Resp, #{ <<"commitment-ids">> => <<"all">> }, #{})
    ),
    assert_routed_content_verifies(Store, Hash).

%% @doc `/~meta@1.0/info' passes through unchanged with the hook installed.
meta_info_passes_through_test() ->
    Node =
        hb_http_server:start_node(#{
            <<"port">> => 0,
            <<"test-config-item">> => <<"present">>,
            <<"on">> => #{ <<"request">> => hook() },
            <<"store">> =>
                [hb_test_utils:test_store(hb_store_volatile, <<"id-route-meta">>)]
        }),
    {ok, Res} = hb_http:get(Node, <<"/~meta@1.0/info">>, #{}),
    ?assertEqual(<<"present">>, hb_ao:get(<<"test-config-item">>, Res, #{})).

%% @doc A standard `?IS_ID' singleton (a published HB-native object) resolves
%% normally with the hook installed: the bare-base guard makes the hook ignore a
%% leading `?IS_ID' segment (a binary base, not a synthetic map base), so a
%% normal id read is untouched.
standard_id_passes_through_test() ->
    ServerWallet = ar_wallet:new(),
    Node =
        hb_http_server:start_node(#{
            <<"port">> => 0,
            <<"priv-wallet">> => ServerWallet,
            <<"on">> => #{ <<"request">> => hook() },
            <<"store">> =>
                [hb_test_utils:test_store(hb_store_volatile, <<"id-route-std">>)]
        }),
    Body = <<"hb native object behind a standard id">>,
    {ok, PubResp} =
        hb_http:post(
            Node,
            <<"/~odysee@1.0/publish">>,
            #{ <<"body">> => Body, <<"content-type">> => <<"text/plain">> },
            #{}
        ),
    ContentID = hb_maps:get(<<"content-id">>, PubResp, undefined, #{}),
    ?assert(?IS_ID(ContentID)),
    {ok, ReadBack} = hb_http:get(Node, <<"/", ContentID/binary>>, #{}),
    Loaded = hb_cache:ensure_all_loaded(ReadBack, #{}),
    ?assertEqual(Body, hb_maps:get(<<"body">>, Loaded, undefined, #{})),
    ?assertEqual(
        <<"hb-native-signed">>,
        hb_maps:get(<<"provenance">>, Loaded, undefined, #{})
    ).

%% @doc A bare 40-hex claim-id IS routed to `source' (a 400 from `source', not a
%% bare-path `not_found', proves the rewrite fired) but is unsupported at this
%% layer.
bare_claim_id_routes_but_is_unsupported_test() ->
    ClaimID = <<"585d54c7bb8fd92043ed583c5aea18a9547028aa">>,
    Node =
        hb_http_server:start_node(#{
            <<"port">> => 0,
            <<"on">> => #{ <<"request">> => hook() },
            <<"store">> =>
                [hb_test_utils:test_store(hb_store_volatile, <<"id-route-claimid">>)]
        }),
    {error, Resp} = hb_http:get(Node, <<"/", ClaimID/binary>>, #{}),
    ?assertEqual(400, hb_maps:get(<<"status">>, Resp, undefined, #{})),
    ?assertEqual(
        <<"unsupported_native_source_id">>,
        hb_maps:get(<<"error">>, Resp, undefined, #{})
    ).

%% @doc Start a node with the id-route hook on `on.request' and the seeded store,
%% drive a bare `GET /<id>' over real HTTP, and return the response.
http_get_bare_id(Store, Id) ->
    Node =
        hb_http_server:start_node(#{
            <<"port">> => 0,
            <<"on">> => #{ <<"request">> => hook() },
            <<"store">> => [Store]
        }),
    {ok, Resp} = hb_http:get(Node, <<"/", Id/binary>>, #{}),
    Resp.

%% @doc A volatile store seeded with native LBRY evidence under its LBRY-id key
%% (the same store-key shape `~odysee@1.0/source' reads through `with_lbry_stores').
seed_store(Key, Msg, StoreName) ->
    Store = hb_test_utils:test_store(hb_store_volatile, StoreName),
    ok = hb_store:write(Store, #{ Key => Msg }, #{}),
    Store.

%% @doc Authoritative "verified content" proof, isolated from the HTTP codec
%% round-trip. The hook rewrites a bare `GET /<id>' to `~odysee@1.0/source' with
%% the id as `native-id'; this resolves that exact target through the device's
%% public API (`hb_ao:raw') against the seeded store and asserts the native
%% commitment re-verifies. `source' itself fails closed on an unverified object,
%% so a returned object with a `true' verify is the verified content the bare-id
%% route serves. Re-verifying the wire-re-encoded HTTP read-back is a codec
%% round-trip concern (see `hb_odysee_e2e_test'); it is asserted in-process here.
assert_routed_content_verifies(Store, Id) ->
    {ok, Resolved} =
        hb_ao:raw(
            <<"odysee@1.0">>,
            <<"source">>,
            #{},
            #{ <<"native-id">> => Id },
            #{ <<"store">> => [Store] }
        ),
    ?assertEqual(
        true,
        hb_message:verify(Resolved, #{ <<"commitment-ids">> => <<"all">> }, #{})
    ).

%% @doc Assert the response is the routed native LBRY object: it carries the
%% expected native commitment device (so it is the `source' evidence, not a
%% `not_found' or an HB-native object).
assert_native_object(Resp, CommitmentDevice) ->
    ?assert(has_commitment(Resp, CommitmentDevice)).

has_commitment(Msg, Device) when is_map(Msg) ->
    lists:any(
        fun(Commitment) ->
            hb_maps:get(<<"commitment-device">>, Commitment, undefined, #{}) == Device
        end,
        maps:values(hb_maps:get(<<"commitments">>, Msg, #{}, #{}))
    );
has_commitment(_Msg, _Device) ->
    false.

%% @doc Whether an `{ok|error, Body}' response carries the given native
%% commitment device. Tolerates a non-map error payload (e.g. the atom
%% `not_found').
response_has_commitment({_Status, Body}, Device) ->
    has_commitment(Body, Device).
