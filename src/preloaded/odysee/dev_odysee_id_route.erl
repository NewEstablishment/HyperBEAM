%%% @doc An `on.request' hook that makes a bare `GET /<lbry-id>' resolve LBRY
%%% content. It detects an LBRY identifier as the leading path segment of an
%%% otherwise-bare request and rewrites the parsed message sequence to route to
%%% `~odysee@1.0/source' with that identifier carried as `native-id', so the
%%% existing verify-on-read `source' resolver serves the content.
%%%
%%% Why a hook (not a change to `?IS_ID'): a bare `GET /<id>' only performs a
%%% store read when the leading segment is `?IS_ID' (32/42/43 bytes). An LBRY
%%% identifier -- a 64-hex txid, a `txid:nout' outpoint, a 96-hex blob hash, or
%%% a 40-hex claim-id -- is non-`?IS_ID' and otherwise resolves to `not_found'
%%% with zero store I/O. Widening `?IS_ID' would reinterpret every non-ID first
%%% segment as a store key for the whole node; routing at the hook layer is
%%% surgical and leaves the singleton parser and `?IS_ID' untouched. The hook
%%% receives the already-parsed message sequence as its `body' (see
%%% `dev_meta:resolve_hook') and a hook MAY rewrite that sequence -- the rewrite
%%% takes effect because `dev_meta:handle_resolve' resolves the hook's returned
%%% `body' (proven by `dev_meta:modify_request_test').
%%%
%%% Shape detection mirrors `dev_odysee's `source' classifier. Of the four
%%% detected shapes, the txid, outpoint, and blob shapes are immutable ids that
%%% resolve to verified content through `source' directly (store-backed,
%%% native-commitment re-verified on read), so they are routed there unchanged.
%%%
%%% A 40-hex claim-id is a mutable locator, not an immutable id: it must first be
%%% mapped to the CURRENT immutable id before `source' can serve it. The hook
%%% therefore consults the local `~odysee-reference@1.0' device -- the meeting's
%%% "claim-id -> reference -> current immutable-id -> store" path -- by resolving
%%% the reference's `current' key with the claim-id as the reference key. When a
%%% reference exists, `current' resolves the link to the current outpoint's
%%% store-backed evidence (a claim message carrying `txid'/`nout'); the hook
%%% reconstructs that outpoint and rewrites to `~odysee@1.0/source' with it as
%%% `native-id', exactly as for a bare outpoint, so `source' re-reads and
%%% re-verifies the native commitment. When NO reference exists (`current'
%%% returns a 404/not_found rather than a resolved claim), the claim-id is routed
%%% to `source' unchanged, which answers with its own structured 400
%%% `unsupported_native_source_id' (mapping an unknown claim-id to an outpoint
%%% needs the network SDK and has no verifiable store-backed path) -- the honest
%%% layer-correct outcome, never a silent unverified network fallback.
%%%
%%% Only a strictly bare LBRY-id request is rewritten: the parsed sequence must
%%% be a synthetic base map (no `device', no `path' -- it carries at most the
%%% inbound HTTP header keys) followed by a single step-message whose `path' is
%%% an LBRY-id shape and which carries no `device' specifier. Anything else -- a
%%% standard `?IS_ID' singleton (binary base), a `~device@ver/...' path
%%% (`{as, Device, _}' base), or a multi-segment path (three or more elements)
%%% -- passes through unchanged. Detection is robust to the HTTP path, where the
%%% parser merges the request's global header keys (`accept', `host', `method',
%%% `user-agent', `priv', ...) into every step message; those keys are ignored
%%% by the guards and forwarded onto the rewritten `source' step.
-module(dev_odysee_id_route).
-implements(<<"odysee-id-route@1.0">>).
-export([request/3]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(SOURCE_DEVICE, <<"odysee@1.0">>).
-define(SOURCE_PATH, <<"source">>).
-define(REFERENCE_DEVICE, <<"odysee-reference@1.0">>).
-define(REFERENCE_CURRENT, <<"current">>).
-define(TXID_BYTES, 32).
-define(BLOB_BYTES, 48).
-define(CLAIM_ID_BYTES, 20).

%% @doc The on-request hook entry point. If the parsed request sequence is a
%% bare LBRY-id read, rewrite it to `~odysee@1.0/source' with an immutable
%% `native-id'; otherwise pass the hook request through unchanged.
%%
%% An immutable shape (txid, outpoint, blob) is routed to `source' with its own
%% id. A claim-id is a mutable locator: the hook first resolves the current
%% immutable id from `~odysee-reference@1.0'. If a reference exists, the request
%% is routed to `source' with the resolved outpoint as `native-id'; if not, the
%% claim-id is routed to `source' unchanged (today's behavior -- `source'
%% answers with a structured 400, never a crash).
request(_Base, HookReq, Opts) ->
    Body = hb_maps:get(<<"body">>, HookReq, [], Opts),
    case bare_lbry_id(Body, Opts) of
        {ok, direct, Id, Base, Step} ->
            ?event(odysee_id_route, {route_to_source, {native_id, Id}}, Opts),
            {ok, HookReq#{ <<"body">> => source_sequence(Id, Base, Step) }};
        {ok, claim_id, ClaimID, Base, Step} ->
            NativeId = resolved_native_id(ClaimID, Opts),
            ?event(odysee_id_route,
                {route_claim_id,
                    {claim_id, ClaimID},
                    {native_id, NativeId}},
                Opts
            ),
            {ok, HookReq#{ <<"body">> => source_sequence(NativeId, Base, Step) }};
        not_lbry_id ->
            {ok, HookReq}
    end.

%% @doc The `native-id' to route to `source' for a claim-id. If a reference
%% exists, it is the CURRENT immutable outpoint the claim-id points at; if no
%% reference exists, it is the claim-id itself (preserving today's behavior --
%% `source' answers an unmapped claim-id with a structured 400).
resolved_native_id(ClaimID, Opts) ->
    case reference_outpoint(ClaimID, Opts) of
        {ok, Outpoint} -> Outpoint;
        not_found -> ClaimID
    end.

%% @doc Resolve the current immutable outpoint a claim-id points at via the local
%% `~odysee-reference@1.0' device. `current' resolves the reference link to the
%% outpoint's store-backed claim evidence, from which the canonical `txid:nout'
%% outpoint is reconstructed. A missing reference (`current' returns a non-2xx
%% status map) or any non-resolving result is reported as `not_found'.
reference_outpoint(ClaimID, Opts) ->
    Base = #{
        <<"device">> => ?REFERENCE_DEVICE,
        <<"key">> => ClaimID
    },
    case hb_ao:resolve(Base, ?REFERENCE_CURRENT, Opts#{ <<"hashpath">> => ignore }) of
        {ok, Current} when is_map(Current) ->
            outpoint_from_current(Current, Opts);
        _ ->
            not_found
    end.

%% @doc Reconstruct the immutable outpoint from a resolved reference target. A
%% hit is the outpoint's claim evidence carrying a `txid' and `nout'; a miss is
%% the device's error map (a `status' >= 400, no `txid'). Only a genuine claim
%% evidence message yields an outpoint.
outpoint_from_current(Current, Opts) ->
    case hb_maps:find(<<"txid">>, Current, Opts) of
        {ok, TxID} when is_binary(TxID), byte_size(TxID) > 0 ->
            Nout = hb_maps:get(<<"nout">>, Current, 0, Opts),
            {ok, <<TxID/binary, ":", (hb_util:bin(Nout))/binary>>};
        _ ->
            not_found
    end.

%% @doc The rewritten sequence is what `GET /~odysee@1.0/source&native-id=<Id>'
%% resolves to: the base wrapped as `{as, odysee@1.0, _}' and the step routed to
%% `source' with the native id. The inbound base/step maps (carrying the request
%% header keys) are forwarded so nothing the downstream needs is dropped; only
%% the routing keys (`path', `native-id') are overridden.
source_sequence(Id, Base, Step) ->
    [
        {as, ?SOURCE_DEVICE, Base},
        Step#{ <<"path">> => ?SOURCE_PATH, <<"native-id">> => Id }
    ].

%% @doc Match the parsed sequence of a strictly bare LBRY-id request. The
%% singleton parser prepends a synthetic base map when the first segment is
%% non-`?IS_ID' (which every LBRY id is), so a bare `/<lbry-id>' parses to a
%% two-element `[BaseMap, StepMap]' where `BaseMap' has no routing identity and
%% `StepMap' carries `path => <lbry-id>'. A binary base (a standard `?IS_ID'),
%% an `{as, _, _}' base (an explicit device), and a three-or-more element body
%% (a multi-segment path) are all non-matches.
bare_lbry_id([Base, Step], Opts) when is_map(Base), is_map(Step) ->
    case is_bare_base(Base, Opts) andalso is_undirected_step(Step, Opts) of
        true ->
            case classify(hb_maps:get(<<"path">>, Step, not_found, Opts)) of
                {ok, Shape, Id} -> {ok, Shape, Id, Base, Step};
                not_lbry_id -> not_lbry_id
            end;
        false ->
            not_lbry_id
    end;
bare_lbry_id(_Body, _Opts) ->
    not_lbry_id.

%% @doc The synthetic base carries no routing identity: no `device' specifier
%% and no `path'. Over HTTP it additionally carries inbound header keys, which
%% are ignored here. A base with a `device' or `path' is an explicit, directed
%% request that must pass through.
is_bare_base(Base, Opts) ->
    not hb_maps:is_key(<<"device">>, Base, Opts)
        andalso not hb_maps:is_key(<<"path">>, Base, Opts).

%% @doc The step carries no `device' specifier (a `~device' suffix would set
%% one); its `path' is the candidate LBRY id. Header keys are ignored.
is_undirected_step(Step, Opts) ->
    not hb_maps:is_key(<<"device">>, Step, Opts).

%% @doc Classify a leading path segment as an LBRY identifier shape, matching
%% `dev_odysee's accepted `source' shapes plus the 40-hex claim-id. Returns the
%% routing shape alongside the id: `direct' for an immutable id (txid, outpoint,
%% blob) routed to `source' as-is, `claim_id' for a mutable locator that must
%% first be resolved through the reference layer. The original segment casing is
%% preserved; `source' lower-cases the key itself.
classify(Segment) when is_binary(Segment), byte_size(Segment) > 0 ->
    case binary:split(Segment, <<":">>) of
        [TxID, NoutBin] ->
            case valid_hex_bytes(TxID, ?TXID_BYTES) andalso is_nout(NoutBin) of
                true -> {ok, direct, Segment};
                false -> not_lbry_id
            end;
        [Single] ->
            single_shape(Single)
    end;
classify(_Segment) ->
    not_lbry_id.

%% @doc A single (non-outpoint) segment is an immutable blob/txid (routed
%% directly) or a mutable claim-id (resolved through the reference layer first).
single_shape(Single) ->
    case valid_hex_bytes(Single, ?BLOB_BYTES)
        orelse valid_hex_bytes(Single, ?TXID_BYTES) of
        true ->
            {ok, direct, Single};
        false ->
            case valid_hex_bytes(Single, ?CLAIM_ID_BYTES) of
                true -> {ok, claim_id, Single};
                false -> not_lbry_id
            end
    end.

valid_hex_bytes(Bin, Bytes) when is_binary(Bin), byte_size(Bin) == Bytes * 2 ->
    try binary:decode_hex(Bin) of
        Decoded -> byte_size(Decoded) == Bytes
    catch
        _:_ -> false
    end;
valid_hex_bytes(_Bin, _Bytes) ->
    false.

is_nout(Bin) when is_binary(Bin), byte_size(Bin) > 0 ->
    all_digits(Bin);
is_nout(_Bin) ->
    false.

all_digits(<<>>) ->
    true;
all_digits(<<Char, Rest/binary>>) when Char >= $0, Char =< $9 ->
    all_digits(Rest);
all_digits(_Bin) ->
    false.
