%%% @doc Tests for the HyperBEAM-native Odysee search device
%%% (`~odysee-search@1.0'): index-on-write, query-by-tags/metadata, return-ids,
%%% delegating to the native `~query@1.0' engine over the node's local store.
%%%
%%% The load-bearing property is that the search returns OUR OWN written content:
%%% we write committed messages carrying distinct searchable fields (`title',
%%% `channel'), then prove a tag query returns exactly the matching message(s) --
%%% asserted by reading the returned ids back and checking their content, the
%%% robust form (`~query@1.0' returns a content-addressed id for each matched
%%% message; whether that id is the committed or the uncommitted form is a store
%%% iteration-order detail, so we assert on the content the id resolves to, not
%%% on a fixed id string). A different value returns a different result; a
%%% no-match query returns empty. The pluggable external backend returns a clean
%%% 501 (the documented seam), never a crash or a silent native fallback.
%%%
%%% Offline: a fresh fs store, real wallets, an ephemeral-port node for the HTTP
%%% path, no network.
-module(hb_odysee_search_test).
-include_lib("eunit/include/eunit.hrl").
-include("include/hb.hrl").

-define(SEARCH_DEVICE, <<"odysee-search@1.0">>).

%% @doc A fresh node Opts: a private fs store and a wallet (so written content is
%% committed and content-addressed). Each test tags its own store so there is no
%% cross-test bleed.
opts(Tag) ->
    #{
        <<"store">> => [hb_test_utils:test_store(hb_store_fs, Tag)],
        <<"priv-wallet">> => ar_wallet:new()
    }.

%% @doc Write a committed message carrying the given searchable fields and return
%% the message itself (so a test can derive any of its ids). This is the "upload"
%% the bridge performs: `hb_cache:write' indexes every top-level key, so the
%% message is immediately queryable -- there is no separate indexing step.
write_content(Fields, Opts) ->
    Msg = hb_message:commit(Fields, Opts),
    {ok, _} = hb_cache:write(Msg, Opts),
    Msg.

%% @doc The set of ids a written message is content-addressed by (committed and
%% uncommitted). `~query@1.0' returns one of these per matched message; a test
%% uses set membership rather than a fixed id, since which form the store's
%% reverse index yields is iteration-order dependent.
ids_of(Msg, Opts) ->
    [hb_message:id(Msg, signed, Opts), hb_message:id(Msg, none, Opts)].

%% @doc Invoke the search device's `query' key with the given request fields via
%% the public AO-Core resolution API (the device is preloaded, so it is reached
%% by binding its name, not by calling its module).
search(Fields, Opts) ->
    Base = Fields#{ <<"device">> => ?SEARCH_DEVICE },
    hb_ao:resolve(Base, <<"query">>, Opts).

%% @doc The `results' list from a successful (200) search response.
results(Resp, Opts) ->
    ?assertEqual(200, hb_maps:get(<<"status">>, Resp, undefined, Opts)),
    hb_maps:get(<<"results">>, Resp, undefined, Opts).

%% @doc Read each result id back through the cache and return its `title' leaf.
%% This is what proves the search returned OUR content: the ids resolve to the
%% messages we wrote, carrying the fields we gave them.
titles_of(Ids, Opts) ->
    lists:sort(
        lists:map(
            fun(Id) ->
                {ok, Msg} = hb_cache:read(Id, Opts),
                hb_maps:get(<<"title">>, Msg, undefined, Opts)
            end,
            Ids
        )
    ).

%% @doc Set up three committed messages with distinct searchable fields and
%% return the Opts plus each message. Two share a `channel' so a channel query
%% is a genuine multi-match; each has a unique `title'.
setup_corpus(Tag) ->
    Opts = opts(Tag),
    MsgA =
        write_content(
            #{
                <<"title">> => <<"first odysee clip">>,
                <<"channel">> => <<"@alice">>
            },
            Opts
        ),
    MsgB =
        write_content(
            #{
                <<"title">> => <<"second odysee clip">>,
                <<"channel">> => <<"@alice">>
            },
            Opts
        ),
    MsgC =
        write_content(
            #{
                <<"title">> => <<"a totally different video">>,
                <<"channel">> => <<"@bob">>
            },
            Opts
        ),
    {Opts, MsgA, MsgB, MsgC}.

%% @doc (1) A tag query returns exactly the matching content. Searching for the
%% unique title of message A returns one id that resolves to A (proving the
%% search returns OUR written content, addressed by its own content id), and
%% nothing else.
query_by_tag_returns_matching_id_test() ->
    {Opts, MsgA, _MsgB, _MsgC} = setup_corpus(<<"search-by-tag">>),
    {ok, Resp} = search(#{ <<"title">> => <<"first odysee clip">> }, Opts),
    [Id] = results(Resp, Opts),
    ?assert(lists:member(Id, ids_of(MsgA, Opts))),
    ?assertEqual([<<"first odysee clip">>], titles_of([Id], Opts)),
    ?assertEqual(<<"native">>, hb_maps:get(<<"backend">>, Resp, undefined, Opts)),
    ?assertEqual(1, hb_maps:get(<<"count">>, Resp, undefined, Opts)).

%% @doc (1b) The same tag query in `messages' return mode returns the actual
%% written message, with its searchable fields intact -- proving the matched
%% content is genuinely ours, not a generic placeholder.
query_by_tag_returns_matching_message_test() ->
    {Opts, _MsgA, _MsgB, _MsgC} = setup_corpus(<<"search-by-tag-msg">>),
    {ok, Resp} =
        search(
            #{
                <<"title">> => <<"a totally different video">>,
                <<"return">> => <<"messages">>
            },
            Opts
        ),
    [Msg] = results(Resp, Opts),
    ?assertEqual(
        <<"a totally different video">>,
        hb_maps:get(<<"title">>, Msg, undefined, Opts)
    ),
    ?assertEqual(<<"@bob">>, hb_maps:get(<<"channel">>, Msg, undefined, Opts)).

%% @doc (2) A query by a DIFFERENT value returns a DIFFERENT result. Two messages
%% share `channel = @alice'; that query returns exactly those two (resolving to
%% their two titles, and never the `@bob' one), distinct from the single match
%% of the title query above.
query_by_different_value_returns_different_result_test() ->
    {Opts, _MsgA, _MsgB, _MsgC} = setup_corpus(<<"search-different">>),
    {ok, Resp} = search(#{ <<"channel">> => <<"@alice">> }, Opts),
    Ids = results(Resp, Opts),
    ?assertEqual(2, hb_maps:get(<<"count">>, Resp, undefined, Opts)),
    ?assertEqual(
        [<<"first odysee clip">>, <<"second odysee clip">>],
        titles_of(Ids, Opts)
    ),
    ?assertNot(lists:member(<<"a totally different video">>, titles_of(Ids, Opts))).

%% @doc (3) A no-match query returns an empty result set with a 200 status -- a
%% search that finds nothing is a successful search, not an error.
query_no_match_returns_empty_test() ->
    {Opts, _MsgA, _MsgB, _MsgC} = setup_corpus(<<"search-no-match">>),
    {ok, Resp} = search(#{ <<"channel">> => <<"@nobody">> }, Opts),
    ?assertEqual([], results(Resp, Opts)),
    ?assertEqual(0, hb_maps:get(<<"count">>, Resp, undefined, Opts)).

%% @doc (4) An unconfigured external backend returns a clean 501 (the documented
%% pluggable seam), never a crash and never a silent fallback to native.
unconfigured_external_backend_returns_501_test() ->
    {Opts, _MsgA, _MsgB, _MsgC} = setup_corpus(<<"search-501">>),
    {ok, Resp} =
        search(
            #{
                <<"backend">> => <<"elasticsearch">>,
                <<"channel">> => <<"@alice">>
            },
            Opts
        ),
    ?assertEqual(501, hb_maps:get(<<"status">>, Resp, undefined, Opts)),
    ?assertEqual(
        <<"elasticsearch">>,
        hb_maps:get(<<"backend">>, Resp, undefined, Opts)
    ),
    ?assertEqual(undefined, hb_maps:get(<<"results">>, Resp, undefined, Opts)).

%% @doc The explicit `native' backend behaves exactly as the default -- the seam
%% does not change the happy path.
explicit_native_backend_matches_default_test() ->
    {Opts, MsgA, _MsgB, _MsgC} = setup_corpus(<<"search-native">>),
    {ok, Resp} =
        search(
            #{
                <<"backend">> => <<"native">>,
                <<"title">> => <<"first odysee clip">>
            },
            Opts
        ),
    [Id] = results(Resp, Opts),
    ?assert(lists:member(Id, ids_of(MsgA, Opts))),
    ?assertEqual([<<"first odysee clip">>], titles_of([Id], Opts)).

%% @doc End-to-end over real HTTP: a tag query against an ephemeral-port node
%% returns our written content, proving the search surface works through the
%% HTTP layer (where the singleton parser merges header keys into the request),
%% not only the in-process resolver.
http_query_by_tag_returns_matching_id_test() ->
    Opts = opts(<<"search-http">>),
    MsgA =
        write_content(
            #{
                <<"title">> => <<"http indexed clip">>,
                <<"channel">> => <<"@carol">>
            },
            Opts
        ),
    Node = hb_http_server:start_node(Opts#{ <<"port">> => 0 }),
    {ok, Resp} =
        hb_http:get(
            Node,
            <<"~odysee-search@1.0/query?title=http+indexed+clip">>,
            Opts
        ),
    ?assertEqual(200, hb_maps:get(<<"status">>, Resp, undefined, Opts)),
    Ids = normalize_results(hb_maps:get(<<"results">>, Resp, undefined, Opts)),
    KnownIds = ids_of(MsgA, Opts),
    ?assert(lists:any(fun(Id) -> lists:member(Id, KnownIds) end, Ids)),
    ?assertEqual([<<"http indexed clip">>], titles_of(Ids, Opts)).

%% @doc The HTTP layer may return a single result either as a bare value or a
%% one-element list depending on codec; normalise to a list for membership.
normalize_results(Results) when is_list(Results) -> Results;
normalize_results(Result) -> [Result].
