%%% @doc The HyperBEAM-native search layer for Odysee content: index-on-write,
%%% query-by-tags/metadata, return-ids. This device is the search surface the
%%% team's "build new search on hyperbeam" goal calls for, built on the VERIFIED
%%% native substrate -- `~query@1.0' -- with the external full-text backend
%%% (Elasticsearch, or a faster alternative) left as a documented, pluggable
%%% extension seam.
%%%
%%% HOW IT WORKS (the `native' backend, the default):
%%%
%%% Content uploaded through the Odysee bridge is written to the node's local
%%% store as committed AO-Core messages. `hb_cache' indexes (links) every
%%% top-level key of a message on write, so a message carrying searchable fields
%%% -- e.g. `title', `channel', `tag' -- is already a queryable index entry the
%%% moment it is stored. There is NO separate indexing step: write IS index.
%%%
%%% `query/3' accepts tag/key = value pairs (carried as request keys) and a
%%% `return' mode, and DELEGATES the actual matching to the native `~query@1.0'
%%% device over the node's local store. `~query@1.0/all' matches a message when
%%% every (non-control) key/value in the request is present on it, and returns
%%% either the matching content-ids (`return=paths') or the matching messages
%%% (`return=messages'). This device is therefore a thin, search-shaped facade
%%% over that engine: it normalises the request, maps the `return' vocabulary,
%%% and shapes the result as an HTTP-friendly status message.
%%%
%%% `return' modes (this device's vocabulary):
%%%
%%% - `ids' (default): return the content-ids of the matching messages, under
%%%   the `results' key. Maps to `~query@1.0''s `paths' return.
%%% - `messages': return the matching messages themselves, under `results'.
%%%   Maps to `~query@1.0''s `messages' return.
%%%
%%% A no-match query is NOT an error: it returns an empty `results' list with a
%%% 200 status. Every outcome is returned as `{ok, #{ <<"status">> => N, ... }}'
%%% (the `dev_odysee' idiom) so a non-2xx outcome maps to the right HTTP status
%%% and a 2xx response carries the results inline.
%%%
%%% THE PLUGGABLE BACKEND SEAM:
%%%
%%% `query/3' reads an optional `backend' from the request (or `Opts'). The only
%%% backend implemented here is `native' (the `~query@1.0' delegation above),
%%% which is the default. ANY OTHER backend value -- notably `elasticsearch' /
%%% `external' / a faster full-text alternative -- is a FUTURE, pluggable
%%% implementation pending the search specification (owned by Sam). Until that
%%% spec lands, an unrecognised backend returns a clear `501 Not Implemented'
%%% status map, NOT a crash and NOT a silent fallback to native.
%%%
%%% This is deliberately the single, obvious place an external-search driver
%%% slots in: add a backend clause to `dispatch_backend/4' (and, idiomatically,
%%% a `dev_odysee_search_<backend>' helper module folded into this device's
%%% package) that talks to the external index. The native path, the request
%%% normalisation, and the result shaping are all backend-agnostic and need no
%%% change. The external API itself is intentionally NOT invented here.
-module(dev_odysee_search).
-implements(<<"odysee-search@1.0">>).
-export([info/1, query/3]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% The native search backend: delegate matching to `~query@1.0'.
-define(BACKEND_NATIVE, <<"native">>).

%%% The native query device this layer delegates to, and the matching key on it.
-define(QUERY_DEVICE, <<"query@1.0">>).
-define(QUERY_KEY, <<"all">>).

%%% Control keys carried in the request that select behaviour rather than being
%%% searchable fields, plus the AO-Core/HTTP plumbing keys the singleton parser
%%% merges into every step message over real HTTP (measured for a GET: `accept',
%%% `accept-bundle', `ao-peer', `host', `method', `path', `priv', `user-agent';
%%% signed/POST requests additionally carry `signature'/`signature-input' and
%%% body framing). All are stripped before the remaining keys are handed to the
%%% matcher as the search spec, so only the caller's genuine search fields (e.g.
%%% `title', `channel', `tag') reach `~query@1.0'. This mirrors the header-key
%%% handling documented in `dev_odysee_id_route'.
-define(
    CONTROL_KEYS,
    [
        %% This device's control keys.
        <<"path">>,
        <<"device">>,
        <<"backend">>,
        <<"return">>,
        %% AO-Core message-structure keys.
        <<"commitments">>,
        <<"priv">>,
        %% HTTP plumbing keys merged into every step message over real HTTP.
        <<"accept">>,
        <<"accept-bundle">>,
        <<"ao-peer">>,
        <<"host">>,
        <<"method">>,
        <<"user-agent">>,
        <<"signature">>,
        <<"signature-input">>,
        <<"content-type">>,
        <<"content-length">>,
        <<"content-digest">>
    ]
).

info(_Opts) ->
    #{
        exports => [<<"query">>]
    }.

%% @doc Search for content by tag/key = value pairs. The searchable fields are
%% taken from the request keys (every key except the control keys -- `backend',
%% `return', and the AO-Core/HTTP plumbing keys). The `backend' selects the
%% search engine (default `native'); `return' selects the result shape (default
%% `ids', the matching content-ids; or `messages', the matching messages).
%%
%% Returns `{ok, #{ <<"status">> => 200, <<"results">> => [...],
%% <<"count">> => N, <<"backend">> => Backend }}' on success (an empty
%% `results' list for a no-match query), or a non-2xx status map on error /
%% unimplemented backend. Always `{ok, Map}', never a bare `{error, _}', so the
%% HTTP layer maps the `status' to the response code.
query(Base, Req, Opts) ->
    Backend = backend(Base, Req, Opts),
    ReturnMode = return_mode(Base, Req, Opts),
    Spec = search_spec(Base, Req, Opts),
    dispatch_backend(Backend, ReturnMode, Spec, Opts).

%% @doc Route to the selected search backend. `native' delegates to
%% `~query@1.0'; every other backend is the documented external-search seam,
%% answered with a 501 until the search spec lands. This is the single point an
%% external driver (Elasticsearch / a faster alternative) plugs in: add a clause
%% here that dispatches to its helper module.
dispatch_backend(?BACKEND_NATIVE, ReturnMode, Spec, Opts) ->
    native_search(ReturnMode, Spec, Opts);
dispatch_backend(Backend, _ReturnMode, _Spec, _Opts) ->
    backend_not_implemented(Backend).

%% @doc The native backend: delegate the match to the `~query@1.0' device over
%% the node's local store. The search spec is handed to `~query@1.0/all' as the
%% REQUEST (its `all' key matches on the request keys, reading the request first
%% then the base), carrying the mapped native return mode. We invoke it with
%% `hb_ao:raw' -- applying the `query@1.0' device's `all' function directly with
%% our Base/Req as-is -- rather than `hb_ao:resolve', because `raw' passes the
%% spec straight through as the request without path-parsing or hashpath
%% computation (`hb_cache:match' itself reaches `~match@1.0' the same way).
%%
%% `~query@1.0' returns the committed (signed) content-ids for `paths', and the
%% matching messages for `messages'. A no-match is normalised to an empty result
%% set with a 200 status: a search that finds nothing is a successful search,
%% not an error. `~query@1.0' signals no-match for the `paths'/`messages' return
%% types as a `{error, not_found}' that surfaces as a `case_clause' (it only
%% special-cases the `boolean' return), so both the error tuple and that
%% exception are treated here as the empty result.
native_search(ReturnMode, Spec, Opts) ->
    Req = Spec#{ <<"return">> => native_return(ReturnMode) },
    try hb_ao:raw(?QUERY_DEVICE, ?QUERY_KEY, #{}, Req, Opts) of
        {ok, Results} when is_list(Results) ->
            ?event(odysee_search,
                {native_search,
                    {return, ReturnMode},
                    {count, length(Results)}},
                Opts
            ),
            success(ReturnMode, Results);
        {error, not_found} ->
            empty_result(ReturnMode, Opts);
        {ok, Other} ->
            error_response({unexpected_query_result, Other});
        {error, Reason} ->
            error_response(Reason)
    catch
        error:{case_clause, {error, not_found}} ->
            empty_result(ReturnMode, Opts)
    end.

%% @doc A no-match: an empty, successful (200) result set.
empty_result(ReturnMode, Opts) ->
    ?event(odysee_search, {native_search, {return, ReturnMode}, no_match}, Opts),
    success(ReturnMode, []).

%% @doc The search spec handed to the matcher: the request keys minus the control
%% keys. Falls back to a base-message key when a search field is supplied there
%% instead of on the request (the `~query@1.0'/`dev_odysee_reference' convention
%% of accepting a field from either the request or the base).
search_spec(Base, Req, Opts) ->
    FromBase = hb_maps:without(?CONTROL_KEYS, Base, Opts),
    FromReq = hb_maps:without(?CONTROL_KEYS, Req, Opts),
    hb_maps:merge(FromBase, FromReq, Opts).

%% @doc The selected backend, normalised. A control value is read from the
%% request first, then the base message, then `Opts', defaulting to `native'
%% (the request/base fallback mirrors `~query@1.0' and `dev_odysee_reference',
%% so the device behaves identically whether reached in-process with the field
%% on the base or over HTTP with it on the request).
backend(Base, Req, Opts) ->
    case control_value(<<"backend">>, Base, Req, Opts) of
        Backend when is_binary(Backend), Backend =/= <<>> ->
            hb_ao:normalize_key(Backend);
        _ ->
            hb_ao:normalize_key(hb_opts:get(<<"backend">>, ?BACKEND_NATIVE, Opts))
    end.

%% @doc The requested return mode (`ids' or `messages'), normalised. Defaults to
%% `ids'. An unrecognised value is treated as `ids'.
return_mode(Base, Req, Opts) ->
    case control_value(<<"return">>, Base, Req, Opts) of
        <<"messages">> -> <<"messages">>;
        _ -> <<"ids">>
    end.

%% @doc Read a control value from the request first, then the base message.
%% Returns `not_found' when present in neither.
control_value(Key, Base, Req, Opts) ->
    case hb_maps:find(Key, Req, Opts) of
        {ok, Value} ->
            Value;
        _ ->
            hb_maps:get(Key, Base, not_found, Opts)
    end.

%% @doc Map this device's return vocabulary onto `~query@1.0''s: `ids' -> the
%% matching content-ids (`paths'); `messages' -> the matching messages.
native_return(<<"messages">>) -> <<"messages">>;
native_return(<<"ids">>) -> <<"paths">>.

%% @doc Shape a successful match as a 200 status message carrying the results
%% and a count, tagged with the backend that served them.
success(ReturnMode, Results) ->
    {ok, #{
        <<"status">> => 200,
        <<"backend">> => ?BACKEND_NATIVE,
        <<"return">> => ReturnMode,
        <<"count">> => length(Results),
        <<"results">> => Results
    }}.

%% @doc The external-search seam: an unrecognised backend is a future, pluggable
%% implementation pending the search specification. Answered with a clear 501 so
%% the caller learns the backend is not configured, never a crash or a silent
%% fallback to native.
backend_not_implemented(Backend) ->
    {ok, #{
        <<"status">> => 501,
        <<"backend">> => Backend,
        <<"message">> =>
            <<"External search backend not configured. Only the `native' "
                "backend (delegating to ~query@1.0 over the node's local "
                "store) is implemented; an external backend (Elasticsearch / a "
                "faster alternative) is a future pluggable implementation "
                "pending the search specification.">>
    }}.

%% @doc All error outcomes are returned as `{ok, #{ status => ... }}', not
%% `{error, _}' (the `dev_odysee' idiom): the HTTP layer maps a non-2xx `status'
%% to the response code, and a bare `{error, _}' returned through a reserved
%% verb wrapper would throw (see `dev_odysee_reference').
error_response(Reason) ->
    {ok, #{
        <<"status">> => 500,
        <<"message">> => hb_util:bin(io_lib:format("~p", [Reason]))
    }}.
