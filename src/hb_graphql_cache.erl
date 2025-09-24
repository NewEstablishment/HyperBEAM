-module(hb_graphql_cache).
-export([init/0, get/2, put/3, cleanup/0]).
-include("include/hb.hrl").

-define(DEFAULT_CACHE_TTL, 300000).
-define(CACHE_TABLE, hb_graphql_cache_table).

%% @doc Initialize ETS table for caching
init() ->
    case ets:info(?CACHE_TABLE) of
        undefined ->
            ets:new(?CACHE_TABLE, [
                named_table, 
                public, 
                {read_concurrency, true},
                {write_concurrency, true}
            ]);
        _ ->
            ?CACHE_TABLE
    end.

%% @doc Get cached result
get(ID, _Opts) ->
    Now = erlang:system_time(millisecond),
    case ets:lookup(?CACHE_TABLE, ID) of
        [{ID, Result, Expiry}] when Expiry > Now ->
            ?event({graphql_cache_hit, ID}),
            {ok, Result};
        _ ->
            not_found
    end.

%% @doc Store result with TTL
put(ID, Result, Opts) ->
    TTL = hb_opts:get(graphql_cache_ttl, ?DEFAULT_CACHE_TTL, Opts),
    Expiry = erlang:system_time(millisecond) + TTL,
    ets:insert(?CACHE_TABLE, {ID, Result, Expiry}),
    ?event({graphql_cache_store, ID, expiry, Expiry}),
    ok.

%% @doc Cleanup expired entries (call periodically)
cleanup() ->
    Now = erlang:system_time(millisecond),
    Deleted = ets:select_delete(?CACHE_TABLE, [
        {{'_', '_', '$1'}, [{'<', '$1', Now}], [true]}
    ]),
    ?event({graphql_cache_cleanup, deleted, Deleted}),
    ok.