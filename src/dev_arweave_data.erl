%%% @doc A device that returns raw Arweave data by TXID, matching the behavior
%%% of arweave.net/raw/<TXID>. Delegates to hb_store_arweave:read_raw/2 for
%%% the actual retrieval.
-module(dev_arweave_data).
-export([info/0, item/3]).
-include("include/hb.hrl").

info() ->
    #{
        exports => [<<"item">>]
    }.

%% @doc Return the raw bytes for an Arweave TXID.
%% The TXID is read from the <<"tx">> key in the request.
item(_Base, Req, Opts) ->
    case hb_ao:get(<<"tx">>, Req, Opts) of
        not_found ->
            {error, <<"Missing tx parameter">>};
        TXID ->
            Stores = hb_opts:get(store, [], Opts),
            case find_arweave_store(Stores) of
                not_found ->
                    {error, not_found};
                ArweaveStoreOpts ->
                    case hb_store_arweave:read_raw(ArweaveStoreOpts, TXID) of
                        {ok, RawBinary, Meta} ->
                            CT = maps:get(<<"content-type">>,
                                Meta, <<"application/octet-stream">>),
                            {ok, #{
                                <<"body">> => RawBinary,
                                <<"content-type">> => CT
                            }};
                        {error, not_found} ->
                            {error, not_found};
                        {error, Reason} ->
                            ?event({read_raw_error, {txid, TXID}, {reason, Reason}}),
                            {ok, #{<<"status">> => 502}}
                    end
            end
    end.

find_arweave_store(S = #{<<"store-module">> := hb_store_arweave}) -> S;
find_arweave_store(M) when is_map(M) -> not_found;
find_arweave_store([]) -> not_found;
find_arweave_store([S = #{<<"store-module">> := hb_store_arweave} | _]) -> S;
find_arweave_store([_ | Rest]) -> find_arweave_store(Rest).

%%% Tests

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

missing_tx_param_test() ->
    Req = #{},
    {error, _} = item(#{}, Req, #{}).

no_arweave_store_test() ->
    Store = hb_test_utils:test_store(),
    Opts = #{store => [Store]},
    Req = #{<<"tx">> => <<"some-txid">>},
    {error, not_found} = item(#{}, Req, Opts).

not_found_in_index_test() ->
    IndexStore = [hb_test_utils:test_store()],
    ArweaveStore = #{
        <<"store-module">> => hb_store_arweave,
        <<"index-store">> => IndexStore
    },
    Opts = #{store => [ArweaveStore]},
    Req = #{<<"tx">> => <<"nonexistent-txid">>},
    {error, not_found} = item(#{}, Req, Opts).

find_arweave_store_list_test() ->
    ArweaveStore = #{<<"store-module">> => hb_store_arweave, <<"index-store">> => []},
    OtherStore = #{<<"store-module">> => hb_store_fs, <<"name">> => <<"test">>},
    ArweaveStore = find_arweave_store([OtherStore, ArweaveStore]).

find_arweave_store_single_map_test() ->
    ArweaveStore = #{<<"store-module">> => hb_store_arweave, <<"index-store">> => []},
    ArweaveStore = find_arweave_store(ArweaveStore).

find_arweave_store_not_found_test() ->
    OtherStore = #{<<"store-module">> => hb_store_fs, <<"name">> => <<"test">>},
    not_found = find_arweave_store([OtherStore]),
    not_found = find_arweave_store(OtherStore),
    not_found = find_arweave_store([]).

-endif.
