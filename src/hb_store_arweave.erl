%%% @doc A store implementation that relays to an Arweave node, using an 
%%% intermediate cache of offsets as an ID->ArweaveLocation mapping.
-module(hb_store_arweave).
%%% Store API:
-export([scope/0, scope/1, type/2, read/2]).
%%% Indexing API:
-export([write_offset/5, path/1]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(ARWEAVE_INDEX_PATH, <<"~arweave@2.9-pre/offset">>).

%% @doc Although the index is local, loading an item via the index will make
%% requests to a remote node, so we define the scope as remote.
scope() -> remote.
scope(#{ <<"scope">> := Scope }) -> Scope;
scope(_) -> scope().

%% @doc Get the type of the data at the given key. We potentially cache the
%% result, so that we don't have to read the data from the GraphQL route
%% multiple times.
type(#{ <<"index-store">> := IndexStore }, ID) ->
    Type = case hb_store:read(IndexStore, path(ID)) of
        {ok, _Offset} -> simple;
        _ -> not_found
    end,
    ?event({type, {id, {explicit, ID}}, {type, Type}}),
    Type.

read(StoreOpts = #{ <<"index-store">> := IndexStore }, ID) ->
    case hb_store:read(IndexStore, path(ID)) of
        {ok, Binary} ->
            [IsTX, StartOffset, Length] = binary:split(Binary, <<":">>, [global]),
            Loaded = case hb_util:bool(IsTX) of
                true ->
                    load_bundle(ID,
                        hb_util:int(StartOffset), hb_util:int(Length), StoreOpts);
                false ->
                    load_item(
                        hb_util:int(StartOffset), hb_util:int(Length), StoreOpts)
            end,
            case Loaded of
                {ok, Message} ->
                    ?event({{read, ok},
                        {id, {explicit, ID}},
                        {is_tx, IsTX},
                        {start_offset, StartOffset},
                        {length, Length}});
                {error, Reason} ->
                    ?event({{read, error}, 
                        {id, {explicit, ID}}, 
                        {is_tx, IsTX},
                        {start_offset, StartOffset},
                        {length, Length},
                        {reason, Reason}})
            end,
            Loaded;
        not_found ->
           read_without_fallback(StoreOpts, ID);
        {ok, Data} ->
            ?event(arweave_store, {local_cache_found, {id, ID}}),
            {ok, Data}
    end,
    maybe_fallback(Result, StoreOpts, ID);
read(_, _) -> 
    {error, not_found}.


read_without_fallback(StoreOpts = #{ <<"index-store">> := IndexStore }, ID) -> 
    case hb_store:read(IndexStore, path(ID)) of
        {ok, Binary} ->
            [IsTX, StartOffset, Length] = binary:split(Binary, <<":">>, [global]),
            Result = case hb_util:bool(IsTX) of
                true ->
                    load_bundle(ID,
                        hb_util:int(StartOffset), hb_util:int(Length), StoreOpts);
                false ->
                    load_item(
                        hb_util:int(StartOffset), hb_util:int(Length), StoreOpts)
            end,
            case Result of 
                {ok, Message} ->
                    ?event(arweave_store, {chunks_found, {id, ID}, {message, Message}}),
                    %% Cannot be async, because it will conflict with some binary 
                    %% transactions.
                    %% This transactions makes fallback to hyperbuddy logic, which give 
                    %% 500 on first try, following by 200.
                    hb_store_remote_node:maybe_cache(StoreOpts, Message),
                    {ok, Message};
                {error, Reason} ->
                    ?event(arweave_store, {chunks_not_found, {id, ID}}),
                    ?event(error, {hb_store_arweave_local_store, {reason, Reason}}),
                    {error, Reason}
            end;
        not_found ->
            ?event(arweave_store, {no_index_found, {id, ID}}),
            {error, not_found}
    end.


maybe_fallback({ok, _} = Result, _, _) ->
    Result;
maybe_fallback({error, not_found}, #{<<"index-store">> := IndexStore} = StoreOpts, ID) ->
    %% We can fallback to /raw (hb_store_gateway)
    %% or we can fallback to force block search
    case hb_arweave_fallback:read(ID, StoreOpts) of 
        {ok, Height} ->
            ?event(arweave_fallback, {item_belong_to_height, {id, ID}, {height, Height}}),
            %% Temporary, we need to change this option to inside the store.
            %% TODO: Proper fix is to remove this and pass StoreOpts
            Opts = #{
                     arweave_index_ids => true,
                     arweave_index_store => #{<<"index-store">> => IndexStore}
                    },
            dev_copycat_arweave:arweave(Height, Height, Opts),
            read_without_fallback(StoreOpts, ID);
        _ ->
            {error, not_found}
    end.

read_with_type(Opts, Key) when is_list(Key) ->
    read_with_type(Opts, hb_store:join(Key));
read_with_type(Opts, Key) ->
    ?event({read_with_type, {key, Key}}),
    case read(Opts, Key) of
        {ok, Value} -> {simple, Value};
        {error, not_found} -> not_found;
        not_found -> not_found
    end.

load_item(StartOffset, Length, Opts) ->
    case read_chunks(StartOffset, Length, Opts) of
        {ok, SerializedItem} ->
            {
                ok,
                hb_message:convert(
                    ar_bundles:deserialize(SerializedItem, Opts),
                    <<"structured@1.0">>,
                    <<"ans104@1.0">>,
                    Opts
                )
            };
        {error, Reason} ->
            {error, Reason}
    end.

load_bundle(ID, StartOffset, Length, Opts) ->
    {ok, StructuredTXHeader} = hb_ao:resolve(
        #{ <<"device">> => <<"arweave@2.9-pre">> },
        #{ <<"path">> => <<"tx">>, <<"tx">> => ID, <<"exclude-data">> => true },
        Opts
    ),
    TXHeader = hb_message:convert(
        StructuredTXHeader,
        <<"tx@1.0">>,
        <<"structured@1.0">>,
        Opts),
    case read_chunks(StartOffset, Length, Opts) of
        {ok, SerializedItem} ->
            {
                ok,
                hb_message:convert(
                    TXHeader#tx{ data = SerializedItem },
                    <<"structured@1.0">>,
                    <<"tx@1.0">>,
                    Opts
                )
            };
        {error, Reason} ->
            {error, Reason}
    end.

read_chunks(StartOffset, Length, Opts) ->
    hb_ao:resolve(
        #{ <<"device">> => <<"arweave@2.9-pre">> },
        #{
            <<"path">> => <<"chunk">>,
            <<"offset">> => StartOffset + 1,
            <<"length">> => Length
        },
        Opts
    ).

write_offset(
        #{ <<"index-store">> := IndexStore }, ID, IsTX, StartOffset, Length) ->
    IsTxInt = hb_util:bool_int(IsTX),
    Value = <<
        (hb_util:bin(IsTxInt))/binary,
        ":",
        (hb_util:bin(StartOffset))/binary,
        ":",
        (hb_util:bin(Length))/binary
    >>,
    hb_store:write(IndexStore, path(ID), Value).

path(ID) ->
    <<
        ?ARWEAVE_INDEX_PATH/binary,
        "/",
        (hb_util:bin(ID))/binary
    >>.


%%% Tests

write_read_tx_test() ->
    Store = [hb_test_utils:test_store()],
    Opts = #{ 
        <<"index-store">> => Store 
    },
    ID = <<"bndIwac23-s0K11TLC1N7z472sLGAkiOdhds87ZywoE">>,
    EndOffset = 363524457284025,
    Size = 8387,
    StartOffset = EndOffset - Size,
    ok = write_offset(Opts, ID, true, StartOffset, Size),
    {ok, Bundle} = read(Opts, ID),
    ?assert(hb_message:verify(Bundle, all, #{})),
    {ok, Child} =
        hb_ao:resolve(
            Bundle,
            <<"1/2">>,
            #{}
        ),
    ?assert(hb_message:verify(Child, all, #{})),
    ExpectedChild = #{
        <<"data">> => <<"{\"totalTickedRewardsDistributed\":0,\"distributedEpochIndexes\":[],\"newDemandFactors\":[],\"newEpochIndexes\":[],\"tickedRewardDistributions\":[],\"newPruneGatewaysResults\":[{\"delegateStakeReturned\":0,\"stakeSlashed\":0,\"gatewayStakeReturned\":0,\"delegateStakeWithdrawing\":0,\"prunedGateways\":[],\"slashedGateways\":[],\"gatewayStakeWithdrawing\":0}]}">>,
        <<"data-protocol">> => <<"ao">>,
        <<"from-module">> => <<"cbn0KKrBZH7hdNkNokuXLtGryrWM--PjSTBqIzw9Kkk">>,
        <<"from-process">> => <<"agYcCFJtrMG6cqMuZfskIkFTGvUPddICmtQSBIoPdiA">>,
        <<"anchor">> => <<"MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAyODAxODg">>,
        <<"reference">> => <<"280188">>,
        <<"target">> => <<"1R5QEtX53Z_RRQJwzFWf40oXiPW2FibErT_h02pu8MU">>,
        <<"type">> => <<"Message">>,
        <<"variant">> => <<"ao.TN.1">>
    },
    ?assert(hb_message:match(ExpectedChild, Child, only_present)),
    ok.

%% XXX TODO: write/read for data item
