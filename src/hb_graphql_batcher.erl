%%% @doc GraphQL request batcher for Arweave gateway queries.
%%%
%%% This module batches multiple GraphQL requests into a single query to reduce
%%% round-trip overhead. When the GraphQL endpoint has ~330ms latency, batching
%%% N requests into one query reduces total latency from N*330ms to ~330ms.
%%%
%%% Supports up to 5 concurrent batches (configurable) for high throughput.
%%%
%%% Usage:
%%%   Instead of calling hb_gateway_client:read/2 directly, call:
%%%   hb_graphql_batcher:read(ID, Opts)
%%%
%%% The batcher collects requests for a configurable window (default 50ms),
%%% then makes a single batched GraphQL query and fans out results to callers.
-module(hb_graphql_batcher).
-behaviour(gen_server).

%% API
-export([start_link/1, read/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include("include/hb.hrl").

-define(SERVER, ?MODULE).
-define(DEFAULT_BATCH_WINDOW_MS, 50).
-define(DEFAULT_MAX_BATCH_SIZE, 100).
-define(DEFAULT_MAX_CONCURRENT_BATCHES, 5).

-record(state, {
    pending = #{} :: #{binary() => [{pid(), reference(), map()}]},
    timer_ref = undefined :: undefined | reference(),
    batch_window_ms :: pos_integer(),
    max_batch_size :: pos_integer(),
    max_concurrent_batches :: pos_integer(),
    %% Track in-flight batches: BatchRef => Pending map
    in_flight = #{} :: #{reference() => map()}
}).

%%====================================================================
%% API
%%====================================================================

start_link(Opts) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Opts, []).

%% @doc Read a data item by ID, batching the GraphQL query with other concurrent requests.
%% Returns {ok, Message} or {error, Reason}.
-spec read(binary(), map()) -> {ok, map()} | {error, term()}.
read(ID, Opts) ->
    NormalizedID = hb_util:human_id(ID),
    Ref = make_ref(),
    Timeout = hb_opts:get(graphql_batch_timeout, 60000, Opts),
    gen_server:cast(?SERVER, {request, NormalizedID, self(), Ref, Opts}),
    receive
        {graphql_batch_result, Ref, Result} ->
            Result
    after Timeout ->
        {error, batch_timeout}
    end.

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(Opts) ->
    BatchWindowMs = hb_opts:get(graphql_batch_window_ms, ?DEFAULT_BATCH_WINDOW_MS, Opts),
    MaxBatchSize = hb_opts:get(graphql_max_batch_size, ?DEFAULT_MAX_BATCH_SIZE, Opts),
    MaxConcurrent = hb_opts:get(graphql_max_concurrent_batches, ?DEFAULT_MAX_CONCURRENT_BATCHES, Opts),
    ?event({graphql_batcher_started,
        {window_ms, BatchWindowMs},
        {max_size, MaxBatchSize},
        {max_concurrent, MaxConcurrent}}),
    {ok, #state{
        batch_window_ms = BatchWindowMs,
        max_batch_size = MaxBatchSize,
        max_concurrent_batches = MaxConcurrent
    }}.

handle_call(_Request, _From, State) ->
    {reply, {error, not_implemented}, State}.

handle_cast({request, ID, Pid, Ref, Opts}, State) ->
    #state{pending = Pending, timer_ref = TimerRef,
           batch_window_ms = WindowMs, max_batch_size = MaxSize} = State,

    %% Add request to pending map
    Waiters = maps:get(ID, Pending, []),
    NewPending = Pending#{ID => [{Pid, Ref, Opts} | Waiters]},

    %% Start timer if this is the first request in a new batch
    NewTimerRef = case TimerRef of
        undefined ->
            erlang:send_after(WindowMs, self(), flush_batch);
        Existing ->
            Existing
    end,

    NewState = State#state{pending = NewPending, timer_ref = NewTimerRef},

    %% Check if we've hit max batch size - flush immediately if possible
    BatchSize = maps:size(NewPending),
    case BatchSize >= MaxSize of
        true ->
            cancel_timer(NewTimerRef),
            maybe_flush_batch(NewState#state{timer_ref = undefined});
        false ->
            {noreply, NewState}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(flush_batch, #state{pending = Pending} = State) when map_size(Pending) == 0 ->
    {noreply, State#state{timer_ref = undefined}};

handle_info(flush_batch, State) ->
    maybe_flush_batch(State#state{timer_ref = undefined});

handle_info({batch_results, BatchRef, Results}, #state{in_flight = InFlight, pending = Pending} = State) ->
    case maps:get(BatchRef, InFlight, not_found) of
        not_found ->
            %% Unknown batch, ignore
            {noreply, State};
        BatchPending ->
            %% Fan out results to all waiting callers in this batch
            maps:foreach(
                fun(ID, Waiters) ->
                    Result = maps:get(ID, Results, {error, not_found}),
                    lists:foreach(
                        fun({Pid, Ref, _Opts}) ->
                            Pid ! {graphql_batch_result, Ref, Result}
                        end,
                        Waiters
                    )
                end,
                BatchPending
            ),
            NewState = State#state{in_flight = maps:remove(BatchRef, InFlight)},
            %% Check if there are pending requests that were waiting for a slot
            case map_size(Pending) > 0 of
                true ->
                    %% Flush pending requests now that we have a free slot
                    maybe_flush_batch(NewState);
                false ->
                    {noreply, NewState}
            end
    end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

%% @doc Try to flush pending batch if we have capacity.
maybe_flush_batch(#state{pending = Pending, in_flight = InFlight,
                         max_concurrent_batches = MaxConcurrent,
                         batch_window_ms = WindowMs} = State) ->
    InFlightCount = maps:size(InFlight),
    case InFlightCount < MaxConcurrent andalso map_size(Pending) > 0 of
        true ->
            %% We have capacity - flush the batch
            do_flush_batch(State);
        false when map_size(Pending) > 0 ->
            %% At capacity - ensure timer is set to retry later
            NewTimerRef = case State#state.timer_ref of
                undefined -> erlang:send_after(WindowMs, self(), flush_batch);
                Ref -> Ref
            end,
            {noreply, State#state{timer_ref = NewTimerRef}};
        false ->
            %% Nothing pending
            {noreply, State}
    end.

%% @doc Flush the current pending batch.
do_flush_batch(#state{pending = Pending, in_flight = InFlight} = State) ->
    %% Create a unique ref for this batch
    BatchRef = make_ref(),

    %% Extract all IDs and pick Opts from first request
    IDs = maps:keys(Pending),
    %% Get Opts from any waiter (they should all have compatible opts for GraphQL)
    [{_, _, FirstOpts} | _] = hd(maps:values(Pending)),

    ?event({graphql_batch_flush,
        {count, length(IDs)},
        {in_flight, maps:size(InFlight) + 1},
        {ids, IDs}}),

    %% Move pending to in_flight and clear pending for new requests
    NewInFlight = InFlight#{BatchRef => Pending},

    %% Spawn a process to do the batch query (don't block gen_server)
    Self = self(),
    spawn(fun() ->
        Results = execute_batch_query(IDs, FirstOpts),
        Self ! {batch_results, BatchRef, Results}
    end),

    {noreply, State#state{pending = #{}, timer_ref = undefined, in_flight = NewInFlight}}.

cancel_timer(undefined) -> ok;
cancel_timer(Ref) -> erlang:cancel_timer(Ref).

%% @doc Execute a batched GraphQL query for multiple IDs.
%% Returns a map of ID => {ok, Message} | {error, Reason}
-spec execute_batch_query([binary()], map()) -> #{binary() => {ok, map()} | {error, term()}}.
execute_batch_query(IDs, Opts) ->
    Query = build_batch_query(),
    Variables = #{<<"transactionIds">> => IDs},
    %% Use arweave.net for batch queries - Goldsky has pagination issues
    %% that cause it to return incomplete results for batch ID queries.
    BatchOpts = Opts#{
        routes => [#{
            <<"template">> => <<"/graphql">>,
            <<"nodes">> => [#{
                <<"prefix">> => hb_opts:get(graphql_batch_endpoint,
                    <<"https://arweave.net">>, Opts)
            }]
        }]
    },

    case hb_gateway_client:query(Query, Variables, BatchOpts) of
        {ok, GqlResponse} ->
            parse_batch_response(IDs, GqlResponse, Opts);
        {error, Reason} ->
            %% Return error for all IDs
            maps:from_list([{ID, {error, Reason}} || ID <- IDs])
    end.

build_batch_query() ->
    ItemSpec = hb_gateway_client:item_spec(),
    <<
        "query($transactionIds: [ID!]!) { ",
            "transactions(ids: $transactionIds, first: 100){ ",
                "edges { ", ItemSpec/binary, " } ",
            "} ",
        "} "
    >>.

%% @doc Parse the batch response and fetch raw data for each item.
parse_batch_response(RequestedIDs, GqlResponse, Opts) ->
    %% Navigate through response structure using maps:get for raw access
    %% (hb_ao:get transforms values, which we don't want here)
    case maps:get(<<"data">>, GqlResponse, not_found) of
        not_found ->
            maps:from_list([{ID, {error, not_found}} || ID <- RequestedIDs]);
        Data ->
            case maps:get(<<"transactions">>, Data, not_found) of
                not_found ->
                    maps:from_list([{ID, {error, not_found}} || ID <- RequestedIDs]);
                Transactions ->
                    case maps:get(<<"edges">>, Transactions, not_found) of
                        not_found ->
                            maps:from_list([{ID, {error, not_found}} || ID <- RequestedIDs]);
                        Edges when is_list(Edges) ->
                            %% Build a map of ID -> Node from the response
                            NodeMap = lists:foldl(
                                fun(Edge, Acc) ->
                                    case maps:get(<<"node">>, Edge, not_found) of
                                        not_found -> Acc;
                                        Node ->
                                            case maps:get(<<"id">>, Node, not_found) of
                                                not_found -> Acc;
                                                ID -> Acc#{ID => Node}
                                            end
                                    end
                                end,
                                #{},
                                Edges
                            ),
                            %% Process each requested ID
                            process_nodes_parallel(RequestedIDs, NodeMap, Opts);
                        _ ->
                            maps:from_list([{ID, {error, invalid_response}} || ID <- RequestedIDs])
                    end
            end
    end.

%% @doc Process nodes in parallel - fetch raw data and convert to messages.
%% For IDs not in NodeMap (GraphQL didn't return them), fall back to individual query.
process_nodes_parallel(IDs, NodeMap, Opts) ->
    Parent = self(),
    Ref = make_ref(),

    %% Spawn workers for each ID
    lists:foreach(
        fun(ID) ->
            spawn(fun() ->
                Result = case maps:get(ID, NodeMap, not_found) of
                    not_found ->
                        %% GraphQL batch didn't include this ID - fall back to individual query
                        fallback_individual_read(ID, Opts);
                    Node ->
                        process_single_node(ID, Node, Opts)
                end,
                Parent ! {Ref, ID, Result}
            end)
        end,
        IDs
    ),

    %% Collect results
    collect_results(Ref, IDs, #{}).

%% @doc Fallback: read a single ID using the original gateway_client (unbatched).
fallback_individual_read(ID, Opts) ->
    hb_gateway_client:read(ID, Opts).

collect_results(_Ref, [], Acc) ->
    Acc;
collect_results(Ref, Remaining, Acc) ->
    receive
        {Ref, ID, Result} ->
            collect_results(Ref, lists:delete(ID, Remaining), Acc#{ID => Result})
    after 30000 ->
        %% Timeout waiting for results - mark remaining as errors
        lists:foldl(
            fun(ID, A) -> A#{ID => {error, timeout}} end,
            Acc,
            Remaining
        )
    end.

%% @doc Process a single node - fetch raw data and convert to message.
process_single_node(ID, Node, Opts) ->
    %% Fetch raw data
    case hb_gateway_client:data(ID, Opts) of
        {ok, RawData} ->
            %% Convert to message using the gateway_client helper
            %% result_to_message/2 extracts ID from the Node's <<"id">> field
            hb_gateway_client:result_to_message(Node#{<<"data">> => RawData}, Opts);
        {error, _} = Err ->
            Err
    end.
