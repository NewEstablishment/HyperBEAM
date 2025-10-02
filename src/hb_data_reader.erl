-module(hb_data_reader).
-export([metadata/2, read_range/4, fetch_full/3, stream/4, stream_from/5, next_chunk/5, chunk_size/1]).
-ifdef(TEST).
-export([compute_next_range/3]).
-endif.

-ifdef(STORE_EVENTS).
-include("hb_logger.hrl").
-else.
-define(event(X), ok).
-define(event(X, Y), ok).
-define(event(X, Y, Z), ok).
-endif.

-define(DEFAULT_CHUNK_SIZE, 1024 * 1024).

chunk_size(Opts) ->
    case hb_opts:get(stream_chunk_size, ?DEFAULT_CHUNK_SIZE, Opts) of
        Size when is_integer(Size), Size > 0 -> Size;
        _ -> ?DEFAULT_CHUNK_SIZE
    end.

%% @doc Retrieve dataset metadata, trying storage first then HTTP.
metadata(ID, Opts) when is_binary(ID), is_map(Opts) ->
    case should_use_storage(ID, Opts) of
        true ->
            ?event({trying_storage_metadata, {id, ID}}),
            case storage_metadata(ID, Opts) of
                {ok, StorageMeta} ->
                    ?event({storage_metadata_success, {id, ID}}),
                    % Storage gave us size, but we need HTTP for accurate content type
                    case http_metadata(ID, Opts) of
                        {ok, HttpMeta} ->
                            % Combine: use storage's store info but HTTP's content type
                            {ok, StorageMeta#{content_type => maps:get(content_type, HttpMeta)}};
                        _ ->
                            % Fall back to storage metadata with default content type
                            {ok, StorageMeta}
                    end;
                Error ->
                    ?event({storage_metadata_failed, {id, ID}, {error, Error}}),
                    http_metadata(ID, Opts)
            end;
        false ->
            http_metadata(ID, Opts)
    end.

%% @doc Retrieve metadata via HTTP (original implementation).
http_metadata(ID, Opts) ->
    case head_request(ID, Opts) of
        {ok, Meta} -> {ok, Meta};
        {error, _} -> range_metadata(ID, Opts)
    end.

%% @doc Materialize the bytes defined by a Range header, trying storage first.
read_range(ID, RangeHeader, Meta = #{size := Total}, Opts) when is_binary(RangeHeader) ->
    case should_use_storage_for_range(ID, Meta, Opts) of
        true ->
            ?event({trying_storage_range, {id, ID}}),
            case storage_read_range(ID, RangeHeader, Meta, Opts) of
                {ok, Result} ->
                    ?event({storage_range_success, {id, ID}}),
                    {ok, Result};
                Error ->
                    ?event({storage_range_failed, {id, ID}, {error, Error}}),
                    http_read_range(ID, RangeHeader, Meta, Opts)
            end;
        false ->
            http_read_range(ID, RangeHeader, Meta, Opts)
    end;
read_range(_, _, _, _) -> {error, invalid_arguments}.

%% @doc Materialize bytes via HTTP (original implementation).
http_read_range(ID, RangeHeader, Meta = #{size := Total}, Opts) ->
    case hb_http_range:parse(RangeHeader, Total) of
        {ok, {Start, End}} ->
            case range_request(ID, Start, End, Meta, Opts) of
                {ok, RangeInfo} ->
                    {ok, RangeInfo#{ content_type => maps:get(content_type, Meta) }};
                {error, Reason} -> {error, Reason}
            end;
        {error, {range_not_satisfiable, _}} ->
            {error, {range_not_satisfiable, Total}};
        {error, invalid_range} ->
            {error, {invalid_range, Total}}
    end.

%% @doc Fetch the entire body, trying storage first.
fetch_full(_ID, #{size := 0, content_type := CType}, _Opts) ->
    {ok, #{ data => <<>>, content_type => CType }};
fetch_full(ID, Meta = #{size := Total}, Opts) when Total > 0 ->
    case should_use_storage_for_range(ID, Meta, Opts) of
        true ->
            ?event({trying_storage_full, {id, ID}}),
            case storage_fetch_full(ID, Meta, Opts) of
                {ok, Result} ->
                    ?event({storage_full_success, {id, ID}}),
                    {ok, Result};
                Error ->
                    ?event({storage_full_failed, {id, ID}, {error, Error}}),
                    http_fetch_full(ID, Meta, Opts)
            end;
        false ->
            http_fetch_full(ID, Meta, Opts)
    end.

%% @doc Fetch full body via HTTP (original implementation).
http_fetch_full(ID, Meta = #{size := Total}, Opts) ->
    End = Total - 1,
    case range_request(ID, 0, End, Meta, Opts) of
        {ok, #{body := Body}} ->
            {ok, #{ data => Body, content_type => maps:get(content_type, Meta) }};
        {error, {range_not_supported, _}} ->
            full_get(ID, Meta, Opts);
        {error, Reason} -> {error, Reason}
    end.

%% @doc Stream from the beginning, chunk size governed by configuration.
stream(ID, Meta = #{size := Total}, ChunkFun, Opts) when is_function(ChunkFun, 2) ->
    case Total of
        0 ->
            ChunkFun(<<>>, true),
            {ok, Meta};
        _ ->
            ChunkSize = chunk_size(Opts),
            stream_loop(ID, Meta, 0, ChunkSize, ChunkFun, Opts)
    end.

%% @doc Continue streaming starting from a specific byte offset, trying storage first.
stream_from(ID, Meta, Offset, ChunkFun, Opts) when is_function(ChunkFun, 2) ->
    case should_use_storage_for_range(ID, Meta, Opts) of
        true ->
            case storage_stream_from(ID, Meta, Offset, ChunkFun, Opts) of
                {ok, Result} -> {ok, Result};
                Error ->
                    ?event({storage_stream_from_failed, {id, ID}, {error, Error}}),
                    http_stream_from(ID, Meta, Offset, ChunkFun, Opts)
            end;
        false ->
            http_stream_from(ID, Meta, Offset, ChunkFun, Opts)
    end.

%% @doc Stream from offset via HTTP (original implementation).
http_stream_from(ID, Meta, Offset, ChunkFun, Opts) ->
    ChunkSize = chunk_size(Opts),
    stream_loop(ID, Meta, Offset, ChunkSize, ChunkFun, Opts).

%% @doc Retrieve the next chunk, trying storage first.
next_chunk(_ID, _Meta = #{size := Total}, Offset, _ChunkSize, _Opts) when Offset >= Total ->
    {error, done};
next_chunk(ID, Meta = #{size := Total}, Offset, ChunkSize, Opts) ->
    case should_use_storage_for_range(ID, Meta, Opts) of
        true ->
            case storage_next_chunk(ID, Meta, Offset, ChunkSize, Opts) of
                {ok, Result} -> {ok, Result};
                Error ->
                    ?event({storage_next_chunk_failed, {id, ID}, {error, Error}}),
                    http_next_chunk(ID, Meta, Offset, ChunkSize, Opts)
            end;
        false ->
            http_next_chunk(ID, Meta, Offset, ChunkSize, Opts)
    end.

%% @doc Retrieve next chunk via HTTP (original implementation).
http_next_chunk(ID, Meta = #{size := Total}, Offset, ChunkSize, Opts) ->
    Normalized = case ChunkSize > 0 of true -> ChunkSize; false -> ?DEFAULT_CHUNK_SIZE end,
    {Start, End, _} = compute_next_range(Offset, Total, Normalized),
    range_request(ID, Start, End, Meta, Opts).

%% Internal helpers --------------------------------------------------------
head_request(ID, Opts) ->
    Req = base_request(ID, <<"HEAD">>),
    case hb_http:request(Req, Opts) of
        {ok, Msg} ->
            with_content_length(Msg,
                fun(Size) ->
                    {ok, #{
                        size => Size,
                        content_type => extract_content_type(Msg, Opts)
                    }}
                end,
                Opts
            );
        {error, _} -> {error, head_failed}
    end.

range_metadata(ID, Opts) ->
    case range_request(ID, 0, 0, #{}, Opts) of
        {ok, #{total := Total, content_type := CType}} ->
            {ok, #{ size => Total, content_type => CType }};
        {error, {range_not_satisfiable, Total}} ->
            {ok, #{ size => Total, content_type => hb_opts:get(range_default_content_type, <<"application/octet-stream">>, Opts) }};
        {error, Reason} -> {error, Reason}
    end.

full_get(ID, Meta, Opts) ->
    Req = base_request(ID, <<"GET">>),
    case hb_http:request(Req, Opts) of
        {ok, Msg} ->
            Body = hb_ao:get(<<"body">>, Msg, <<>>, Opts),
            {ok, #{ data => Body, content_type => maps:get(content_type, Meta) }};
        {error, Reason} -> {error, Reason}
    end.

stream_loop(ID, Meta = #{size := Total}, Offset, ChunkSize, ChunkFun, Opts) ->
    case Offset >= Total of
        true -> {ok, Meta};
        false ->
            {Start, End, IsFinal} = compute_next_range(Offset, Total, ChunkSize),
            case range_request(ID, Start, End, Meta, Opts) of
                {ok, #{body := Chunk, range_end := RangeEnd}} ->
                    ChunkFun(Chunk, IsFinal),
                    case IsFinal of
                        true -> {ok, Meta};
                        false -> stream_loop(ID, Meta, RangeEnd + 1, ChunkSize, ChunkFun, Opts)
                    end;
                {error, Reason} -> {error, Reason}
            end
    end.

range_request(ID, Start, End, Meta, Opts) when Start =< End ->
    RangeValue = range_header(Start, End),
    BaseReq = base_request(ID, <<"GET">>),
    Req = BaseReq#{ <<"range">> => RangeValue },
    case hb_http:request(Req, Opts) of
        {ok, Msg} ->
            handle_range_response(Msg, Start, End, Meta, Opts);
        {error, Msg} ->
            Status = hb_ao:get(<<"status">>, Msg, 0, Opts),
            case Status of
                416 ->
                    Total = range_total_from_header(hb_ao:get(<<"content-range">>, Msg, <<"bytes */0">>, Opts)),
                    {error, {range_not_satisfiable, Total}};
                _ -> {error, {http_error, Status}}
            end
    end;
range_request(_, _, _, _, _) -> {error, invalid_range_request}.

handle_range_response(Msg, Start, End, Meta, Opts) ->
    Status = hb_ao:get(<<"status">>, Msg, 200, Opts),
    Body = hb_ao:get(<<"body">>, Msg, <<>>, Opts),
    CType = extract_content_type(Msg, Opts),
    case Status of
        206 ->
            CR = hb_ao:get(<<"content-range">>, Msg, undefined, Opts),
            case parse_content_range(CR) of
                {ok, RangeStart, RangeEnd, Total} ->
                    ResolvedTotal = resolve_total(Total, Meta, Body, Start, RangeEnd),
                    {ok, #{
                        body => Body,
                        start => RangeStart,
                        range_end => RangeEnd,
                        total => ResolvedTotal,
                        content_type => CType,
                        final => final_flag(RangeEnd, ResolvedTotal)
                    }};
                error ->
                    ActualEnd = Start + erlang:max(byte_size(Body) - 1, 0),
                    ResolvedTotal = resolve_total(undefined, Meta, Body, Start, ActualEnd),
                    {ok, #{
                        body => Body,
                        start => Start,
                        range_end => ActualEnd,
                        total => ResolvedTotal,
                        content_type => CType,
                        final => final_flag(ActualEnd, ResolvedTotal)
                    }}
            end;
        200 ->
            case {Start, byte_size(Body)} of
                {0, Size} when Size >= (End - Start + 1) ->
                    ActualEnd = Start + Size - 1,
                    ResolvedTotal = resolve_total(Size, Meta, Body, Start, ActualEnd),
                    {ok, #{
                        body => Body,
                        start => Start,
                        range_end => ActualEnd,
                        total => ResolvedTotal,
                        content_type => CType,
                        final => final_flag(ActualEnd, ResolvedTotal)
                    }};
                _ ->
                    {error, {range_not_supported, Status}}
            end;
        _ ->
            {error, {http_error, Status}}
    end.

resolve_total(undefined, Meta, Body, Start, RangeEnd) ->
    case maps:get(size, Meta, undefined) of
        undefined -> Start + byte_size(Body);
        Size ->
            case RangeEnd >= Size of
                true -> Size;
                false -> Size
            end
    end;
resolve_total(Value, _Meta, _Body, _Start, _RangeEnd) when is_integer(Value) -> Value.

range_total_from_header(ContentRange) ->
    case parse_content_range(ContentRange) of
        {ok, _S, _E, Total} when is_integer(Total) -> Total;
        {unsatisfied, Total} -> Total;
        _ -> 0
    end.

parse_content_range(undefined) -> error;
parse_content_range(<<>>) -> error;
parse_content_range(<<"bytes ", Rest/binary>>) ->
    case binary:split(Rest, <<"/">>, []) of
        [<<"*">>, TotalBin] ->
            case safe_int(TotalBin) of
                {ok, Total} -> {unsatisfied, Total};
                error -> error
            end;
        [RangePart, TotalBin] ->
            case {safe_int(TotalBin), binary:split(RangePart, <<"-">>, [])} of
                {{ok, Total}, [StartBin, EndBin]} ->
                    case {safe_int(StartBin), safe_int(EndBin)} of
                        {{ok, Start}, {ok, RangeEnd}} -> {ok, Start, RangeEnd, Total};
                        _ -> error
                    end;
                _ -> error
            end;
        _ -> error
    end;
parse_content_range(_) -> error.

range_header(Start, End) when Start =< End ->
    iolist_to_binary([
        <<"bytes=" >>,
        integer_to_binary(Start),
        <<"-" >>,
        integer_to_binary(End)
    ]);
range_header(Start, End) ->
    error({invalid_range_bounds, Start, End}).

with_content_length(Msg, Fun, Opts) ->
    case hb_ao:get(<<"content-length">>, Msg, undefined, Opts) of
        undefined -> {error, missing_content_length};
        Bin ->
            case safe_int(Bin) of
                {ok, Size} -> Fun(Size);
                error -> {error, invalid_content_length}
            end
    end.

extract_content_type(Msg, Opts) ->
    hb_ao:get(
        <<"content-type">>,
        Msg,
        hb_opts:get(range_default_content_type, <<"application/octet-stream">>, Opts),
        Opts
    ).

safe_int(Bin) when is_binary(Bin) ->
    try {ok, binary_to_integer(Bin)} catch _:_ -> error end;
safe_int(Int) when is_integer(Int) -> {ok, Int};
safe_int(_) -> error.

base_request(ID, Method) ->
    #{
        <<"multirequest-responses">> => 1,
        <<"path">> => <<"/raw/", ID/binary>>,
        <<"method">> => Method
    }.

compute_next_range(Offset, Total, ChunkSize) when Offset < Total, ChunkSize > 0 ->
    Start = Offset,
    End = erlang:min(Start + ChunkSize - 1, Total - 1),
    {Start, End, End >= Total - 1};
compute_next_range(_, Total, _) when Total =< 0 -> {0, -1, true};
compute_next_range(_, _, _) -> {0, -1, true}.

final_flag(_RangeEnd, undefined) -> false;
final_flag(RangeEnd, Total) when is_integer(Total) -> RangeEnd >= Total - 1.

%% Storage-aware helper functions

%% @doc Decide whether to use storage hierarchy for initial requests.
should_use_storage(ID, Opts) ->
    hb_opts:get(use_storage_for_range, true, Opts).

%% @doc Decide whether to use storage for range requests based on size.
should_use_storage_for_range(_ID, Meta, Opts) ->
    case hb_opts:get(use_storage_for_range, true, Opts) of
        false -> false;
        true ->
            Size = maps:get(size, Meta, 0),
            MaxSize = 100 * 1024 * 1024, % 100MB hardcoded limit
            Size =< MaxSize
    end.


%% @doc Get metadata from storage hierarchy.
storage_metadata(ID, Opts) ->
    Stores = hb_opts:get(store, [], Opts),
    storage_metadata_from_stores(ID, Stores, Opts).

storage_metadata_from_stores(_, [], _) -> not_found;
storage_metadata_from_stores(ID, [Store | Rest], Opts) ->
    case hb_store:get_size(Store, ID) of
        {ok, Size} ->
            % For storage metadata, we can't reliably get content type
            % Fall back to default for now - HTTP metadata will be used for accurate content type
            {ok, #{
                size => Size,
                content_type => hb_opts:get(range_default_content_type, <<"application/octet-stream">>, Opts),
                store => Store
            }};
        _ -> storage_metadata_from_stores(ID, Rest, Opts)
    end.


%% @doc Read range from storage.
storage_read_range(ID, RangeHeader, Meta = #{size := Total}, Opts) ->
    case hb_http_range:parse(RangeHeader, Total) of
        {ok, {Start, End}} ->
            case maps:get(store, Meta, undefined) of
                Store when Store =/= undefined ->
                    storage_range_from_store(Store, ID, Start, End, Meta, Opts);
                _ ->
                    Stores = hb_opts:get(store, [], Opts),
                    storage_range_from_stores(ID, Start, End, Meta, Stores, Opts)
            end;
        {error, Reason} -> {error, Reason}
    end.

storage_range_from_store(Store, ID, Start, End, Meta, _Opts) ->
    case hb_store:read_range(Store, ID, Start, End) of
        {ok, Data} ->
            {ok, #{
                body => Data,
                start => Start,
                range_end => End,
                total => maps:get(size, Meta),
                content_type => maps:get(content_type, Meta),
                final => End >= (maps:get(size, Meta) - 1)
            }};
        Error -> Error
    end.

storage_range_from_stores(_, _, _, _, [], _) -> not_found;
storage_range_from_stores(ID, Start, End, Meta, [Store | Rest], Opts) ->
    case storage_range_from_store(Store, ID, Start, End, Meta, Opts) of
        {ok, Result} -> {ok, Result};
        _ -> storage_range_from_stores(ID, Start, End, Meta, Rest, Opts)
    end.

%% @doc Fetch full data from storage.
storage_fetch_full(ID, Meta, Opts) ->
    case maps:get(store, Meta, undefined) of
        Store when Store =/= undefined ->
            case hb_store:read(Store, ID) of
                {ok, Data} ->
                    {ok, #{
                        data => Data,
                        content_type => maps:get(content_type, Meta)
                    }};
                Error -> Error
            end;
        _ ->
            Stores = hb_opts:get(store, [], Opts),
            storage_full_from_stores(ID, Meta, Stores)
    end.

storage_full_from_stores(_, _, []) -> not_found;
storage_full_from_stores(ID, Meta, [Store | Rest]) ->
    case hb_store:read(Store, ID) of
        {ok, Data} ->
            {ok, #{
                data => Data,
                content_type => maps:get(content_type, Meta)
            }};
        _ -> storage_full_from_stores(ID, Meta, Rest)
    end.


%% @doc Get next chunk from storage.
storage_next_chunk(_ID, _Meta = #{size := Total}, Offset, _ChunkSize, _Opts) when Offset >= Total ->
    {error, done};
storage_next_chunk(ID, Meta, Offset, ChunkSize, Opts) ->
    Total = maps:get(size, Meta),
    Normalized = case ChunkSize > 0 of true -> ChunkSize; false -> 1024*1024 end,
    Start = Offset,
    End = erlang:min(Start + Normalized - 1, Total - 1),
    case maps:get(store, Meta, undefined) of
        Store when Store =/= undefined ->
            storage_range_from_store(Store, ID, Start, End, Meta, Opts);
        _ ->
            Stores = hb_opts:get(store, [], Opts),
            storage_range_from_stores(ID, Start, End, Meta, Stores, Opts)
    end.

%% @doc Stream from storage.
storage_stream_from(ID, Meta, Offset, ChunkFun, Opts) ->
    ChunkSize = chunk_size(Opts),
    Total = maps:get(size, Meta),
    storage_stream_loop(ID, Meta, Offset, Total, ChunkSize, ChunkFun, Opts).

storage_stream_loop(_, _, Offset, Total, _, _, _) when Offset >= Total ->
    {ok, done};
storage_stream_loop(ID, Meta, Offset, Total, ChunkSize, ChunkFun, Opts) ->
    Start = Offset,
    case storage_next_chunk(ID, Meta, Start, ChunkSize, Opts) of
        {ok, #{body := Chunk, range_end := RangeEnd}} ->
            IsFinal = RangeEnd >= Total - 1,
            ChunkFun(Chunk, IsFinal),
            case IsFinal of
                true -> {ok, #{size => Total}};
                false -> storage_stream_loop(ID, Meta, RangeEnd + 1, Total, ChunkSize, ChunkFun, Opts)
            end;
        Error -> Error
    end.
