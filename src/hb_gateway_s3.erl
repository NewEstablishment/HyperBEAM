-module(hb_gateway_s3).
-export([read/2, data/2, read_headers/2, parse_ans104_header/1, data_with_range/3, stream_data/3, get_stream_info/2]).
-include("include/hb.hrl").

%% @doc Read ANS-104 DataItem from S3's `s3_bucket` using erlcloud backend
read(ID, Opts) ->
    Bucket = get_bucket(Opts),
    Key = <<ID/binary, ".ans104">>,
    io:format("S3 GATEWAY DEBUG: read() called for ID=~p, Bucket=~p, Key=~p~n", [ID, Bucket, Key]),
    S3Config = hb_s3_config:load_s3_device_config(),
    % Avoid downloading the full object: fetch only header + tags then parse.
    case read_headers(ID, Opts) of
        {ok, #{data_offset := DataOffset}} when is_integer(DataOffset), DataOffset > 0 ->
            HeaderEnd = DataOffset - 1,
            Range = <<"bytes=0-", (integer_to_binary(HeaderEnd))/binary>>,
            io:format("S3 GATEWAY DEBUG: Fetching header+tags only, Range=~p~n", [Range]),
            case hb_s3_erlcloud:get_object(Bucket, Key, #{range => Range}, S3Config) of
                {ok, #{<<"body">> := Partial, <<"status">> := _Status}} ->
                    io:format("S3 GATEWAY DEBUG: Retrieved header+tags bytes=~p~n", [byte_size_safe(Partial)]),
                    Result = parse_stored_ans104(Partial, Opts),
                    io:format("S3 GATEWAY DEBUG: parse_stored_ans104 result=~p~n", [element(1, Result)]),
                    Result;
                {error, #{<<"status">> := 404}} ->
                    io:format("S3 GATEWAY DEBUG: S3 returned 404 on header+tags fetch~n"),
                    not_found;
                {error, Reason} ->
                    io:format("S3 GATEWAY DEBUG: S3 error on header+tags fetch = ~p~n", [Reason]),
                    {error, Reason}
            end;
        not_found ->
            not_found;
        {error, Reason} ->
            {error, Reason}
    end.

byte_size_safe(Data) when is_binary(Data) -> byte_size(Data);
byte_size_safe(Data) when is_list(Data) -> length(Data);
byte_size_safe(_) -> unknown.

type(Data) when is_binary(Data) -> binary;
type(Data) when is_list(Data) -> list;
type(_) -> unknown.

%% @doc Get raw data from ANS-104 DataItem using range requests
data(ID, Opts) ->
    case read_data_field_only(ID, Opts) of
        {ok, Data} -> {ok, Data};
        {error, Reason} -> {error, Reason};
        not_found -> not_found
    end.

%% @doc Parse S3-stored ANS-104 format into HyperBEAM message format
parse_stored_ans104(RawData, Opts) ->
    io:format("S3 GATEWAY DEBUG: parse_stored_ans104 called, data type=~p, size=~p~n", [type(RawData), byte_size_safe(RawData)]),
    
    % Convert to binary if it's a list
    Binary = case RawData of
        Data when is_binary(Data) -> 
            io:format("S3 GATEWAY DEBUG: Data is already binary~n"),
            Data;
        Data when is_list(Data) -> 
            io:format("S3 GATEWAY DEBUG: Converting list to binary~n"),
            list_to_binary(Data);
        _ -> 
            io:format("S3 GATEWAY DEBUG: Unknown data type, trying to convert~n"),
            iolist_to_binary(RawData)
    end,
    
    io:format("S3 GATEWAY DEBUG: After conversion - binary size=~p~n", [byte_size(Binary)]),
    
    try 
        io:format("S3 GATEWAY DEBUG: Calling ar_bundles:deserialize~n"),
        % Deserialize as ANS-104 binary format
        case ar_bundles:deserialize(Binary) of
            TX when is_record(TX, tx) ->
                io:format("S3 GATEWAY DEBUG: ar_bundles:deserialize successful, TX record created~n"),
                % Convert TX record to HyperBEAM message format
                Message = tx_to_message(TX, Opts),
                io:format("S3 GATEWAY DEBUG: tx_to_message successful, message keys=~p~n", [maps:keys(Message)]),
                {ok, Message};
            Other ->
                io:format("S3 GATEWAY DEBUG: ar_bundles:deserialize returned unexpected result=~p~n", [Other]),
                {error, invalid_ans104_format}
        end
    catch
        Error:Reason:Stacktrace ->
            io:format("S3 GATEWAY DEBUG: Exception in parse_stored_ans104 - Error=~p, Reason=~p, Stacktrace=~p~n", [Error, Reason, Stacktrace]),
            {error, failed_to_parse_ans104}
    end.

%% @doc Convert TX record to HyperBEAM message format
tx_to_message(TX, _Opts) ->
    TagFields = tx_tags_to_message_fields(TX#tx.tags),
    BaseMessage = #{
        <<"data">> => TX#tx.data,
        <<"id">> => hb_util:encode(hb_util:id(TX, signed)),
        <<"owner">> => hb_util:encode(TX#tx.owner),
        <<"signature">> => hb_util:encode(TX#tx.signature)
    },
    maps:merge(BaseMessage, TagFields).

%% @doc Convert TX tags to message fields
tx_tags_to_message_fields(Tags) ->
    lists:foldl(
        fun({Name, Value}, Acc) ->
            Acc#{Name => Value}
        end,
        #{},
        Tags
    ).

%% @doc Read only ANS-104 headers to get data offset
read_headers(ID, Opts) ->
    io:format("S3 GATEWAY: read_headers called for ID=~p~n", [ID]),
    Bucket = get_bucket(Opts),
    Key = <<ID/binary, ".ans104">>,
    io:format("S3 GATEWAY: read_headers using bucket=~p, key=~p~n", [Bucket, Key]),

    % Fetch only first 2KB for header parsing
    HeaderSize = 2048,
    Range = <<"bytes=0-", (integer_to_binary(HeaderSize-1))/binary>>,
    
    io:format("S3 GATEWAY DEBUG: Fetching headers only, Range=~p~n", [Range]),

    S3Config = hb_s3_config:load_s3_device_config(),
    case hb_s3_erlcloud:get_object(Bucket, Key, #{range => Range}, S3Config) of
        {ok, #{<<"body">> := HeaderData, <<"status">> := 200}} ->
            parse_ans104_header(HeaderData);
        {ok, #{<<"body">> := HeaderData, <<"status">> := 206}} ->
            % 206 is partial content - expected for range requests
            parse_ans104_header(HeaderData);
        {error, #{<<"status">> := 404}} ->
            not_found;
        Error -> 
            io:format("S3 GATEWAY DEBUG: Header fetch error=~p~n", [Error]),
            Error
    end.

%% @doc Parse ANS-104 header to find data offset
parse_ans104_header(HeaderBinary) when is_binary(HeaderBinary) ->
    try
        <<SigType:16/little, _Rest0/binary>> = HeaderBinary,
        SigLength = case SigType of
            1 -> 512; 
            2 -> 64; 
            3 -> 64; 
            4 -> 64; 
            _ -> 512
        end,
        
        Offset1 = 2 + SigLength + 512, % SigType + Signature + Owner
        <<_:Offset1/binary, TargetFlag, _Rest1/binary>> = HeaderBinary,
        
        Offset2 = Offset1 + 1 + (case TargetFlag of 1 -> 32; _ -> 0 end),
        <<_:Offset2/binary, AnchorFlag, _Rest2/binary>> = HeaderBinary,
        
        Offset3 = Offset2 + 1 + (case AnchorFlag of 1 -> 32; _ -> 0 end),
        <<_:Offset3/binary, _NumTags:64/little, TagsSize:64/little, _/binary>> = HeaderBinary,
        
        DataOffset = Offset3 + 8 + 8 + TagsSize,
        io:format("S3 GATEWAY DEBUG: Parsed header - DataOffset=~p~n", [DataOffset]),
        {ok, #{data_offset => DataOffset, header_size => Offset3 + 16}}
    catch
        _:_ -> 
            io:format("S3 GATEWAY DEBUG: Header parse failed~n"),
            {error, header_parse_failed}
    end.

%% @doc Read only the data field using range requests
read_data_field_only(ID, Opts) ->
    % First get headers to find data offset
    case read_headers(ID, Opts) of
        {ok, #{data_offset := Offset}} ->
            Bucket = get_bucket(Opts),
            Key = <<ID/binary, ".ans104">>,
            
            % Fetch only the data portion
            Range = <<"bytes=", (integer_to_binary(Offset))/binary, "-">>,
            
            io:format("S3 GATEWAY DEBUG: Fetching data field only, Range=~p~n", [Range]),
            S3Config = hb_s3_config:load_s3_device_config(),
            case hb_s3_erlcloud:get_object(Bucket, Key, #{range => Range}, S3Config) of
                {ok, #{<<"body">> := DataOnly, <<"status">> := Status}} when Status =:= 200; Status =:= 206 ->
                    io:format("S3 GATEWAY DEBUG: Data field fetched, size=~p~n", [byte_size_safe(DataOnly)]),
                    {ok, DataOnly};
                {error, #{<<"status">> := 404}} ->
                    not_found;
                Error -> 
                    io:format("S3 GATEWAY DEBUG: Data fetch error=~p~n", [Error]),
                    Error
            end;
        not_found -> not_found;
        Error -> Error
    end.

%% @doc Read only the data field with HTTP Range support (single range)
data_with_range(ID, RangeBin, Opts) ->
    % First get headers to find data offset
    case read_headers(ID, Opts) of
        {ok, #{data_offset := Offset}} ->
            Bucket = get_bucket(Opts),
            Key = <<ID/binary, ".ans104">>,
            % Load S3 config
            S3Config = hb_s3_config:load_s3_device_config(),
            % Get full object size
            case hb_s3_erlcloud:head_object(Bucket, Key, S3Config) of
                {ok, Head} ->
                    TotalBin = maps:get(<<"content_length">>, Head, <<"0">>),
                    Total = case TotalBin of
                        BinVal when is_binary(BinVal) -> try binary_to_integer(BinVal) catch _:_ -> 0 end;
                        ListVal when is_list(ListVal) -> try list_to_integer(ListVal) catch _:_ -> 0 end;
                        I when is_integer(I) -> I;
                        _ -> 0
                    end,
                    DataSize = max(Total - Offset, 0),
                    case DataSize > 0 of
                        false -> {error, {range_not_satisfiable, 0}};
                        true ->
                            case hb_http_range:parse(RangeBin, DataSize) of
                                {ok, {Start, End}} ->
                                    % Get max chunk size from config (default 10MB)
                                    MaxChunkSize = hb_opts:get(s3_max_chunk_size, 10485760, #{}),
                                    % Limit the requested range to MaxChunkSize
                                    RequestedSize = End - Start + 1,
                                    ActualEnd = case RequestedSize > MaxChunkSize of
                                        true ->
                                            io:format("S3 GATEWAY DEBUG: Limiting range from ~p bytes to ~p bytes~n",
                                                     [RequestedSize, MaxChunkSize]),
                                            Start + MaxChunkSize - 1;
                                        false ->
                                            End
                                    end,
                                    S3Start = Offset + Start,
                                    S3End = Offset + ActualEnd,
                                    S3Range = <<"bytes=", (integer_to_binary(S3Start))/binary, "-", (integer_to_binary(S3End))/binary>>,
                                    case hb_s3_erlcloud:get_object(Bucket, Key, #{range => S3Range}, S3Config) of
                                        {ok, #{<<"body">> := Partial}} ->
                                            {ok, #{
                                                <<"body">> => Partial,
                                                <<"start">> => Start,
                                                <<"end">> => ActualEnd,  % Use ActualEnd instead of End
                                                <<"total">> => DataSize
                                            }};
                                        {error, #{<<"status">> := 404}} -> not_found;
                                        Error -> Error
                                    end;
                                {error, {range_not_satisfiable, _}} -> {error, {range_not_satisfiable, DataSize}};
                                {error, invalid_range} -> {error, invalid_range}
                            end
                    end;
                {error, _} -> {error, not_found}
            end;
        not_found -> not_found;
        Error -> Error
    end.

%% @private Resolve S3 bucket name from s3_module.config
get_bucket(_Opts) ->
    Conf = hb_s3_config:load_s3_device_config(),
    case maps:get(bucket, Conf, undefined) of
        Val when is_binary(Val), Val =/= <<>> -> Val;
        _ -> <<"offchain-dataitems">>  % Default fallback
    end.

%% @doc Stream data field with callback for chunked transfer encoding
stream_data(ID, CallbackFun, Opts) ->
    case read_headers(ID, Opts) of
        {ok, #{data_offset := Offset}} ->
            Bucket = get_bucket(Opts),
            Key = <<ID/binary, ".ans104">>,
            S3Config = hb_s3_config:load_s3_device_config(),
            
            % Get total size first
            case hb_s3_erlcloud:head_object(Bucket, Key, S3Config) of
                {ok, Head} ->
                    TotalSizeBin = maps:get(<<"content_length">>, Head, <<"0">>),
                    TotalSize = binary_to_integer(TotalSizeBin),
                    DataSize = TotalSize - Offset,
                    
                    % Create a callback that skips the header and only sends data
                    StreamCallback = fun(Chunk, ChunkOffset, _Total) ->
                        % Calculate actual position in file
                        FileOffset = ChunkOffset,
                        if
                            FileOffset + byte_size(Chunk) =< Offset ->
                                % This chunk is entirely before data section, skip it
                                continue;
                            FileOffset >= Offset ->
                                % This chunk is entirely in data section, send it all
                                CallbackFun(Chunk),
                                continue;
                            true ->
                                % This chunk spans the boundary
                                SkipBytes = Offset - FileOffset,
                                DataPart = binary:part(Chunk, SkipBytes, byte_size(Chunk) - SkipBytes),
                                CallbackFun(DataPart),
                                continue
                        end
                    end,
                    
                    % Stream the entire file, but only send data portion
                    case hb_s3_erlcloud:stream_object(Bucket, Key, #{}, S3Config, StreamCallback) of
                        {ok, _} -> {ok, DataSize};
                        {error, Reason} -> {error, Reason}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        not_found ->
            not_found;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Retrieve streaming metadata for a full data request without downloading the body
%% Returns content-type (from ANS-104 tags) and total data size (Content-Length - data_offset)
get_stream_info(ID, Opts) ->
    io:format("S3 GATEWAY: get_stream_info called for ID=~p~n", [ID]),
    case read_headers(ID, Opts) of
        {ok, #{data_offset := Offset}} ->
            Bucket = get_bucket(Opts),
            Key = <<ID/binary, ".ans104">>,
            io:format("S3 GATEWAY: Using bucket=~p, key=~p~n", [Bucket, Key]),
            S3Config = hb_s3_config:load_s3_device_config(),
            io:format("S3 GATEWAY: S3Config=~p~n", [S3Config]),
            % Fetch header+tags only to extract Content-Type
            HeaderEnd = Offset - 1,
            Range = <<"bytes=0-", (integer_to_binary(HeaderEnd))/binary>>,
            ContentType =
                case hb_s3_erlcloud:get_object(Bucket, Key, #{range => Range}, S3Config) of
                    {ok, #{<<"body">> := Partial}} ->
                        case safe_deserialize(Partial) of
                            {ok, TX} ->
                                find_content_type(TX#tx.tags);
                            _ -> <<"application/octet-stream">>
                        end;
                    _ -> <<"application/octet-stream">>
                end,
            % Get full object size, compute data length
            case hb_s3_erlcloud:head_object(Bucket, Key, S3Config) of
                {ok, Head} ->
                    TotalSizeBin = maps:get(<<"content_length">>, Head, <<"0">>),
                    Total = case TotalSizeBin of
                        B when is_binary(B) -> try binary_to_integer(B) catch _:_ -> 0 end;
                        L when is_list(L) -> try list_to_integer(L) catch _:_ -> 0 end;
                        I when is_integer(I) -> I;
                        _ -> 0
                    end,
                    DataSize = erlang:max(Total - Offset, 0),
                    {ok, #{ content_type => ContentType, total_size => DataSize, offset => Offset }};
                {error, Reason} ->
                    {error, Reason}
            end;
        not_found -> not_found;
        {error, Reason} -> {error, Reason}
    end.

%% @private Find Content-Type tag value, defaulting sensibly
find_content_type(Tags) ->
    case lists:keyfind(<<"Content-Type">>, 1, Tags) of
        {_, V} when is_binary(V) -> V;
        _ ->
            case lists:keyfind(<<"content-type">>, 1, Tags) of
                {_, V2} when is_binary(V2) -> V2;
                _ -> <<"application/octet-stream">>
            end
    end.

%% @private Safely deserialize a potentially partial ANS-104 (header+tags only)
safe_deserialize(Bin) when is_binary(Bin) ->
    try
        TX = ar_bundles:deserialize(Bin),
        case is_record(TX, tx) of
            true -> {ok, TX};
            false -> {error, invalid}
        end
    catch
        _:_ -> {error, invalid}
    end.
