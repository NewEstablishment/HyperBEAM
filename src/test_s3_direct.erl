-module(test_s3_direct).
-export([test_s3_direct/0]).
-include("include/hb.hrl").

test_s3_direct() ->
    io:format("Testing direct S3 HTTP adapter call...~n"),

    % Get default options
    Opts = hb_opts:default_message_with_env(),

    % Test the S3 adapter directly
    Method = <<"GET">>,
    S3URL = <<"s3://migrated">>,
    Path = <<"/WuQZzOuWImTfVjyS0lkGCs14SND-e0ZnLd3VxWUlEVU/data">>,
    Message = #{},

    io:format("Calling hb_http_s3:request(~p, ~p, ~p, ~p, ~p)~n", [Method, S3URL, Path, Message, maps:without([store, routes], Opts)]),

    try
        Result = hb_http_s3:request(Method, S3URL, Path, Message, Opts),
        io:format("Result: ~p~n", [Result])
    catch
        Error:Reason:Stacktrace ->
            io:format("Error: ~p:~p~n", [Error, Reason]),
            io:format("Stacktrace: ~p~n", [Stacktrace])
    end.