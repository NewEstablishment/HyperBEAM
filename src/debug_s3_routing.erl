-module(debug_s3_routing).
-export([test_routing/0]).

test_routing() ->
    % Test the route matching for /{id}/data pattern
    TestID = <<"WuQZzOuWImTfVjyS0lkGCs14SND-e0ZnLd3VxWUlEVU">>,
    TestPath = <<"/", TestID/binary, "/data">>,

    io:format("Testing route matching for path: ~p~n", [TestPath]),

    % Load the default options to get routes
    Opts = hb_opts:default_message_with_env(),
    Routes = hb_opts:get(routes, [], Opts),

    io:format("Found ~p routes~n", [length(Routes)]),

    % Find matching routes
    Matches = lists:filter(fun(Route) ->
        Template = maps:get(<<"template">>, Route, <<>>),
        case re:run(TestPath, Template) of
            {match, _} ->
                io:format("Route ~p matches path ~p~n", [Template, TestPath]),
                true;
            nomatch -> false
        end
    end, Routes),

    io:format("Found ~p matching routes~n", [length(Matches)]),

    case Matches of
        [] ->
            io:format("No routes matched! Available route templates:~n"),
            lists:foreach(fun(Route) ->
                Template = maps:get(<<"template">>, Route, <<"undefined">>),
                io:format("  - ~p~n", [Template])
            end, Routes);
        [FirstMatch|_] ->
            io:format("First matching route: ~p~n", [FirstMatch]),
            Nodes = maps:get(<<"nodes">>, FirstMatch, []),
            case Nodes of
                [FirstNode|_] ->
                    Prefix = maps:get(<<"prefix">>, FirstNode, <<>>),
                    io:format("First node prefix: ~p~n", [Prefix]),
                    case Prefix of
                        <<"s3://", _/binary>> ->
                            io:format("S3 routing should work!~n");
                        _ ->
                            io:format("First node is not S3: ~p~n", [Prefix])
                    end;
                [] ->
                    io:format("No nodes in matching route~n")
            end
    end,

    ok.