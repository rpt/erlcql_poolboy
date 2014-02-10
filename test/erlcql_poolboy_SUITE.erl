-module(erlcql_poolboy_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

-define(OPTS, [{cql_version, <<"3.0.0">>}]).
-define(POOL, erlcql_poolboy_test_pool).
-define(KEYSPACE, <<"erlcql_poolboy_tests">>).

-define(CREATE_KEYSPACE,
        <<"CREATE KEYSPACE IF NOT EXISTS erlcql_poolboy_tests ",
          "WITH replication = {'class': 'SimpleStrategy', ",
          "'replication_factor': 1}">>).
-define(DROP_KEYSPACE, <<"DROP KEYSPACE IF EXISTS erlcql_poolboy_tests">>).
-define(CREATE_TABLE, <<"CREATE TABLE IF NOT EXISTS erlcql_poolboy_tests.t ",
                        "(k int PRIMARY KEY, v text)">>).

-define(c(Name, Config), proplists:get_value(Name, Config)).

%% Suite ----------------------------------------------------------------------

all() ->
    [prepared_reconnect].

%% Fixtures -------------------------------------------------------------------

init_per_suite(Config) ->
    {ok, Pid} = erlcql_poolboy:start_link(setup, [], ?OPTS),
    erlcql_poolboy:q(setup, ?CREATE_KEYSPACE),
    erlcql_poolboy:q(setup, ?CREATE_TABLE),
    unlink(Pid),
    exit(Pid, kill),
    Config.

init_per_testcase(prepared_reconnect, Config) ->
    PoolboyOpts = [{size, 2}, {max_overflow, 0}],
    Query = [{select, <<"SELECT * FROM t">>}],
    ErlCQLOpts = [{prepare, Query}, {use, ?KEYSPACE} | ?OPTS],
    {ok, Pid} = erlcql_poolboy:start_link(?POOL, PoolboyOpts, ErlCQLOpts),
    [{pid, Pid} | Config];
init_per_testcase(_, Config) ->
    Config.

end_per_testcase(prepared_reconnect, Config) ->
    Pid = ?c(pid, Config),
    unlink(Pid),
    exit(Pid, kill);
end_per_testcase(_, _Config) ->
    ok.

end_per_suite(_Config) ->
    {ok, Pid} = erlcql_poolboy:start_link(setup, [], ?OPTS),
    erlcql_poolboy:q(setup, ?DROP_KEYSPACE),
    unlink(Pid),
    exit(Pid, kill).

%% Tests ----------------------------------------------------------------------

prepared_reconnect(_Config) ->
    2 = number_of_workers(),
    Workers1 = workers(),
    kill_one_worker(),
    timer:sleep(500),
    2 = number_of_workers(),
    Workers2 = workers(),
    Workers1 /= Workers2,
    [NewWorker] = lists:subtract(Workers2, Workers1),
    {ok, _} = erlcql_client:execute(NewWorker, select, [], one).

%% Helpers --------------------------------------------------------------------

number_of_workers() ->
    {_, N, _, _} = poolboy:status(?POOL),
    N.

workers() ->
    N = number_of_workers(),
    Workers = [poolboy:checkout(?POOL) || _ <- lists:seq(1, N)],
    [poolboy:checkin(?POOL, Worker) || Worker <- Workers],
    Workers.

kill_one_worker() ->
    Worker = poolboy:checkout(?POOL),
    ok = poolboy:checkin(?POOL, Worker),
    exit(Worker, kill).
