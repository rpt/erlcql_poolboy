%% The MIT License (MIT)
%%
%% Copyright (c) 2013 Krzysztof Rutka
%%
%% Permission is hereby granted, free of charge, to any person obtaining a
%% copy of this software and associated documentation files (the "Software"),
%% to deal in the Software without restriction, including without limitation
%% the rights to use, copy, modify, merge, publish, distribute, sublicense,
%% and/or sell copies of the Software, and to permit persons to whom the
%% Software is furnished to do so, subject to the following conditions:
%%
%% The above copyright notice and this permission notice shall be included in
%% all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
%% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
%% IN THE SOFTWARE.

%% @author Krzysztof Rutka <krzysztof.rutka@gmail.com>
-module(erlcql_poolboy).

-export([start_link/2,
         start_link/3]).
-export([q/2, 'query'/2, q/3, 'query'/3,
         e/3, execute/3, e/4, execute/4,
         p/3, prepare/3]).

-type values() :: erlcql:values().

-spec start_link(atom(), proplists:proplist()) ->
          {ok, pid()} | {error, term()}.
start_link(Name, SizeOpts) ->
    start_link(Name, SizeOpts, []).

-spec start_link(atom(), proplists:proplist(), proplists:proplist()) ->
          {ok, pid()} | {error, term()}.
start_link(Name, SizeOpts, WorkerOpts) ->
    Tid = ets:new(erlcql_poolboy_prepared, [set, public,
                                            {read_concurrency, true}]),
    Key = prepared_statements_ets_tid,
    WorkerOpts2 = lists:keystore(Key, 1, WorkerOpts, {Key, Tid}),

    PoolOpts = [{name, {local, Name}},
                {worker_module, erlcql_client}],
    poolboy:start_link(PoolOpts ++ SizeOpts, WorkerOpts2).

q(PoolName, Query) ->
    q(PoolName, Query, erlcql:default(consistency)).

-spec 'query'(atom(), iodata()) -> erlcql:response().
'query'(PoolName, Query) ->
    'query'(PoolName, Query, erlcql:default(consistency)).

q(PoolName, Query, Consistency) ->
    'query'(PoolName, Query, Consistency).

-spec 'query'(atom(), iodata(), erlcql:consistency()) -> erlcql:response().
'query'(PoolName, Query, Consistency) ->
    Fun = fun(Worker) ->
                  erlcql_client:async_query(Worker, Query, Consistency)
          end,
    poolboy_call(PoolName, Fun).

p(PoolName, Query, Name) ->
    prepare(PoolName, Query, Name).

-spec prepare(atom(), iodata(), atom()) -> erlcql:response().
prepare(PoolName, Query, Name) ->
    {_, N, _, _} = poolboy:status(PoolName),
    First = poolboy:checkout(PoolName),
    case erlcql_client:prepare(First, Query, Name) of
        {ok, QueryId} ->
            Rest = [poolboy:checkout(PoolName) || _ <- lists:seq(1, N - 1)],
            ok = poolboy:checkin(PoolName, First),
            _ = [prepare_rest(PoolName, Worker, Query, QueryId) || Worker <- Rest],
            {ok, QueryId};
        {error, _Reason} = Error ->
            Error
    end.

-spec prepare_rest(atom(), pid(), iodata(), erlcql:uuid()) -> ok.
prepare_rest(PoolName, Worker, Query, QueryId) ->
    {ok, QueryId} = erlcql_client:prepare(Worker, Query),
    ok = poolboy:checkin(PoolName, Worker).

e(PoolName, QueryId, Values) ->
    execute(PoolName, QueryId, Values, erlcql:default(consistency)).

-spec execute(atom(), erlcql:uuid() | atom(), values()) -> erlcql:response().
execute(PoolName, QueryId, Values) ->
    execute(PoolName, QueryId, Values, erlcql:default(consistency)).

e(PoolName, QueryId, Values, Consistency) ->
    execute(PoolName, QueryId, Values, Consistency).

-spec execute(atom(), erlcql:uuid() | atom(),
              values(), erlcql:consistency()) -> erlcql:response().
execute(PoolName, QueryId, Values, Consistency) ->
    Fun = fun(Worker) ->
                  erlcql_client:async_execute(Worker, QueryId,
                                              Values, Consistency)
          end,
    poolboy_call(PoolName, Fun).

-spec poolboy_call(atom(), fun((pid()) -> erlcql:response())) -> erlcql:response().
poolboy_call(PoolName, Fun) ->
    Worker = poolboy:checkout(PoolName),
    Res = Fun(Worker),
    ok = poolboy:checkin(PoolName, Worker),
    case Res of
        {ok, QueryRef} ->
            erlcql_client:await(QueryRef);
        {error, _Reason} = Error ->
            Error
    end.
