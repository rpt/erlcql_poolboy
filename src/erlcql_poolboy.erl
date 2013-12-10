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
-export([q/2,
         q/3]).

-spec start_link(atom(), proplists:proplist()) ->
          {ok, pid()} | {error, term()}.
start_link(Name, SizeOpts) ->
    start_link(Name, SizeOpts, []).

-spec start_link(atom(), proplists:proplist(), proplists:proplist()) ->
          {ok, pid()} | {error, term()}.
start_link(Name, SizeOpts, WorkerOpts) ->
    PoolOpts = [{name, {local, Name}},
                {worker_module, erlcql_client}],
    poolboy:start_link(PoolOpts ++ SizeOpts, WorkerOpts).

-spec q(atom(), iodata()) -> erlcql:reponse().
q(PoolName, Query) ->
    q(PoolName, Query, any).

-spec q(atom(), iodata(), erlcql:consistency()) -> erlcql:reponse().
q(PoolName, Query, Consistency) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        erlcql:q(Worker, Query, Consistency)
    end).
