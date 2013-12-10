# Pool of erlCQL connections [![Build Status][travis_ci_image]][travis_ci]

See [erlCQL][erlcql] for more info.

## API

### Start

See [poolboy][poolboy] for pool specific options.

``` erlang
erlcql_poolboy:start_link(PoolName :: atom(),
                          PoolboyOptions :: proplists:proplist(),
                          ErlCQLOptions :: proplists:proplist()) ->
    {ok, Pid :: pid()} | {error, Reason :: term()}.
```

### Query

``` erlang
erlcql_poolboy:q(PoolName :: atom(),
                 Query :: iodata(),
                 Consistency :: erlcql:consistency()) ->
    erlcql:response().
```

[travis_ci]: https://travis-ci.org/rpt/erlcql_poolboy
[travis_ci_image]: https://travis-ci.org/rpt/erlcql_poolboy.png
[erlcql]: https://github.com/rpt/erlcql
[poolboy]: https://github.com/devinus/poolboy
