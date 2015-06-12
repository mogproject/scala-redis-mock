[![Stories in Ready](https://badge.waffle.io/mogproject/scala-redis-mock.png?label=ready&title=Ready)](https://waffle.io/mogproject/scala-redis-mock)
[![Build Status](https://travis-ci.org/mogproject/scala-redis-mock.svg?branch=master)](https://travis-ci.org/mogproject/scala-redis-mock)
[![Coverage Status](https://coveralls.io/repos/mogproject/scala-redis-mock/badge.svg)](https://coveralls.io/r/mogproject/scala-redis-mock)
[![License](https://img.shields.io/badge/license-Apache2-brightgreen.svg)](http://choosealicense.com/licenses/apache-2.0/)

# scala-redis-mock
Pure scala mock for [scala-redis](https://github.com/debasishg/scala-redis)

## Versions

Only these versions are tested.

|scala-redis-mock|scala-redis|redis-server|
|:-:|:-:|:-:|
|0.1.0|2.15|2.8.19|
|N/A|3.0|2.8.19|

## Getting Started

#### Clone this repository

```
$ cd your_work_dir
$ git clone git@github.com:mogproject/scala-redis-mock.git
$ cd scala-redis-mock
```

#### Try it out (without real Redis)!

```
$ sbt console

scala> r.set("foo", "bar")
res0: Boolean = true

scala> r.get("foo")
res1: Option[String] = Some(bar)
```

## Testing

- With mock

```
sbt test
```

- With real redis-server

You need to install Redis in the local host.

```
redis-server &
USE_REAL_REDIS=yes sbt test

redis-cli shutdown
```

## Supported Operations

- `Connection`, `Keys`, `String`, `Lists`, `Sets`, `Sorted Sets`, `Hashes`, `HyperLogLog`

The following features are not supported.

- `Server`, `Scripting`, `Pub/Sub`, `Transactions`, `Cluster`

