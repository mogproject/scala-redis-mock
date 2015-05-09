[![Stories in Ready](https://badge.waffle.io/mogproject/scala-redis-mock.png?label=ready&title=Ready)](https://waffle.io/mogproject/scala-redis-mock)
[![Build Status](https://travis-ci.org/mogproject/scala-redis-mock.svg?branch=master)](https://travis-ci.org/mogproject/scala-redis-mock)
[![Coverage Status](https://coveralls.io/repos/mogproject/scala-redis-mock/badge.svg)](https://coveralls.io/r/mogproject/scala-redis-mock)
[![License](https://img.shields.io/badge/license-Apache2-brightgreen.svg)](http://choosealicense.com/licenses/apache-2.0/)

# scala-redis-mock
Pure scala mock for [scala-redis](https://github.com/debasishg/scala-redis)

> **Note**: This library is under development!


## Clone this repository

```
$ cd your_work_dir
$ git clone git@github.com:mogproject/scala-redis-mock.git
$ cd scala-redis-mock
```

## Try it out (without real Redis)!

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


## To be implemented

#### Commands

|started|feature impl|complete|feature|
|:-:|:-:|:-:|:--|
|[x]|[x]|[ ]|Keys|
|[x]|[x]|[ ]|Strings|
|[x]|[x]|[ ]|Lists|
|[x]|[x]|[ ]|Sets|
|[x]|[x]|[ ]|Sorted Sets|
|[x]|[x]|[ ]|Hashes|
|[ ]|[ ]|[ ]|HyperLogLog|
|[ ]|[ ]|[ ]|sscan, hscan, zscan|
|[ ]|[ ]|[ ]|sort, sortNStore|

#### Features
|started|feature impl|complete|feature|
|:-:|:-:|:-:|:--|
|[x]|[x]|[ ]|Connection|
|[ ]|[ ]|[ ]|Server|
|[ ]|[ ]|[ ]|Scripting|
|[ ]|[ ]|[ ]|Pub/Sub|
|[ ]|[ ]|[ ]|Transactions|
|[ ]|[ ]|[ ]|Cluster|
