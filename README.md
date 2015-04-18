[![Stories in Ready](https://badge.waffle.io/mogproject/scala-redis-mock.png?label=ready&title=Ready)](https://waffle.io/mogproject/scala-redis-mock)
[![Build Status](https://travis-ci.org/mogproject/scala-redis-mock.svg?branch=master)](https://travis-ci.org/mogproject/scala-redis-mock)
[![License](https://img.shields.io/badge/license-Apache2-brightgreen.svg)](http://choosealicense.com/licenses/apache-2.0/)

# scala-redis-mock
Pure scala mock for [scala-redis](https://github.com/debasishg/scala-redis)

> **Note**: This library is under development!

## Try it out (without real Redis)!

```
sbt console

scala> r.set("foo", "bar")
res0: Boolean = true

scala> r.get("foo")
res1: Option[String] = Some(bar)
```

## To be implemented

#### Commands

|started|feature impl|complete|feature|
|:-:|:-:|:-:|:--|
|[x]|[ ]|[ ]|Keys|
|[x]|[x]|[ ]|Strings|
|[ ]|[ ]|[ ]|Lists|
|[ ]|[ ]|[ ]|Sets|
|[ ]|[ ]|[ ]|Sorted Sets|
|[ ]|[ ]|[ ]|Hashes|
|[ ]|[ ]|[ ]|HyperLogLog|

#### Features
|started|feature impl|complete|feature|
|:-:|:-:|:-:|:--|
|[x]|[x]|[ ]|Connection|
|[ ]|[ ]|[ ]|Server|
|[ ]|[ ]|[ ]|Scripting|
|[ ]|[ ]|[ ]|Pub/Sub|
|[ ]|[ ]|[ ]|Transactions|
|[ ]|[ ]|[ ]|Cluster|
