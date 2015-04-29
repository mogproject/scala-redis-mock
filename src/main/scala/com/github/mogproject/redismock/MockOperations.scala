package com.github.mogproject.redismock

import com.github.mogproject.redismock.entity.Key
import com.github.mogproject.redismock.storage.Storage
import com.github.mogproject.redismock.util.StringUtil
import com.github.mogproject.redismock.util.ops._
import com.redis.{Operations, Redis}
import com.redis.serialization.{Format, Parse}
import scala.util.Random


trait MockOperations extends Operations with Storage {
  self: Redis =>

  lazy val random = new Random(12345L)

  // SORT
  // sort keys in a set, and optionally pull values for them
  override def sort[A](key: String,
                       limit: Option[(Int, Int)] = None,
                       desc: Boolean = false,
                       alpha: Boolean = false,
                       by: Option[String] = None,
                       get: List[String] = Nil)(implicit format: Format, parse: Parse[A]): Option[List[Option[A]]] = ???

  // SORT with STORE
  // sort keys in a set, and store result in the supplied key
  override def sortNStore[A](key: String,
                             limit: Option[(Int, Int)] = None,
                             desc: Boolean = false,
                             alpha: Boolean = false,
                             by: Option[String] = None,
                             get: List[String] = Nil,
                             storeAt: String)(implicit format: Format, parse: Parse[A]): Option[Long] = ???

  // KEYS
  // returns all the keys matching the glob-style pattern.
  override def keys[A](pattern: Any = "*")(implicit format: Format, parse: Parse[A]): Option[List[Option[A]]] = {
    val r = StringUtil.globToRegex(pattern.toString)
    currentDB.keys.view.map(_.k).filter(_.parse(Parse.parseStringSafe).matches(r))
      .map(_.parseOption(parse)).toList |> Some.apply
  }

  // RANDKEY
  // return a randomly selected key from the currently selected DB.
  @deprecated("use randomkey", "2.8") override def randkey[A](implicit parse: Parse[A]): Option[A] = randomkey[A]

  // RANDOMKEY
  // return a randomly selected key from the currently selected DB.
  override def randomkey[A](implicit parse: Parse[A]): Option[A] = {
    // TODO: make random after implementing 'scan'
    currentDB.keys.headOption.map(_.k.parse(parse))
  }

  // RENAME (oldkey, newkey)
  // atomically renames the key oldkey to newkey.
  override def rename(oldkey: Any, newkey: Any)(implicit format: Format): Boolean =
    currentDB.renameKey(Key(oldkey), Key(newkey)) whenFalse {throw new RuntimeException("ERR no such key")}

  // RENAMENX (oldkey, newkey)
  // rename oldkey into newkey but fails if the destination key newkey already exists.
  override def renamenx(oldkey: Any, newkey: Any)(implicit format: Format): Boolean = withDB {
    !exists(newkey) && rename(oldkey, newkey)
  }

  // DBSIZE
  // return the size of the db.
  override def dbsize: Option[Long] = Some(currentDB.size)

  // EXISTS (key)
  // test if the specified key exists.
  override def exists(key: Any)(implicit format: Format): Boolean = Key(key) |> currentDB.contains

  // DELETE (key1 key2 ..)
  // deletes the specified keys.
  override def del(key: Any, keys: Any*)(implicit format: Format): Option[Long] = {
    val oldSize = currentDB.size
    (key :: keys.toList) foreach {Key(_) |> currentDB.remove}
    Some(oldSize - currentDB.size)
  }

  // TYPE (key)
  // return the type of the value stored at key in form of a string.
  override def getType(key: Any)(implicit format: Format): Option[String] =
    (Key(key) |> currentDB.get).map(_.valueType.toString.toLowerCase).getOrElse("none") |> Some.apply

  // EXPIRE (key, expiry)
  // sets the expire time (in sec.) for the specified key.
  override def expire(key: Any, ttl: Int)(implicit format: Format): Boolean = pexpire(key, ttl * 1000)

  // PEXPIRE (key, expiry)
  // sets the expire time (in milli sec.) for the specified key.
  override def pexpire(key: Any, ttlInMillis: Int)(implicit format: Format): Boolean =
    currentDB.updateTTL(Key(key), ttlInMillis)

  // EXPIREAT (key, unix timestamp)
  // sets the expire time for the specified key.
  override def expireat(key: Any, timestamp: Long)(implicit format: Format): Boolean = pexpireat(key, timestamp * 1000L)

  // PEXPIREAT (key, unix timestamp)
  // sets the expire timestamp in millis for the specified key.
  override def pexpireat(key: Any, timestampInMillis: Long)(implicit format: Format): Boolean =
    currentDB.updateExpireAt(Key(key), timestampInMillis)

  // TTL (key)
  // returns the remaining time to live of a key that has a timeout
  override def ttl(key: Any)(implicit format: Format): Option[Long] =
    pttl(key).map(x => if (x < 0L) -1L else math.round(x / 1000.0))

  // PTTL (key)
  // returns the remaining time to live of a key that has a timeout in millis
  override def pttl(key: Any)(implicit format: Format): Option[Long] = currentDB.getTTL(Key(key))

  // SELECT (index)
  // selects the DB to connect, defaults to 0 (zero).
  override def select(index: Int): Boolean = (0 <= index) whenTrue {db = index}

  // FLUSHDB the DB
  // removes all the DB data.
  override def flushdb: Boolean = true whenTrue currentDB.clear()

  // FLUSHALL the DB's
  // removes data from all the DB's.
  override def flushall: Boolean = true whenTrue currentNode.clear()

  // MOVE
  // Move the specified key from the currently selected DB to the specified destination DB.
  override def move(key: Any, db: Int)(implicit format: Format): Boolean = if (this.db == db) {
    false
  } else withDB {
    val k = Key(key)
    currentDB.getWithExpireAt(k).map { case (v, t) =>
      val dst = getDB(db)
      dst.synchronized {
        dst.update(k, v, t)
        currentDB.remove(k)
      }
    }.isDefined
  }

  // QUIT
  // exits the server.
  override def quit: Boolean = disconnect

  // AUTH
  // auths with the server.
  override def auth(secret: Any)(implicit format: Format): Boolean = true // always returns true in the mock

  // PERSIST (key)
  // Remove the existing timeout on key, turning the key from volatile (a key with an expire set)
  // to persistent (a key that will never expire as no timeout is associated).
  override def persist(key: Any)(implicit format: Format): Boolean = currentDB.removeExpireAt(Key(key))

  // SCAN
  // Incrementally iterate the keys space (since 2.8)
  override def scan[A](cursor: Int, pattern: Any = "*", count: Int = 10)
                      (implicit format: Format, parse: Parse[A])
  : Option[(Option[Int], Option[List[Option[A]]])] =
  // TODO: implement
    ???
}
