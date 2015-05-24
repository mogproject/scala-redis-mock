package com.github.mogproject.redismock

import com.github.mogproject.redismock.entity._
import com.github.mogproject.redismock.generic.GenericOperations
import com.github.mogproject.redismock.storage.Storage
import com.github.mogproject.redismock.util.{Bytes, StringUtil}
import com.github.mogproject.redismock.util.ops._
import com.redis.{Operations, Redis}
import com.redis.serialization.{Format, Parse}
import scala.util.{Try, Random}


trait MockOperations extends Operations with Storage with GenericOperations {
  self: Redis =>

  lazy val random = new Random(12345L)

  /** helper class for sort */
  case class Sorter[A](data: Seq[Bytes],
                       lookup: Bytes => Option[Bytes] = Some.apply,
                       alpha: Boolean = false,
                       noSort: Boolean = false,
                       postProcess: Seq[Bytes] => Seq[Bytes] = identity,
                       getterOverride: Option[Bytes => List[Option[A]]] = None)
                      (implicit format: Format, parse: Parse[A]) {

    private val patternHash = """(.*)\*(.*)->(.+)""".r
    private val patternString = """(.*)\*(.*)""".r

    private def getFromString(prefix: String, suffix: String)(b: Bytes): Option[Bytes] =
      currentDB.get(Key(prefix + b.newString + suffix)).map(_.as[StringValue].data)

    private def getFromHash(prefix: String, suffix: String, field: String)(b: Bytes): Option[Bytes] =
      currentDB.get(Key(prefix + b.newString + suffix)).flatMap(_.as[HashValue].data.get(Bytes(field)))

    private def sortByNumber(xs: Bytes): Double = Try(xs.parse[String]().toDouble)
      .getOrElse(throw new Exception("ERR One or more scores can't be converted into double"))

    // setters
    def setLookup(by: Option[String]): Sorter[A] = by match {
      case None => this
      case Some(patternHash(prefix, suffix, field)) => copy(lookup = getFromHash(prefix, suffix, field))
      case Some(patternString(prefix, suffix)) => copy(lookup = getFromString(prefix, suffix))
      case _ => copy(noSort = true) // nosort
    }

    def setAlpha(alpha: Boolean): Sorter[A] = copy(alpha = alpha)

    def setOrder(desc: Boolean): Sorter[A] = if (desc) copy(postProcess = _.reverse) else this

    def setLimit(limit: Option[(Int, Int)]): Sorter[A] = limit match {
      case Some((offset, count)) => copy(postProcess = postProcess andThen (_.slice(offset, offset + count)))
      case None => this
    }

    // getters
    private val getter: Bytes => List[Option[A]] = getterOverride.getOrElse(xs => List(defaultGetter(xs)))

    private def defaultGetter(xs: Bytes): Option[A] = Some(xs.parse(parse))

    private def noneGetter(xs: Bytes): Option[A] = None

    def setGetter(getList: List[String]): Sorter[A] =
      if (getList.isEmpty)
        this
      else
        copy(getterOverride = Some(xs => getList.map {
          case "#" => defaultGetter(xs)
          case patternHash(prefix, suffix, field) => getFromHash(prefix, suffix, field)(xs).map(_.parse(parse))
          case patternString(prefix, suffix) => getFromString(prefix, suffix)(xs).map(_.parse(parse))
          case _ => noneGetter(xs)
        }))

    // make it together
    private def sort = (noSort, alpha) match {
      case (true, _) => data
      case (false, true) => data.sortWith {
        // compare with Option[Byte]
        case (a, b) => (lookup(a), lookup(b)) match {
          case (None, None) => false
          case (None, Some(y)) => true
          case (Some(x), None) => false
          case (Some(x), Some(y)) => x < y
        }
      }
      case (false, false) => data.sortBy(lookup(_).map(sortByNumber))
    }

    private def get(xs: Seq[Bytes]): List[Option[A]] = xs.toList.flatMap(getter)

    def result: List[Option[A]] = sort |> postProcess |> get
  }

  private def getBytesSeq(v: Value): Seq[Bytes] = v match {
    case ys: ListValue => ys.data
    case ys: SetValue => ys.data.toSeq
    case ys: SortedSetValue => ys.data.toSeq.map(_._2)
    case _ => throw new Exception("WRONGTYPE Operation against a key holding the wrong kind of value")
  }

  /**
   * Returns or stores the elements contained in the list, set or sorted set at key. By default, sorting is numeric and
   * elements are compared by their value interpreted as double precision floating point number.
   *
   * @see http://redis.io/commands/sort
   */
  override def sort[A](key: String,
                       limit: Option[(Int, Int)] = None,
                       desc: Boolean = false,
                       alpha: Boolean = false,
                       by: Option[String] = None,
                       get: List[String] = Nil)
                      (implicit format: Format, parse: Parse[A]): Option[List[Option[A]]] = withDB {
    currentDB.get(Key(key)) map { v =>
      Sorter(getBytesSeq(v)).setLookup(by).setAlpha(alpha).setOrder(desc).setLimit(limit).setGetter(get).result
    } orElse Some(List.empty)
  }

  /**
   * SORT with STORE
   * sort keys in a set, and store result in the supplied key
   *
   * @see http://redis.io/commands/sort
   */
  override def sortNStore[A](key: String,
                             limit: Option[(Int, Int)] = None,
                             desc: Boolean = false,
                             alpha: Boolean = false,
                             by: Option[String] = None,
                             get: List[String] = Nil,
                             storeAt: String)(implicit format: Format, parse: Parse[A]): Option[Long] = withDB {
    val xs = sort(key, limit, desc, alpha, by, get).map(_.flatten).get
    currentDB.update(Key(storeAt), ListValue(xs.map(Bytes.apply)))
    Some(xs.length)
  }

  /**
   * Returns all keys matching pattern.
   *
   * While the time complexity for this operation is O(N), the constant times are fairly low. For example, Redis
   * running on an entry level laptop can scan a 1 million key database in 40 milliseconds.
   *
   * @see http://redis.io/commands/keys
   */
  override def keys[A](pattern: Any = "*")(implicit format: Format, parse: Parse[A]): Option[List[Option[A]]] = {
    val r = StringUtil.globToRegex(pattern.toString)
    currentDB.keys.view.map(_.k).filter(_.parse(Parse.parseStringSafe).matches(r))
      .map(_.parseOption(parse)).toList |> Some.apply
  }

  /**
   * RANDKEY (deprecated)
   */
  @deprecated("use randomkey", "2.8")
  override def randkey[A](implicit parse: Parse[A]): Option[A] = randomkey[A]

  /**
   * Return a random key from the currently selected database.
   *
   * @see http://redis.io/commands/randomkey
   */
  override def randomkey[A](implicit parse: Parse[A]): Option[A] =
    Some(currentDB.keys.toSeq).filter(_.nonEmpty).map(ks => ks(random.nextInt(ks.length)).k.parse(parse))

  /**
   * Renames key to newkey. It returns an error when the source and destination names are the same, or when key does not
   * exist. If newkey already exists it is overwritten, when this happens RENAME executes an implicit DEL operation, so
   * if the deleted key contains a very big value it may cause high latency even if RENAME itself is usually a
   * constant-time operation.
   *
   * @see http://redis.io/commands/rename
   */
  override def rename(oldkey: Any, newkey: Any)(implicit format: Format): Boolean =
    currentDB.renameKey(Key(oldkey), Key(newkey)) whenFalse {
      throw new RuntimeException("ERR no such key")
    }

  /**
   * Renames key to newkey if newkey does not yet exist. It returns an error under the same conditions as RENAME.
   *
   * @see http://redis.io/commands/renamenx
   */
  override def renamenx(oldkey: Any, newkey: Any)(implicit format: Format): Boolean = withDB {
    !exists(newkey) && rename(oldkey, newkey)
  }

  /**
   * Return the number of keys in the currently-selected database.
   *
   * @see http://redis.io/commands/dbsize
   */
  override def dbsize: Option[Long] = Some(currentDB.size)

  /**
   * Returns if key exists.
   *
   * @see http://redis.io/commands/exists
   */
  override def exists(key: Any)(implicit format: Format): Boolean = Key(key) |> currentDB.contains

  /**
   * Removes the specified keys. A key is ignored if it does not exist.
   *
   * @see http://redis.io/commands/del
   */
  override def del(key: Any, keys: Any*)(implicit format: Format): Option[Long] = {
    val oldSize = currentDB.size
    (key :: keys.toList) foreach {
      Key(_) |> currentDB.remove
    }
    Some(oldSize - currentDB.size)
  }

  /**
   * Returns the string representation of the type of the value stored at key. The different types that can be returned
   * are: string, list, set, zset and hash.
   *
   * @see http://redis.io/commands/type
   */
  override def getType(key: Any)(implicit format: Format): Option[String] =
    (Key(key) |> currentDB.get).map(_.typeName).getOrElse("none") |> Some.apply

  /**
   * Set a timeout on key. After the timeout has expired, the key will automatically be deleted. A key with an
   * associated timeout is often said to be volatile in Redis terminology.
   *
   * @see http://redis.io/commands/expire
   */
  override def expire(key: Any, ttl: Int)(implicit format: Format): Boolean = pexpire(key, ttl * 1000)

  /**
   * This command works exactly like EXPIRE but the time to live of the key is specified in milliseconds instead of
   * seconds.
   *
   * @see http://redis.io/commands/pexpire
   */
  override def pexpire(key: Any, ttlInMillis: Int)(implicit format: Format): Boolean =
    currentDB.updateTTL(Key(key), ttlInMillis)

  /**
   * EXPIREAT has the same effect and semantic as EXPIRE, but instead of specifying the number of seconds representing
   * the TTL (time to live), it takes an absolute Unix timestamp (seconds since January 1, 1970).
   *
   * @see http://redis.io/commands/expireat
   */
  override def expireat(key: Any, timestamp: Long)(implicit format: Format): Boolean = pexpireat(key, timestamp * 1000L)

  /**
   * PEXPIREAT has the same effect and semantic as EXPIREAT, but the Unix time at which the key will expire is specified
   * in milliseconds instead of seconds.
   *
   * @see http://redis.io/commands/pexpireat
   */
  override def pexpireat(key: Any, timestampInMillis: Long)(implicit format: Format): Boolean =
    currentDB.updateExpireAt(Key(key), timestampInMillis)

  /**
   * Returns the remaining time to live of a key that has a timeout. This introspection capability allows a Redis client
   * to check how many seconds a given key will continue to be part of the dataset.
   *
   * In Redis 2.6 or older the command returns -1 if the key does not exist or if the key exist but has no associated
   * expire.
   *
   * Starting with Redis 2.8 the return value in case of error changed:
   *
   * - The command returns -2 if the key does not exist.
   * - The command returns -1 if the key exists but has no associated expire.
   *
   * See also the PTTL command that returns the same information with milliseconds resolution (Only available in Redis
   * 2.6 or greater).
   *
   * @see http://redis.io/commands/ttl
   */
  override def ttl(key: Any)(implicit format: Format): Option[Long] =
    pttl(key).map(x => if (x < 0L) -1L else math.round(x / 1000.0))

  /**
   * Like TTL this command returns the remaining time to live of a key that has an expire set, with the sole difference
   * that TTL returns the amount of remaining time in seconds while PTTL returns it in milliseconds.
   *
   * In Redis 2.6 or older the command returns -1 if the key does not exist or if the key exist but has no associated
   * expire.
   *
   * Starting with Redis 2.8 the return value in case of error changed:
   *
   * - The command returns -2 if the key does not exist.
   * - The command returns -1 if the key exists but has no associated expire.
   *
   * @see http://redis.io/commands/pttl
   */
  override def pttl(key: Any)(implicit format: Format): Option[Long] = currentDB.getTTL(Key(key))

  /**
   * Select the DB with having the specified zero-based numeric index. New connections always use DB 0.
   *
   * @see http://redis.io/commands/select
   */
  override def select(index: Int): Boolean = (0 <= index) whenTrue (db = index)

  /**
   * Delete all the keys of the currently selected DB. This command never fails.
   *
   * The time-complexity for this operation is O(N), N being the number of keys in the database.
   *
   * @see http://redis.io/commands/flushdb
   */
  override def flushdb: Boolean = true whenTrue currentDB.clear()

  /**
   * Delete all the keys of all the existing databases, not just the currently selected one. This command never fails.
   *
   * The time-complexity for this operation is O(N), N being the number of keys in the database.
   *
   * @see http://redis.io/commands/flushall
   */
  override def flushall: Boolean = true whenTrue currentNode.clear()

  /**
   * Move key from the currently selected database (see SELECT) to the specified destination database. When key already
   * exists in the destination database, or it does not exist in the source database, it does nothing. It is possible to
   * use MOVE as a locking primitive because of this.
   *
   * @see http://redis.io/commands/move
   */
  override def move(key: Any, db: Int)(implicit format: Format): Boolean = if (this.db == db) {
    throw new Exception("ERR source and destination objects are the same")
  } else withDB {
    val k = Key(key)
    currentDB.get(k).map { v =>
      val dst = getDB(db)
      dst.synchronized {
        dst.update(k, v, None)
        currentDB.remove(k)
      }
    }.isDefined
  }

  /**
   * Ask the server to close the connection. The connection is closed as soon as all pending replies have been written
   * to the client.
   *
   * @see http://redis.io/commands/quit
   */
  override def quit: Boolean = disconnect

  /**
   * Request for authentication in a password-protected Redis server. Redis can be instructed to require a password
   * before allowing clients to execute commands. This is done using the requirepass directive in the configuration
   * file.
   *
   * If password matches the password in the configuration file, the server replies with the OK status code and starts
   * accepting commands. Otherwise, an error is returned and the clients needs to try a new password.
   *
   * @see http://redis.io/commands/auth
   */
  override def auth(secret: Any)(implicit format: Format): Boolean =
    throw new Exception("ERR Client sent AUTH, but no password is set") // always fails

  /**
   * Remove the existing timeout on key, turning the key from volatile (a key with an expire set) to persistent (a key
   * that will never expire as no timeout is associated).
   *
   * @see http://redis.io/commands/persist
   */
  override def persist(key: Any)(implicit format: Format): Boolean = currentDB.removeExpireAt(Key(key))

  /**
   * The SCAN command and the closely related commands SSCAN, HSCAN and ZSCAN are used in order to incrementally iterate
   * over a collection of elements.
   *
   * - SCAN iterates the set of keys in the currently selected Redis database.
   * - SSCAN iterates elements of Sets types.
   * - HSCAN iterates fields of Hash types and their associated values.
   * - ZSCAN iterates elements of Sorted Set types and their associated scores.
   *
   * Since these commands allow for incremental iteration, returning only a small number of elements per call, they can
   * be used in production without the downside of commands like KEYS or SMEMBERS that may block the server for a long
   * time (even several seconds) when called against big collections of keys or elements.
   *
   * @see http://redis.io/commands/scan
   */
  override def scan[A](cursor: Int, pattern: Any = "*", count: Int = 10)
                      (implicit format: Format, parse: Parse[A]): Option[(Option[Int], Option[List[Option[A]]])] =
    genericScan(currentDB.keys.map(_.k), cursor, pattern, count)

}
