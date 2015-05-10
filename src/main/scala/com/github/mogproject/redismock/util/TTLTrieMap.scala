package com.github.mogproject.redismock.util

import scala.collection.concurrent.TrieMap
import com.github.mogproject.redismock.util.ops._

/**
 * Thread-safe key-value store with the time-to-live attribute
 */
class TTLTrieMap[K, V] {
  private[util] final val store = TrieMap.empty[K, V]
  private[util] final val expireAt = TrieMap.empty[K, Long]
  private val neverExpire = -1L

  private[this] def now(): Long = System.currentTimeMillis()

  /**
   * update key/value without changing ttl
   *
   * @param key key
   * @param value value
   */
  def update(key: K, value: V): Unit = store.update(key, value)

  /**
   * update key/value with ttl
   *
   * @param key key
   * @param value value
   * @param ttl time to live in millis (None means infinite ttl)
   */
  def update(key: K, value: V, ttl: Option[Long]): Unit = update(key, value, ttl.map(now() + _).getOrElse(neverExpire))

  /**
   * update key/value with datetime to be expired
   *
   * @param key key
   * @param value value
   * @param timestamp timestamp to be expired in millis (-1: never expired)
   */
  def update(key: K, value: V, timestamp: Long): Unit = synchronized {
    store.update(key, value)
    if (timestamp == neverExpire) expireAt.remove(key) else expireAt.update(key, timestamp)
  }

  /**
   * update datetime to be expired for the specified key
   * @param key key
   * @param timestamp timestamp to be expired in millis
   * @return true if the key exists
   */
  def updateExpireAt(key: K, timestamp: Long): Boolean = synchronized {
    contains(key) whenTrue expireAt.update(key, timestamp)
  }

  /**
   * update key's ttl
   *
   * @param key key
   * @param ttl time to live in millis
   * @return true if the key exists
   */
  def updateTTL(key: K, ttl: Long): Boolean = updateExpireAt(key, now() + ttl)

  /**
   * get the current value from the key
   *
   * @param key key
   * @return return None if the key doesn't exist or has expired,
   *         otherwise return the value wrapped with Some
   */
  def get(key: K): Option[V] = withTruncate() {store.get(key)}

  /**
   * get the current value and the datetime to be expired in millis
   *
   * @param key key
   * @return return None if the key doesn't exist or has expired,
   *         otherwise return 2-tuple of the value and the expired-at (-1: never expired) wrapped with Some
   */
  def getWithExpireAt(key: K): Option[(V, Long)] =
    withTruncate() {store.get(key).map { v => (v, expireAt.getOrElse(key, neverExpire))}}

  /**
   * get the datetime to be expired in millis
   *
   * @param key key
   * @return if the key exists and the ttl is not set, then return Some(-1)
   */
  def getExpireAt(key: K): Option[Long] = getWithExpireAt(key).map(_._2)

  /**
   * get time to live
   *
   * @param key key
   * @return time to live in millis wrapped by Option
   */
  def getTTL(key: K): Option[Long] = getExpireAt(key).map(_ - now())

  /**
   * rename the key
   *
   * @param oldKey old key
   * @param newKey new key
   * @return true when the old key exists
   */
  def renameKey(oldKey: K, newKey: K): Boolean = synchronized {
    (getWithExpireAt(oldKey) map { case (v, t) =>
      update(newKey, v, t)
      remove(oldKey)
    }).isDefined
  }

  /**
   * get the lazy list of all keys
   */
  def keys: Iterable[K] = withTruncate()(store.keys)

  /**
   * clear all the keys/values
   */
  def clear(): Unit = synchronized {
    store.clear()
    expireAt.clear()
  }

  /**
   * get numbers of living keys
   */
  def size: Int = withTruncate()(store.size)

  /**
   * check if the key exists and is alive
   * @param key key
   */
  def contains(key: K): Boolean = withTruncate()(store.contains(key))

  /**
   * remove specified key with value
   * @param key key
   */
  def remove(key: K): Unit = synchronized {
    store.remove(key)
    expireAt.remove(key)
  }

  /**
   * remove the timestamp to be expired
   * This means the key will be alive forever.
   *
   * @param key key
   * @return true if the key exists
   */
  def removeExpireAt(key: K): Boolean = withTruncate() {
    expireAt.remove(key)
    contains(key)
  }

  private def truncate(time: Long): Unit = {
    expireAt.foreach {
      case (k, t) if t <= time => remove(k)
      case _ =>
    }
  }

  private[util] def withTruncate[A](time: Long = now())(f: => A): A = synchronized {
    truncate(time)
    f
  }

  // Note: Because of the type erasure, it could happen to return true with different Key/Value types.
  override def equals(other: Any): Boolean = other match {
    case that: TTLTrieMap[K, V] => withTruncate()(this.store == that.store && this.expireAt == that.expireAt)
    case _ => false
  }

  override def hashCode: Int = store.hashCode * 41 + expireAt.hashCode
}

object TTLTrieMap {
  /**
   * generate new empty TTLTrieMap
   */
  def empty[K, V]: TTLTrieMap[K, V] = new TTLTrieMap[K, V]
}
