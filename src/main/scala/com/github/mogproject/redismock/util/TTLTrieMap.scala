package com.github.mogproject.redismock.util

import scala.collection.concurrent.TrieMap
import com.github.mogproject.redismock.util.ops._

/**
 * Thread-safe key-value store with the time-to-live attribute
 */
class TTLTrieMap[K, V] {
  private[util] final val store = TrieMap.empty[K, V]
  private[util] final val expireAt = TrieMap.empty[K, Long]

  private[this] def now(): Long = System.currentTimeMillis()

  /**
   * update key/value without changing ttl
   *
   * @param key key
   * @param value value
   */
  def update(key: K, value: V): Unit = {
    store.update(key, value)
  }

  /**
   * update key/value with ttl
   *
   * @param key key
   * @param value value
   * @param ttl time to live in millis (None means infinite ttl)
   */
  def update(key: K, value: V, ttl: Option[Long]): Unit = {
    store.update(key, value)
    ttl match {
      case Some(t) => expireAt.update(key, now() + t)
      case None => expireAt.remove(key)
    }
  }

  /**
   * update datetime to be expired for the specified key
   * @param key key
   * @param timestamp timestamp to be expired in millis
   * @return true if the key exists
   */
  def updateExpireAt(key: K, timestamp: Long): Boolean = withTransaction { t =>
    t.contains(key) whenTrue t.expireAt.update(key, timestamp)
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
  def get(key: K): Option[V] = {
    if (expireAt.get(key).exists(_ <= now())) {
      remove(key)
      None
    } else {
      store.get(key)
    }
  }

  /**
   * get the datetime to be expired in millis
   *
   * @param key key
   * @return if the key exists and the ttl is not set, then return Some(-1)
   */
  def getExpireAt(key: K): Option[Long] =
    withTruncate(){ if (contains(key)) Some(expireAt.getOrElse(key, -1L)) else None }

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
    get(oldKey) match {
      case Some(v) =>
        store.update(newKey, v)
        expireAt.get(oldKey) foreach { t =>
          expireAt.update(newKey, t)
          expireAt.remove(oldKey)
        }
        store.remove(oldKey)
        true
      case None =>
        false
    }
  }

  /**
   * get the lazy list of all keys
   */
  def keys: Iterable[K] = store.keys

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
  def removeExpireAt(key: K): Boolean = withTruncate(){
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

  def withTransaction[A](thunk: TTLTrieMap[K, V] => A): A = synchronized(thunk(this))

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
