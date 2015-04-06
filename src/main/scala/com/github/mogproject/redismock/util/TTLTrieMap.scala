package com.github.mogproject.redismock.util

import scala.collection.concurrent.TrieMap

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
   * clear all the keys/values
   */
  def clear(): Unit = {
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
  def remove(key: K): Unit = {
    store.remove(key)
    expireAt.remove(key)
  }

  private def truncate(time: Long): Unit = {
    expireAt.foreach {
      case (k, t) if t <= time => remove(k)
      case _ =>
    }
  }

  private[util] def withTruncate[A](time: Long = now())(f: => A): A = { truncate(time); f }


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
