package com.github.mogproject.redismock.storage

import com.github.mogproject.redismock.entity.{Key, Value}
import com.github.mogproject.redismock.util.TTLTrieMap
import com.redis.Redis

import scala.collection.concurrent.TrieMap

trait Storage {
  self: Redis =>

  type Database = TTLTrieMap[Key, Value]

  type Node = TrieMap[Int, Database]

  private[this] lazy val index = TrieMap.empty[(String, Int), Node]

  def currentDB: Database = currentNode.getOrElseUpdate(db, new Database)

  def currentNode: Node = index.getOrElseUpdate((host, port), new Node)
}
