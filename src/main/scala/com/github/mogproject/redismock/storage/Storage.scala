package com.github.mogproject.redismock.storage

import com.github.mogproject.redismock.entity.{Key, Value}
import com.github.mogproject.redismock.util.TTLTrieMap
import com.redis.Redis

import scala.collection.concurrent.TrieMap

trait Storage {
  self: Redis =>

  import Storage.{Database, Node}


  def currentDB: Database = currentNode.getOrElseUpdate(db, new Database)

  def currentNode: Node = Storage.index.getOrElseUpdate((host, port), new Node)
}

object Storage {
  type Database = TTLTrieMap[Key, Value]
  type Node = TrieMap[Int, Database]

  lazy val index = TrieMap.empty[(String, Int), Node]

}