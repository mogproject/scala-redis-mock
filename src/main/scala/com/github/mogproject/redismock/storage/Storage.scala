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

  /**
   * get specified database in the current node
   */
  def getDB(db: Int): Database = {
    require(db >= 0)
    currentNode.getOrElseUpdate(db, new Database)
  }

  /**
   * syntax sugar for executing atomic tasks with current DB
   * @param thunk atomic tasks
   */
  def withDB[A](thunk: => A): A = { currentDB.synchronized(thunk) }
}

object Storage {
  type Database = TTLTrieMap[Key, Value]
  type Node = TrieMap[Int, Database]

  lazy val index = TrieMap.empty[(String, Int), Node]

}