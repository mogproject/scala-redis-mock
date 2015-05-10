package com.github.mogproject.redismock

import com.redis.{Redis, NodeOperations}


trait MockNodeOperations extends NodeOperations {
  self: Redis =>

  // SAVE
  // save the DB on disk now.
  override def save: Boolean =
    throw new UnsupportedOperationException("server operation is not supported")

  // BGSAVE
  // save the DB in the background.
  override def bgsave: Boolean =
    throw new UnsupportedOperationException("server operation is not supported")

  // LASTSAVE
  // return the UNIX TIME of the last DB SAVE executed with success.
  override def lastsave: Option[Long] =
    throw new UnsupportedOperationException("server operation is not supported")

  // SHUTDOWN
  // Stop all the clients, save the DB, then quit the server.
  override def shutdown: Boolean =
    throw new UnsupportedOperationException("server operation is not supported")

  // BGREWRITEAOF
  override def bgrewriteaof: Boolean =
    throw new UnsupportedOperationException("server operation is not supported")

  // INFO
  // the info command returns different information and statistics about the server.
  override def info =
    throw new UnsupportedOperationException("server operation is not supported")

  // MONITOR
  // is a debugging command that outputs the whole sequence of commands received by the Redis server.
  override def monitor: Boolean =
    throw new UnsupportedOperationException("server operation is not supported")

  // SLAVEOF
  // The SLAVEOF command can change the replication settings of a slave on the fly.
  override def slaveof(options: Any): Boolean =
    throw new UnsupportedOperationException("server operation is not supported")

  @deprecated("use slaveof", "1.2.0") override def slaveOf(options: Any): Boolean =
    throw new UnsupportedOperationException("server operation is not supported")

}

