package com.github.mogproject.redismock

import java.io._

import com.redis.IO


trait MockIO extends IO {
  private[this] var isSocketBounded: Boolean = false

  override def connected = isSocketBounded

  // Connects the socket, and sets the input and output streams.
  override def connect: Boolean = {
    if (isSocketBounded) {
      isSocketBounded = false
      throw new RuntimeException("socket is already used")
    } else {
      isSocketBounded = true
      true
    }
  }

  // Disconnects the socket.
  override def disconnect: Boolean = {
    isSocketBounded = false
    true
  }

  // Wrapper for the socket write operation.
  override def write_to_socket(data: Array[Byte])(op: OutputStream => Unit) =
    throw new UnsupportedOperationException("mock should work without I/O")

  // Writes data to a socket using the specified block.
  override def write(data: Array[Byte]) =
    throw new UnsupportedOperationException("mock should work without I/O")

  override def readLine: Array[Byte] =
    throw new UnsupportedOperationException("mock should work without I/O")

  override def readCounted(count: Int): Array[Byte] =
    throw new UnsupportedOperationException("mock should work without I/O")
}
