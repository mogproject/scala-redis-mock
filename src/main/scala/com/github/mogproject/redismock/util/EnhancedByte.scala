package com.github.mogproject.redismock.util


/**
 * Wrapper for Byte
 *
 * @param x base Byte
 */
class EnhancedByte(x: Byte) {

  /** Convert to unsigned integer */
  def toUnsignedInt: Int = (x.toInt + 256) % 256

  /** Count the number of 1-bit */
  def popCount: Int = {
    val s = (x & 0x11) + ((x >> 1) & 0x11) + ((x >> 2) & 0x11) + ((x >> 3) & 0x11)
    (s & 15) + (s >> 4)
  }
}
