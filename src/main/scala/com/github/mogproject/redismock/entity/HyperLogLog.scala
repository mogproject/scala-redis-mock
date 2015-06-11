package com.github.mogproject.redismock.entity

import com.github.mogproject.redismock.util.hashing.MurmurHash
import com.github.mogproject.redismock.util.Bytes

import scala.annotation.tailrec
import scala.util.Try


case class HyperLogLog(isDense: Boolean = false,
                       registers: Seq[Int] = Seq.fill(HyperLogLog.m)(0),
                       cache: Long = 0x8000000000000000L) {

  import HyperLogLog.{m, rankTable}

  lazy val header: Bytes = Bytes('H', 'Y', 'L', 'L') ++
    Bytes(if (isDense) 0 else 1) ++ Bytes(0, 0, 0) ++ Bytes(Seq.tabulate(8)(i => (cache >>> (8 * i) & 0xff).toByte))

  lazy val numZeros = registers.count(_ == 0)

  /**
   * @param value element to add
   * @return tuple of (new HyperLogLog, is updated)
   */
  def add(value: Bytes): (HyperLogLog, Boolean) = {
    val hashed = MurmurHash.murmurHash64A(value.toArray)
    val index = (hashed & ((1L << 14) - 1)).toInt
    val rank = (14 until 64).find(i => ((hashed >>> i) & 1L) != 0L).getOrElse(63) - 13

    if (rank <= registers(index)) {
      (this, false)
    } else {
      val toDense = isDense || rank > 32 || sparseBytes.length > 3000
      (HyperLogLog(toDense, registers.updated(index, math.max(registers(index), rank))), true)
    }
  }

  def count: (HyperLogLog, Long) = {
    val alpha = 0.7213 / (1 + 1.079 / m)
    val c = (1 / registers.map(rankTable).sum * alpha * alpha match {
      case e if e < 2.5 * m && numZeros != 0 => m * math.log(m.toDouble / numZeros)
      case e if m == 16384 && e < 72000 =>
        val bias = 5.9119 * 1.0e-18 * (e * e * e * e) - 1.4253 * 1.0e-12 * (e * e * e) + 1.2940 * 1.0e-7 * (e * e) -
          5.2921 * 1.0e-3 * e + 83.3216
        e - e * (bias / 100)
      case e => e
    }).toLong
    (copy(cache = c), c)
  }

  def merge(that: HyperLogLog): HyperLogLog = {
    copy(
      isDense = true,
      registers = (registers zip that.registers).map { case (a: Int, b: Int) => math.max(a, b)}
    )
  }

  def toBytes: Bytes = header ++ (if (isDense) denseBytes else sparseBytes)

  private lazy val sparseBytes: Bytes = {
    import HyperLogLog.SparseEncoding._

    @tailrec
    def f(sofar: Bytes, xs: List[Int], storedVal: Int, storedLen: Int): Bytes = xs match {
      case _ if storedLen == 4 && storedVal != 0 => f(sofar ++ encodeVal(storedVal, 4), xs, storedVal, 0)
      case x :: ys if x == storedVal => f(sofar, ys, storedVal, storedLen + 1)
      case _ if storedLen != 0 && storedVal == 0 => f(sofar ++ encodeZero(storedLen), xs, -1, 0)
      case _ if storedLen != 0 => f(sofar ++ encodeVal(storedVal, storedLen), xs, -1, 0)
      case x :: ys => f(sofar, ys, x, 1)
      case Nil => sofar
    }
    f(Bytes(), registers.toList, -1, 0)
  }

  private lazy val denseBytes: Bytes = {
    Bytes(Seq.tabulate(m * 3 / 4){
      i => ((i % 3, i / 3 * 4) match {
        case (0, j) => registers(j) | ((registers(j + 1) & 0x03) << 6)
        case (1, j) => (registers(j + 1) >>> 2) | ((registers(j + 2) & 0x0f) << 4)
        case (2, j) => (registers(j + 2) >>> 4) | (registers(j + 3) << 2)
      }).toByte
    })
  }

}

object HyperLogLog {

  val m = 1 << 14

  val rankTable = Seq.iterate(1.0, 64)(_ / 2)

  /**
   * Create HyperLogLog from raw bytes
   *
   * @param xs bytes stored as a STRING value
   * @return HyperLogLog instance
   */
  def fromBytes(xs: Bytes): HyperLogLog = {
    val (header, registers) = (xs.take(16), xs.drop(16))
    (for {
      isDense <- parseHeader(header)
      hll <- if (isDense) parseDenseBytes(registers) else parseSparseBytes(registers)
    } yield {
      hll
    }).getOrElse(throw new Exception("WRONGTYPE Key is not a valid HyperLogLog string value."))
  }

  /**
   * Parse header bytes
   *
   * @param header header bytes
   * @return is dense representation
   */
  private def parseHeader(header: Bytes): Try[Boolean] = Try {
    // verification
    require(header.length == 16)
    require(header.take(4) == Bytes('H', 'Y', 'L', 'L'))
    require((0 to 1).contains(header(4)))
    require(header.slice(5, 8).forall(_ == 0))

    header(4) == 0
  }

  /**
   * Parse sparse representation
   * @param registerBytes register bytes
   * @return HyperLogLog instance wrapped by Try
   */
  def parseSparseBytes(registerBytes: Bytes): Try[HyperLogLog] = Try {
    @tailrec
    def f(sofar: Seq[Int], xs: List[Byte]): Seq[Int] = xs match {
      case x :: ys if (x & 0xc0) == 0 => f(sofar ++ SparseEncoding.decodeZero(x), ys) // ZERO
      case x :: y :: ys if (x & 0xc0) == 0x40 => f(sofar ++ SparseEncoding.decodeZero(x, y), ys) // XZERO
      case x :: ys if (x & 0x80) == 0x80 => f(sofar ++ SparseEncoding.decodeVal(x), ys) // VAL
      case Nil => sofar
    }

    val registers = f(Seq.empty, registerBytes.toList)
    assert(registers.length == m, s"registers.length: ${registers.length} is not m: ${m}, registerBytes: ${registerBytes}")
    HyperLogLog(isDense = false, registers)
  }

  /**
   * Parse dense representation
   * @param registerBytes register bytes
   * @return HyperLogLog instance wrapped by Try
   */
  private def parseDenseBytes(registerBytes: Bytes): Try[HyperLogLog] = Try {
    val registers = Seq.tabulate(m) {
      i => (i % 4, i / 4 * 3) match {
        case (0, j) => registerBytes(j) & 0x3f
        case (1, j) => (registerBytes(j) >>> 6) | ((registerBytes(j + 1) & 0x0f) << 2)
        case (2, j) => (registerBytes(j + 1) >>> 4) | ((registerBytes(j + 2) & 0x03) << 4)
        case (3, j) => registerBytes(j + 2) >>> 2
      }
    }
    HyperLogLog(isDense = true, registers)
  }

  object SparseEncoding {
    def encodeZero(len: Int): Bytes = {
      require(0 < len && len <= (1 << 14))

      if (len <= (1 << 6))
        Bytes(Seq((len - 1).toByte))
      else
        Bytes(Seq((((len - 1) >>> 8) | 0x40).toByte, ((len - 1) & 0xff).toByte))
    }

    def encodeVal(value: Int, len: Int): Bytes = {
      require(0 < value && value <= (1 << 5))
      require(0 < value && len <= 4)

      Bytes(Seq((0x80 | (value - 1) << 2 | (len - 1)).toByte))
    }

    def decodeZero(x: Byte): Seq[Int] = Seq.fill(1 + (x & 0x3f))(0)

    def decodeZero(x: Byte, y: Byte): Seq[Int] = Seq.fill(1 + (((x & 0x3f) << 8) | (y & 0xff)))(0)

    def decodeVal(x: Byte): Seq[Int] = Seq.fill(1 + (x & 0x03))(1 + ((x & 0x7c) >>> 2))
  }
}
