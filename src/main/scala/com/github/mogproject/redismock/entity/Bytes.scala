package com.github.mogproject.redismock.entity

import com.redis.serialization.{Format, Parse}
import com.redis.serialization.Parse.parseDefault
import scala.collection.immutable.VectorBuilder
import scala.collection.mutable
import scala.util.Try


/**
 * data should be stored as vector to compare accurately
 * @param value value binary
 */
case class Bytes(value: Vector[Byte])
  extends scala.collection.AbstractSeq[Byte]
  with scala.collection.immutable.IndexedSeq[Byte]
  with scala.collection.TraversableLike[Byte, Bytes]
  with scala.collection.IndexedSeqLike[Byte, Bytes]
  with scala.Serializable {

  override def newBuilder: mutable.Builder[Byte, Bytes] = new BytesBuilder

  override val length: Int = value.length

  def parse[A](parse: Parse[A] = parseDefault): A = parse(value.toArray)

  def parseOption[A](parse: Parse[A] = parseDefault): Option[A] = Try(this.parse(parse)).toOption

  def ++(bs: Bytes): Bytes = Bytes(value ++ bs.value)

  def ++(bs: Traversable[Byte]): Bytes = Bytes(value ++ bs)

  /** get n-th element */
  def apply(n: Int): Byte = value(n)

  /** get n-th element safely */
  def get(index: Int): Option[Byte] = Try(value(index)).toOption

  def getOrElse(index: Int, default: => Byte = 0.toByte): Byte = get(index).getOrElse(default)

  def updated(index: Int, elem: Byte, padding: Byte): Bytes = {
    if (index >= length) {
      // padding with zeros
      new Bytes(value ++ Vector.fill(length - index)(padding) ++ Vector(elem))
    } else {
      new Bytes(value.updated(index, elem))
    }
  }

  /** Return n-length Bytes padding with elem */
  def resized(n: Int, elem: => Byte = 0.toByte): Bytes = take(n) ++ Bytes.fill(n - length)(elem)

  override def equals(other: Any): Boolean = canEqual(other) && (other match {
    case that: Bytes => this.value == that.value
    case _ => false
  })

  override def hashCode: Int = value.hashCode

  def newString = new String(value.toArray)
}

/**
 * Companion class to the Bytes class
 */
object Bytes {

  def apply(v: Traversable[Byte]): Bytes = new Bytes(v.toVector)

  def apply(xs: Int*): Bytes = new Bytes(xs.map(_.toByte).toVector)

  def apply(v: Any)(implicit format: Format): Bytes = new Bytes(format(v).toVector)

  def fill(n: Int)(elem: => Byte): Bytes = Bytes(Vector.fill(n)(elem))

  lazy val empty = Bytes(Vector.empty[Byte])
}


final class BytesBuilder() extends mutable.Builder[Byte, Bytes] {

  private val builder = new VectorBuilder[Byte]

  def +=(elem: Byte): this.type = {
    builder += elem
    this
  }

  override def ++=(xs: TraversableOnce[Byte]): this.type =
    super.++=(xs)

  def result: Bytes = new Bytes(builder.result)

  def clear(): Unit = builder.clear()
}