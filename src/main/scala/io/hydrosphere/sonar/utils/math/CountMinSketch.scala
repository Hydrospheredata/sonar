package io.hydrosphere.sonar.utils.math

import net.openhft.hashing.LongHashFunction

import scala.collection.mutable

case class MutableCountMinSketch(size: Int, buckets: mutable.Map[Int, Long]) {
  private val width = 1 << size
  private val mask = width - 1

  def add(value: Double): Unit = {
    var k = (-value * 1e3).toLong ^ Long.MaxValue
    while (k != 0) {
      buckets(hash(k, 0)) += 1
      buckets(width + hash(k, 1)) += 1
      k += k & -k
    }
  }

  def toCountMinSketch: CountMinSketch = new CountMinSketch(size, buckets.toMap)

  private def hash(value: Long, seed: Int): Int = {
    (LongHashFunction.xx(seed).hashLong(value) & mask).toInt
  }
}

object MutableCountMinSketch {
  def apply(size: Int): MutableCountMinSketch = new MutableCountMinSketch(size, mutable.Map.empty[Int, Long].withDefaultValue(0L))
}

case class CountMinSketch(size: Int, buckets: Map[Int, Long]) {
  private val width = 1 << size
  private val mask = width - 1

  def add(value: Double): CountMinSketch = {
    var k = (-value * 1e3).toLong ^ Long.MaxValue
    val newBucket = mutable.Map.empty[Int, Long].withDefaultValue(0L)
    while (k != 0) {
      newBucket(hash(k, 0)) += 1
      newBucket(width + hash(k, 1)) += 1
      k += k & -k
    }
    new CountMinSketch(size, (buckets.toSeq ++ newBucket.toSeq).groupBy(_._1).mapValues(_.map(_._2).sum))
  }

  def quantile(value: Double, total: Long): Double = {
    val position = value * total
    var begin = 0L
    var count = -1L

    while (count != 0) {
      val step: Long = count >>> 1
      val it: Long = begin + step
      if (getSumFor(it, total) < position) {
        begin = it + 1
        count -= step + 1
      } else {
        count = step
      }
    }

    (-begin ^ Long.MaxValue) / 1000d
  }
  
  private def getSumFor(k: Long, total: Long): Double = {
    var sum = 0d
    var idx = k
    while (idx != 0) {
      sum += math.min(buckets.getOrElse(hash(idx, 0), 0L), buckets.getOrElse(width + hash(idx, 1), 0L)).toDouble - Math.E / width * total / 2
      idx -= idx & -idx
    }
    sum
  }

  def hash(value: Long, seed: Int): Int = {
    (LongHashFunction.xx(seed).hashLong(value) & mask).toInt
  }
}

object CountMinSketch {
  def apply(size: Int): CountMinSketch = new CountMinSketch(size, Map.empty[Int, Long])
}
