package io.hydrosphere.sonar.utils.math

import java.nio.ByteBuffer

import net.openhft.hashing.LongHashFunction

import scala.collection.mutable

abstract class HyperLogLogBase {
  def countZeros(lng: Long): Int = {
    var count = 1
    var shifted = lng
    for (_ <- Range(0, 64)) {
      if ((shifted & 0x1) == 1) {
        return count
      }
      count += 1
      shifted = shifted >> 1
    }
    count
  }
}

case class MutableHyperLogLog(size: Int, buckets: mutable.Map[Int, Int]) extends HyperLogLogBase {
  private def addHashed(hashed: Long): Unit = {
    val bucket = (hashed >>> (64 - size)).toInt
    val trailingZeros = countZeros(hashed)
    val oldValue = buckets.getOrElse(bucket, 0)
    val newValue = math.max(trailingZeros, oldValue)
    buckets.update(bucket, newValue.toInt)
  }
  
  def add(value: Double): Unit = {
    val hashed: Long = LongHashFunction.xx().hashBytes(ByteBuffer.allocate(8).putDouble(value).array())
    addHashed(hashed)
  }
  
  def add(value: String): Unit = {
    val hashed: Long = LongHashFunction.xx().hashChars(value)
    addHashed(hashed)
  }

  def toHyperLogLog: HyperLogLog = new HyperLogLog(size, buckets.toMap)
}

object MutableHyperLogLog {
  def apply(size: Int): MutableHyperLogLog = new MutableHyperLogLog(size, mutable.Map.empty[Int, Int])
}

case class HyperLogLog(size: Int, buckets: Map[Int, Int]) extends HyperLogLogBase {

  private val bucketCount: Long = math.pow(2, size).toLong

  private val alpha: Double = 0.7213 / (1 + 1.079 / bucketCount) * math.pow(bucketCount, 2)
  
  private def addHashed(hashed: Long): HyperLogLog = {
    val bucket = (hashed >>> (64 - size)).toInt
    val trailingZeros = countZeros(hashed)
    val oldValue = buckets.getOrElse(bucket, 0)
    val newValue = math.max(trailingZeros, oldValue)
    val newBuckets = buckets.updated(bucket, newValue.toInt)
    new HyperLogLog(size, newBuckets)
  }

  def add(value: Double): HyperLogLog = {
    val hashed: Long = LongHashFunction.xx().hashBytes(ByteBuffer.allocate(8).putDouble(value).array())
    addHashed(hashed)
  }
  
  def add(value: String): HyperLogLog = {
    val hashed: Long = LongHashFunction.xx().hashChars(value)
    addHashed(hashed)
  }
  
  def count: Long = {
    var newCountZeroBuckets = 0
    var newSumOfInverses = 0d

    for (i <- Range(0, bucketCount.toInt)) {
      val value = buckets.getOrElse(i, 0)
      if (value == 0) {
        newCountZeroBuckets += 1
      }
      newSumOfInverses += math.pow(2, -value)
    }
    var estimate: Double = alpha / newSumOfInverses
    if (newCountZeroBuckets > 0 && estimate < 2.5d * bucketCount) {
      estimate = bucketCount.toDouble * math.log(bucketCount.toDouble / newCountZeroBuckets.toDouble)
    }

    math.round(estimate)
  }

  def error: Double = {
    1.04d / math.sqrt(bucketCount)
  }
}

object HyperLogLog {
  def apply(size: Int): HyperLogLog = {

    val buckets = Map.empty[Int, Int].withDefaultValue(0)
    new HyperLogLog(size, buckets)
  }
}
