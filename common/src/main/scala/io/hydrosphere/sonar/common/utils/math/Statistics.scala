package io.hydrosphere.sonar.common.utils.math

import breeze.stats.distributions._
import cats.data.NonEmptyList
import enumeratum._
import eu.timepit.refined._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric._

import scala.collection.immutable

object Statistics {
  
  sealed trait Distribution extends EnumEntry
  object Distribution extends Enum[Distribution] {
    val values: immutable.IndexedSeq[Distribution] = findValues
    
    case object Normal extends Distribution
    case object Exponential extends Distribution
    case object Laplace extends Distribution
    case object Poisson extends Distribution
  }
  
  /* this is safe because of refined type of size */
  private def nonEmpty(seq: Seq[Double]): NonEmptyList[Double] = NonEmptyList(seq.head, seq.tail.toList) 
  
  def generateDistribution(distribution: Distribution, size: Int Refined Positive, mu: Double = 0d, sigma: Double = 1d, rate: Double = 2d): NonEmptyList[Double] = distribution match {
    case Distribution.Normal => nonEmpty(new Gaussian(mu = mu, sigma = sigma).sample(size))
    case Distribution.Exponential => nonEmpty(new Exponential(rate).sample(size))
    case Distribution.Laplace => nonEmpty(Laplace(location = mu, scale = sigma).sample(size))
    case Distribution.Poisson => nonEmpty(new Poisson(mean = mu).sample(size).map(_.toDouble))
  }
}
