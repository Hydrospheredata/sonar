package io.hydrosphere.sonar.common.utils.math

import cats.data.NonEmptyList

case class KolmogorovSmirnovTestResult(value: Double, rejectionLevel: Double)

object KolmogorovSmirnovTest {

  def test(sampleOne: NonEmptyList[Double], sampleTwo: NonEmptyList[Double]): KolmogorovSmirnovTestResult = {
    val combined = (sampleOne.map(x => (1, x)) ++ sampleTwo.toList.map(x => (2, x))).toList sortWith ((a, b) => a._2 < b._2)

    var counter = 0.0
    val cdfOne = combined.map { case (key, _) => if (key equals 1) counter += 1; counter / sampleOne.size }
    counter = 0.0
    val cdfTwo = combined.map { case (key, _) => if (key equals 2) counter += 1; counter / sampleTwo.size }

    val D = cdfOne zip cdfTwo map (x => Math.abs(x._1 - x._2)) max

    val num: Double = sampleOne.size + sampleTwo.size
    val den: Double = sampleOne.size * sampleTwo.size

    val rejectionLevel = 1.36 * Math.sqrt(num / den)
    KolmogorovSmirnovTestResult(D, rejectionLevel)
  }
  
}
