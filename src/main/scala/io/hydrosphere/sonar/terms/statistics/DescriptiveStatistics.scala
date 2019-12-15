package io.hydrosphere.sonar.terms.statistics

import io.hydrosphere.sonar.terms.NumericalPreprocessedProfile

case class DescriptiveStatistics(
  standardDeviation: Double, 
  variationCoef: Double, 
  kurtosis: Double, 
  mean: Double, 
  skewness: Double, 
  variance: Double
)

object DescriptiveStatistics {
  def apply(pp: NumericalPreprocessedProfile, median: Double): DescriptiveStatistics = {
    val variance = if (pp.size != 0 && pp.size != 1) {
      ((pp.squaresSum - pp.sum * pp.sum / pp.size) / (pp.size - 1)).toDouble
    } else {
      Double.NaN
    }
    val stddev = math.sqrt(variance)

    val mean = (pp.sum / pp.size).toDouble

    val variationCoef = (stddev / mean) * 100

    val kurtosis = if (pp.squaresSum != 0) {
      ((pp.fourthPowersSum / pp.size) / ((pp.squaresSum / pp.size) * (pp.squaresSum / pp.size)) - 3).toDouble
    } else {
      Double.NaN
    }

    val skewness = 3 * (mean - median) / stddev

    new DescriptiveStatistics(stddev, variationCoef, kurtosis, mean, skewness, variance)
  }
}