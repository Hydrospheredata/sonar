package io.hydrosphere.sonar.common.terms.statistics

import io.hydrosphere.sonar.common.terms.NumericalPreprocessedProfile

case class QuantileStatistics(
  min: Double, 
  max: Double, 
  median: Double, 
  percentile5: Double, 
  percentile95: Double, 
  q1: Double, 
  q3: Double, 
  range: Double, 
  interquartileRange: Double
)

object QuantileStatistics {
  def apply(pp: NumericalPreprocessedProfile): QuantileStatistics = {
    val definedCount = pp.size - pp.missing

    def quantile(v: Double): Double = pp.countMinSketch.quantile(v, definedCount)

    val median = quantile(0.5)
    val percentile5 = quantile(0.05)
    val percentile95 = quantile(0.95)

    val q1 = quantile(0.25)
    val q3 = quantile(0.75)

    val range = pp.max - pp.min
    val interquartileRange = q3 - q1

    new QuantileStatistics(pp.min, pp.max, median, percentile5, percentile95, q1, q3, range, interquartileRange)
  }
}