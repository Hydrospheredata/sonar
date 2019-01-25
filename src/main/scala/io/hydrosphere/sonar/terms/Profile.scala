package io.hydrosphere.sonar.terms

import io.hydrosphere.sonar.terms.statistics.{CommonStatistics, DescriptiveStatistics, Histogram, QuantileStatistics}

sealed trait Profile

case class NumericalProfile(
   name: String,
   modelVersionId: Long,
   commonStatistics: CommonStatistics,
   quantileStatistics: QuantileStatistics,
   descriptiveStatistics: DescriptiveStatistics,
   histogram: Histogram
) extends Profile

object NumericalProfile {
  def apply(pp: NumericalPreprocessedProfile): NumericalProfile = {
    val quantileStatistics = QuantileStatistics(pp)
    new NumericalProfile(
      pp.name,
      pp.modelVersionId,
      CommonStatistics(pp),
      quantileStatistics,
      DescriptiveStatistics(pp, quantileStatistics.median),
      Histogram(pp)
    )
  }
}
