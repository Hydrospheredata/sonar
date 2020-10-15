package io.hydrosphere.sonar.common.terms

import io.hydrosphere.sonar.common.terms.statistics._

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
    val quantileStatistics = statistics.QuantileStatistics(pp)
    new NumericalProfile(
      pp.name,
      pp.modelVersionId,
      statistics.CommonStatistics(pp),
      quantileStatistics,
      statistics.DescriptiveStatistics(pp, quantileStatistics.median),
      statistics.Histogram(pp)
    )
  }
}


case class TextProfile(
  name: String,
  modelVersionId: Long,
  commonStatistics: CommonStatistics,
  textStatistics: TextStatistics
) extends Profile

object TextProfile {
  def apply(pp: TextPreprocessedProfile): TextProfile = {
    new TextProfile(
      pp.name,
      pp.modelVersionId,
      statistics.CommonStatistics(pp),
      statistics.TextStatistics(pp)
    )
  }
}
