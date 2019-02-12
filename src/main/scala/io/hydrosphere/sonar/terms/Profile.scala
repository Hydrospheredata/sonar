package io.hydrosphere.sonar.terms

import io.hydrosphere.sonar.terms.statistics._

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
      CommonStatistics(pp),
      TextStatistics(pp)
    )
  }
}
