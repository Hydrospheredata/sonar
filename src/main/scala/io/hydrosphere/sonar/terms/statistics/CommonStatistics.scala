package io.hydrosphere.sonar.terms.statistics

import io.hydrosphere.sonar.terms.NumericalPreprocessedProfile

case class CommonStatistics(count: Long, distinctCount: Long, missing: Long)

object CommonStatistics {
  def apply(pp: NumericalPreprocessedProfile): CommonStatistics = {
    new CommonStatistics(pp.size, pp.hyperLogLog.count, pp.missing)
  }
}