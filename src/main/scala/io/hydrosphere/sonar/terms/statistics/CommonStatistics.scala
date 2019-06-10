package io.hydrosphere.sonar.terms.statistics

import io.hydrosphere.sonar.terms.{NumericalPreprocessedProfile, TextPreprocessedProfile}

case class CommonStatistics(count: Long, distinctCount: Long, missing: Long)

object CommonStatistics {
  def apply(pp: NumericalPreprocessedProfile): CommonStatistics = {
    new CommonStatistics(pp.size, pp.hyperLogLog.count, pp.missing)
  }

  def apply(pp: TextPreprocessedProfile): CommonStatistics = {
    new CommonStatistics(pp.size, pp.tokenHyperLogLog.count, pp.missing)
  }
}