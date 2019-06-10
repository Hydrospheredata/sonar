package io.hydrosphere.sonar.terms

import io.hydrosphere.sonar.utils.math.{CountMinSketch, HyperLogLog}

sealed trait PreprocessedProfile

case class NumericalPreprocessedProfile(
  modelVersionId: Long,
  name: String,
  sum: BigDecimal,
  size: Long,
  squaresSum: BigDecimal,
  fourthPowersSum: BigDecimal,
  missing: Long,
  min: Double,
  max: Double,
  histogramBins: Map[Double, Long],
  hyperLogLog: HyperLogLog,
  countMinSketch: CountMinSketch
) extends PreprocessedProfile

case class TextPreprocessedProfile(
  modelVersionId: Long,
  name: String,
  size: Long,
  missing: Long,
  sentimentSum: Long,
  lengthSum: Long,
  tokenLengthSum: Long,
  treeDepthSum: Long,
  uniqueLemmasSum: Double,
  languagesSum: Map[String, Long],
  posTagsSum: Map[String, Long],
  tokenHyperLogLog: HyperLogLog
) extends PreprocessedProfile
