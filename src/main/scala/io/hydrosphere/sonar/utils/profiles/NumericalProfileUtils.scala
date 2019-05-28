package io.hydrosphere.sonar.utils.profiles

import java.math.{MathContext, RoundingMode}

import io.hydrosphere.sonar.Logging
import io.hydrosphere.sonar.terms.NumericalPreprocessedProfile
import io.hydrosphere.sonar.utils.math.{MutableCountMinSketch, MutableHyperLogLog}

import scala.collection.mutable

object NumericalProfileUtils extends Logging {

  def fromColumn(modelVersionId: Long, input: String, idx: Int, column: Seq[Double]): NumericalPreprocessedProfile = {
    val name = s"${input}_$idx"
    var missingCount = 0
    var size = 0
    var min = Double.MaxValue
    var max = Double.MinValue
    var sum: BigDecimal = 0
    var squaresSum: BigDecimal = 0
    var fourthPowersSum: BigDecimal = 0
    val histogramBins = mutable.Map.empty[Double, Long].withDefaultValue(0L)
    val hll = MutableHyperLogLog(14)
    val cms = MutableCountMinSketch(13)
    for (value: Double <- column) {
      size += 1
      if (value.isNaN) {
        missingCount += 1
      } else {
        try {
          if (value < min) {
            min = value
          }
          if (value > max) {
            max = value
          }
          sum += value
          squaresSum += math.pow(value, 2)
          fourthPowersSum += math.pow(value, 4)
          val binValue = BigDecimal(value).round(new MathContext(2, RoundingMode.DOWN)).toDouble
          histogramBins(binValue) += 1
          hll.add(value)
          cms.add(value)
        } catch {
          case e: Exception =>
            logger.error("Error while processing the stream", e)
        } 
      }
    }
    NumericalPreprocessedProfile(
      modelVersionId,
      name,
      sum,
      size,
      squaresSum,
      fourthPowersSum,
      missingCount,
      min,
      max,
      histogramBins.toMap,
      hll.toHyperLogLog,
      cms.toCountMinSketch
    )
  }
  
}
