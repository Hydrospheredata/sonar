package io.hydrosphere.sonar.terms.statistics

import java.math.{MathContext, RoundingMode}

import io.circe.generic.JsonCodec
import io.hydrosphere.sonar.terms.NumericalPreprocessedProfile

@JsonCodec
case class Histogram(
  min: Double, 
  max: Double, 
  step: Double, 
  bars: Int, 
  frequencies: Seq[Long], 
  bins: Seq[Double]
)

object Histogram {
  def apply(preprocessedProfiles: NumericalPreprocessedProfile): Histogram = {
    val sorted = preprocessedProfiles.histogramBins.toSeq.sortBy(_._1)
    val sortedWithSteps = sorted.map({
      case (k, v) =>
        val b = BigDecimal(k / 10).round(new MathContext(1, RoundingMode.DOWN))
        val step = if (b == 0) 0d else (b / (b * scala.math.pow(10, b.scale))).toDouble
        (step, k, v)
    })
    val steps = sortedWithSteps.groupBy(_._1).mapValues(_.map(_._3).sum).toSeq.sortBy(_._2)
    val largestStep = steps.last._1
    val bins = sortedWithSteps.groupBy(_._1).flatMap({
      case (st, list) if st < largestStep =>
        list
          .map(x => (BigDecimal(x._2).round(new MathContext(1, RoundingMode.DOWN)).toDouble, x._3))
          .groupBy(_._1)
          .mapValues(_.map(_._2).sum)
          .toSeq
      case (_, list) =>
        if (list.size >= 20) {
          list.map(x => (x._2, x._3)).grouped(math.floor(list.size / 10).toInt).toList.map({ x =>
            x.reduce((a, b) => (math.min(a._1, b._1), a._2 + b._2))
          })
        } else {
          list.map(x => (x._2, x._3))
        }
    }).toSeq.sortBy(_._1)
    val binsKeys = bins.map(_._1)
    val binsValues = bins.map(_._2)
    new Histogram(binsKeys.head, binsKeys.last, largestStep, bins.size, binsValues, binsKeys)
  }
}