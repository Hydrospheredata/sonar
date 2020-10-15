package io.hydrosphere.sonar.common.utils.math

import cats.Semigroup
import cats.implicits._
import io.hydrosphere.sonar.common.terms.{NumericalPreprocessedProfile, TextPreprocessedProfile}

trait Semigroups {
  implicit val hyperLogLogSemigroup: Semigroup[HyperLogLog] = (x: HyperLogLog, y: HyperLogLog) => {
    val newBuckets = (x.buckets.toSeq ++ y.buckets.toSeq).groupBy(_._1).mapValues(_.map(_._2).max).toMap
    new HyperLogLog(math.max(x.size, y.size), newBuckets)
  }
  
  implicit val countMinSketchSemigroup: Semigroup[CountMinSketch] = (x: CountMinSketch, y: CountMinSketch) => {
    assert(x.size == y.size)
    CountMinSketch(
      x.size,
      (x.buckets.toSeq ++ y.buckets.toSeq).groupBy(_._1).mapValues(_.map(_._2).sum).toMap
    )
  }
  
  implicit val numericalPreprocessedProfileSemigroup: Semigroup[NumericalPreprocessedProfile] = (x: NumericalPreprocessedProfile, y: NumericalPreprocessedProfile) => {
    assert(x.modelVersionId == y.modelVersionId && x.name == y.name)
    NumericalPreprocessedProfile(
      x.modelVersionId,
      x.name,
      x.sum + y.sum,
      x.size + y.size,
      x.squaresSum + y.squaresSum,
      x.fourthPowersSum + y.fourthPowersSum,
      x.missing + y.missing,
      math.min(x.min, y.min),
      math.max(x.max, y.max),
      x.histogramBins |+| y.histogramBins,
      x.hyperLogLog |+| y.hyperLogLog,
      x.countMinSketch |+| y.countMinSketch
    )
  }
  
  implicit val textPreprocessedProfileSemigroup: Semigroup[TextPreprocessedProfile] = (x: TextPreprocessedProfile, y: TextPreprocessedProfile) => {
    assert(x.modelVersionId == y.modelVersionId && x.name == y.name)
    TextPreprocessedProfile(
      x.modelVersionId,
      x.name,
      x.size + y.size,
      x.missing + y.missing,
      x.sentimentSum + y.sentimentSum,
      x.lengthSum + y.lengthSum,
      x.tokenLengthSum + y.tokenLengthSum,
      x.treeDepthSum + y.treeDepthSum,
      x.uniqueLemmasSum + y.uniqueLemmasSum,
      x.languagesSum |+| y.languagesSum,
      x.posTagsSum |+| y.posTagsSum,
      x.tokenHyperLogLog |+| y.tokenHyperLogLog
    )
  }
}
