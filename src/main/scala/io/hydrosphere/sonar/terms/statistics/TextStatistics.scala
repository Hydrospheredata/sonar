package io.hydrosphere.sonar.terms.statistics

import io.hydrosphere.sonar.terms.TextPreprocessedProfile

case class TextStatistics (
  meanTokenLength: Double,
  meanCharacterLength: Double,
  meanDepTreeDepth: Double,
  meanUniqueLemmaRatio: Double,
  meanSentimentScore: Double,
  meanLanguageProba: Map[String, Double],
  meanPOSProba: Map[String, Double]
)

object TextStatistics {
  def apply(pp: TextPreprocessedProfile): TextStatistics = {
    val meanTokenLength = (pp.tokenLengthSum / pp.size).toDouble
    val meanCharacterLength = (pp.lengthSum / pp.size).toDouble
    val meanDepTreeDepth = (pp.treeDepthSum / pp.size).toDouble
    val meanUniqueLemmaRatio = (pp.uniqueLemmasSum / pp.size).toDouble
    val meanSentimentScore = (pp.sentimentSum / pp.size).toDouble
    val languageProba: Map[String, Double] = pp.languagesSum.map { case (a, b) => a -> (b / pp.size).toDouble }
    val POSProba: Map[String, Double] = pp.posTagsSum.map { case (a, b) => a -> (b / pp.size).toDouble }

    new TextStatistics(
      meanTokenLength = meanTokenLength,
      meanCharacterLength = meanCharacterLength,
      meanDepTreeDepth = meanDepTreeDepth,
      meanUniqueLemmaRatio = meanUniqueLemmaRatio,
      meanSentimentScore = meanSentimentScore,
      meanLanguageProba = languageProba,
      meanPOSProba = POSProba)
  }
}
