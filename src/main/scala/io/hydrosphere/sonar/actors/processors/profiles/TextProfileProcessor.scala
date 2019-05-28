package io.hydrosphere.sonar.actors.processors.profiles

import java.io.StringReader
import java.util.Properties

import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import cats.implicits._
import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, SentencesAnnotation, TokensAnnotation}
import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.parser.shiftreduce.ShiftReduceParser
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.process.{CoreLabelTokenFactory, PTBTokenizer, TokenizerFactory}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import edu.stanford.nlp.tagger.maxent.MaxentTagger
import edu.stanford.nlp.util.CoreMap
import io.hydrosphere.serving.manager.data_profile_types.DataProfileType
import io.hydrosphere.serving.manager.grpc.entities.ModelVersion
import io.hydrosphere.sonar.actors.Processor
import io.hydrosphere.sonar.actors.writers.ProfileWriter
import io.hydrosphere.sonar.config.Configuration
import io.hydrosphere.sonar.terms.TextPreprocessedProfile
import io.hydrosphere.sonar.utils.CollectionOps
import io.hydrosphere.sonar.utils.ExecutionInformationOps._
import io.hydrosphere.sonar.utils.math.MutableHyperLogLog

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

class TextProfileProcessor(config: Configuration) extends ProfileProcessor {
  case object TimeKey
  case object Timeout extends Processor.ProfileMessage

  private lazy val pipeline = {
    val pros = new Properties()
    pros.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment") // TokenRegexNER not found to make "ner"
    pros.setProperty("tokenize.model", config.profile.text.taggerPath)
    pros.setProperty("ssplit.model", config.profile.text.taggerPath)
    pros.setProperty("pos.model", config.profile.text.taggerPath)
    pros.setProperty("lemma.model", config.profile.text.taggerPath)
    pros.setProperty("parse.model", config.profile.text.lexParserPath)
    pros.setProperty("sentiment.model", config.profile.text.sentimentPath)
    new StanfordCoreNLP(pros)
  }

  private lazy val tokenizerFactory: TokenizerFactory[CoreLabel] = PTBTokenizer.factory(new CoreLabelTokenFactory(), "")

  lazy val tagger: MaxentTagger = new MaxentTagger(config.profile.text.taggerPath)

  lazy val parser: ShiftReduceParser = ShiftReduceParser.loadModel(config.profile.text.shiftReduceParserPath)

  def behavior(context: ActorContext[Processor.ProfileMessage], modelVersion: ModelVersion, saveTo: ActorRef[ProfileWriter.Message], duration: FiniteDuration, maxSize: Int): Behavior[Processor.ProfileMessage] = {
    Behaviors.withTimers { timers =>
      timers.startPeriodicTimer(TimeKey, Timeout, duration)
      active(timers, context, modelVersion, saveTo, duration, maxSize, Vector.empty[Processor.ProfileRequest])
    }
  }

  def process(requests: Vector[Processor.ProfileRequest], context: ActorContext[Processor.ProfileMessage], saveTo: ActorRef[ProfileWriter.ProcessedProfile], modelVersion: ModelVersion): Unit = {
    val inputs = numericalInputs(modelVersion, DataProfileType.TEXT)
    inputs.foreach { input =>
      val flat = requests.map(r => r.payload.getStringInput(input))
      val transposed = CollectionOps.safeTranspose(flat)
      transposed.zipWithIndex.foreach { case (column, idx) =>
        val name = s"${input}_$idx"
        val hyperLogLog = MutableHyperLogLog(14)
        var size = 0L
        var missing = 0L
        var sentimentSum = 0L
        var lengthSum = 0L
        var tokenCount = 0L
        var sentenceTreeDepthSum = 0L
        var uniqueLemmaRatioSum = 0d
        var tagsOccurrence = Map.empty[String, Long]
        for (value: String <- column) {
          size += 1
          if (value == "") {
            missing += 1
          }

          val annotation = new Annotation(value)
          pipeline.annotate(annotation)
          val sentenceAnnotation: java.util.List[CoreMap] = annotation.get(classOf[SentencesAnnotation])
          val sentiments = sentenceAnnotation.asScala.map { sentence: CoreMap =>
            sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])
          }.map(tree => RNNCoreAnnotations.getPredictedClass(tree))
          val sentimentScore = sentiments.sum / sentiments.size
          sentimentSum += sentimentScore
          
          lengthSum += value.length
          
          val tokens = tokenizerFactory.getTokenizer(new StringReader(value)).tokenize()
          tokenCount += tokens.size()
          
          val tagged = tagger.tagSentence(tokens)
//          val sentenceTreeDepth = parser.apply(tagged).depth()
//          sentenceTreeDepthSum += sentenceTreeDepth
          
          val lemmas = sentenceAnnotation.asScala.toList.flatMap{ sentence => 
            sentence
              .get(classOf[TokensAnnotation])
              .asScala
              .map(word => word.get(classOf[LemmaAnnotation]))
          }
          val lemmaLength = lemmas.size
          val uniqueLemmaRatio = lemmaLength.toDouble / tokens.size().toDouble
          uniqueLemmaRatioSum += uniqueLemmaRatio

          tagsOccurrence = tagsOccurrence |+| tagged.asScala.map(_.tag()).groupBy(identity).mapValues(_.size.toLong).withDefaultValue(0L)
          
          for (token: CoreLabel <- tokens.asScala) {
            hyperLogLog.add(token.value())
          }
        }
        saveTo ! ProfileWriter.ProcessedProfile(
          TextPreprocessedProfile(
            modelVersion.id,
            name,
            size,
            missing,
            sentimentSum,
            lengthSum,
            tokenCount,
            0L,
            uniqueLemmaRatioSum,
            // TODO: implement language detection
            Map.empty,
            tagsOccurrence,
            hyperLogLog.toHyperLogLog
          )
        )
      }
    }
  }

  def active(timers: TimerScheduler[Processor.ProfileMessage], context: ActorContext[Processor.ProfileMessage], modelVersion: ModelVersion, saveTo: ActorRef[ProfileWriter.Message], duration: FiniteDuration, maxSize: Int, buffer: Vector[Processor.ProfileRequest]): Behavior[Processor.ProfileMessage] = {
    Behaviors.receiveMessage {
      case m: Processor.ProfileRequest =>
        val newBuffer = buffer :+ filterRequest(m, modelVersion, DataProfileType.TEXT)
        if (newBuffer.size == maxSize) {
          context.log.debug(s"Processing TextProfile for ${newBuffer.size} elements (max)")
          process(newBuffer, context, saveTo, modelVersion)
          active(timers, context, modelVersion, saveTo, duration, maxSize, Vector.empty[Processor.ProfileRequest])
        } else {
          active(timers, context, modelVersion, saveTo, duration, maxSize, newBuffer)
        }
      case Timeout =>
        context.log.debug(s"Processing TextProfile for ${buffer.size} elements (timeout)")
        process(buffer, context, saveTo, modelVersion)
        active(timers, context, modelVersion, saveTo, duration, maxSize, Vector.empty[Processor.ProfileRequest])
    }
  }
}
