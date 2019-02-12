package io.hydrosphere.sonar.actors.processors.profiles

import java.math.{MathContext, RoundingMode}

import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import io.hydrosphere.serving.manager.data_profile_types.DataProfileType
import io.hydrosphere.serving.manager.grpc.entities.ModelVersion
import io.hydrosphere.sonar.actors.Processor
import io.hydrosphere.sonar.actors.writers.ProfileWriter
import io.hydrosphere.sonar.terms.NumericalPreprocessedProfile
import io.hydrosphere.sonar.utils.CollectionOps
import io.hydrosphere.sonar.utils.ExecutionInformationOps._
import io.hydrosphere.sonar.utils.math.{MutableCountMinSketch, MutableHyperLogLog}

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

object NumericalProfileProcessor extends ProfileProcessor {
  
  case object TimeKey
  case object Timeout extends Processor.ProfileMessage
  
  def behavior(context: ActorContext[Processor.ProfileMessage], modelVersion: ModelVersion, saveTo: ActorRef[ProfileWriter.Message], duration: FiniteDuration, maxSize: Int): Behavior[Processor.ProfileMessage] = {
    Behaviors.withTimers { timers =>
      timers.startPeriodicTimer(TimeKey, Timeout, duration)
      active(timers, context, modelVersion, saveTo, duration, maxSize, Vector.empty[Processor.ProfileRequest])
    }
  }
  
  def process(requests: Vector[Processor.ProfileRequest], context: ActorContext[Processor.ProfileMessage], saveTo: ActorRef[ProfileWriter.ProcessedProfile], modelVersion: ModelVersion): Unit = {
    val inputs = numericalInputs(modelVersion, DataProfileType.NUMERICAL)
    inputs.foreach { input =>
      val flat = requests.map(r => r.payload.getDoubleInput(input))
      val transposed = CollectionOps.safeTranspose(flat)
      transposed.zipWithIndex.foreach { case (column, idx) => 
        val name = s"${input}_$idx"
        val missingCount = 0
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
              context.log.error(e, "Error while processing the stream")
          }
        }
        saveTo ! ProfileWriter.ProcessedProfile(NumericalPreprocessedProfile(
          modelVersion.id,
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
        ))
      }
    }
  }
  
  def active(timers: TimerScheduler[Processor.ProfileMessage], context: ActorContext[Processor.ProfileMessage], modelVersion: ModelVersion, saveTo: ActorRef[ProfileWriter.Message], duration: FiniteDuration, maxSize: Int, buffer: Vector[Processor.ProfileRequest]): Behavior[Processor.ProfileMessage] = {
    Behaviors.receiveMessage {
      case m: Processor.ProfileRequest =>
        val newBuffer = buffer :+ filterRequest(m, modelVersion, DataProfileType.NUMERICAL)
        if (newBuffer.size == maxSize) {
          context.log.debug(s"Processing NumericalProfile for ${newBuffer.size} elements (max)")
          process(newBuffer, context, saveTo, modelVersion)
          active(timers, context, modelVersion, saveTo, duration, maxSize, Vector.empty[Processor.ProfileRequest])
        } else {
          active(timers, context, modelVersion, saveTo, duration, maxSize, newBuffer)
        }
      case Timeout =>
        context.log.debug(s"Processing NumericalProfile for ${buffer.size} elements (timeout)")
        process(buffer, context, saveTo, modelVersion)
        active(timers, context, modelVersion, saveTo, duration, maxSize, Vector.empty[Processor.ProfileRequest])
    }
  }
  
}
