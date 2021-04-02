package io.hydrosphere.sonar.actors.processors.profiles

import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import io.hydrosphere.serving.proto.contract.types.DataProfileType
import io.hydrosphere.serving.proto.manager.entities.ModelVersion
import io.hydrosphere.serving.proto.runtime.api.PredictRequest
import io.hydrosphere.monitoring.proto.sonar.entities.ExecutionInformation
import io.hydrosphere.sonar.actors.Processor
import io.hydrosphere.sonar.actors.Processor.ProfileRequest
import io.hydrosphere.sonar.actors.writers.ProfileWriter
import io.hydrosphere.sonar.utils.CollectionOps
import io.hydrosphere.sonar.utils.ExecutionInformationOps._
import io.hydrosphere.sonar.utils.profiles.NumericalProfileUtils

import scala.concurrent.duration.FiniteDuration
import edu.stanford.nlp.patterns.Data

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
    val inputs = filterInputs(modelVersion, DataProfileType.NUMERICAL)
      .map(input => (input, requests.map(r => r.payload.getDoubleInput(input))))
    val outputs = filterOutputs(modelVersion, DataProfileType.NUMERICAL)
      .map(output => (output, requests.map(_.payload.getDoubleOutput(output))))
    
    (inputs ++ outputs).foreach { case (field, flat) =>
      val transposed = CollectionOps.safeTranspose(flat)
      transposed.zipWithIndex.foreach { case (column, idx) => 
        val preprocessedProfile = NumericalProfileUtils.fromColumn(modelVersion.id, field, idx, column)
        saveTo ! ProfileWriter.ProcessedProfile(preprocessedProfile)
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
