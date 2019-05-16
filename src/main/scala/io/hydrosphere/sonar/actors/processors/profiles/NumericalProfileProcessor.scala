package io.hydrosphere.sonar.actors.processors.profiles

import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import io.hydrosphere.serving.manager.data_profile_types.DataProfileType
import io.hydrosphere.serving.manager.grpc.entities.ModelVersion
import io.hydrosphere.serving.monitoring.api.ExecutionInformation
import io.hydrosphere.serving.tensorflow.api.predict.PredictRequest
import io.hydrosphere.sonar.actors.Processor
import io.hydrosphere.sonar.actors.Processor.ProfileRequest
import io.hydrosphere.sonar.actors.writers.ProfileWriter
import io.hydrosphere.sonar.utils.CollectionOps
import io.hydrosphere.sonar.utils.ExecutionInformationOps._
import io.hydrosphere.sonar.utils.profiles.NumericalProfileUtils

import scala.concurrent.duration.FiniteDuration

object NumericalProfileProcessor {
  
  case object TimeKey
  case object Timeout extends Processor.ProfileMessage
  
  def numericalInputs(modelVersion: ModelVersion): Set[String] = {
    modelVersion
      .contract.flatMap(_.predict)
      .map(_.inputs)
      .getOrElse(Seq.empty)
      .filter(x => x.profile == DataProfileType.NUMERICAL)
      .map(_.name)
      .toSet
  }
  
  def filterRequest(request: Processor.ProfileRequest, modelVersion: ModelVersion): Processor.ProfileRequest = {
    val inputs = numericalInputs(modelVersion)
    ProfileRequest(
      payload = ExecutionInformation(
        request = request.payload.request.map(r => PredictRequest(r.modelSpec, r.inputs.filter({ case (key, _) => inputs.contains(key) }))),
        metadata = request.payload.metadata,
        responseOrError = ExecutionInformation.ResponseOrError.Empty
      )
    )
  }
  
  def behavior(context: ActorContext[Processor.ProfileMessage], modelVersion: ModelVersion, saveTo: ActorRef[ProfileWriter.Message], duration: FiniteDuration, maxSize: Int): Behavior[Processor.ProfileMessage] = {
    Behaviors.withTimers { timers =>
      timers.startPeriodicTimer(TimeKey, Timeout, duration)
      active(timers, context, modelVersion, saveTo, duration, maxSize, Vector.empty[Processor.ProfileRequest])
    }
  }
  
  def process(requests: Vector[Processor.ProfileRequest], context: ActorContext[Processor.ProfileMessage], saveTo: ActorRef[ProfileWriter.ProcessedProfile], modelVersion: ModelVersion): Unit = {
    val inputs = numericalInputs(modelVersion)
    inputs.foreach { input =>
      val flat = requests.map(r => r.payload.getDoubleInput(input))
      val transposed = CollectionOps.safeTranspose(flat)
      transposed.zipWithIndex.foreach { case (column, idx) => 
        val preprocessedProfile = NumericalProfileUtils.fromColumn(modelVersion.id, input, idx, column)
        saveTo ! ProfileWriter.ProcessedProfile(preprocessedProfile)
      }
    }
  }
  
  def active(timers: TimerScheduler[Processor.ProfileMessage], context: ActorContext[Processor.ProfileMessage], modelVersion: ModelVersion, saveTo: ActorRef[ProfileWriter.Message], duration: FiniteDuration, maxSize: Int, buffer: Vector[Processor.ProfileRequest]): Behavior[Processor.ProfileMessage] = {
    Behaviors.receiveMessage {
      case m: Processor.ProfileRequest =>
        val newBuffer = buffer :+ filterRequest(m, modelVersion)
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
