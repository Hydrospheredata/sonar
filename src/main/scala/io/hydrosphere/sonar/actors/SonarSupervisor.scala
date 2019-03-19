package io.hydrosphere.sonar.actors

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior, PostStop, Signal, SupervisorStrategy}
import cats.effect.IO
import io.hydrosphere.serving.manager.data_profile_types.DataProfileType
import io.hydrosphere.serving.monitoring.monitoring.ExecutionInformation
import io.hydrosphere.sonar.actors.processors.profiles.NumericalProfileProcessor
import io.hydrosphere.sonar.actors.processors.subsampling.ReservoirProcessor
import io.hydrosphere.sonar.actors.writers.{MetricWriter, ProfileWriter}
import io.hydrosphere.sonar.config.{Configuration, Reservoir}
import io.hydrosphere.sonar.services._
import io.hydrosphere.sonar.terms.{MetricSpec, Processable}

import scala.concurrent.duration._

object SonarSupervisor {
  sealed trait Message
  final case class Request(payload: ExecutionInformation) extends Message
  final case class AddProcessor(metricSpec: MetricSpec) extends Message
  final case class RemoveProcessor(metricSpecId: String) extends Message
  private[actors] final case class MetricProcessorWasTerminated(actor: ActorRef[Processor.MetricMessage]) extends Message
  private[actors] final case class ProfileProcessorWasTerminated(actor: ActorRef[Processor.ProfileMessage]) extends Message
  private[actors] final case class SubsamplingProcessorWasTerminated(actor: ActorRef[Processor.SubsamplingMessage]) extends Message

  def apply(implicit config: Configuration, metricSpecService: MetricSpecService[IO], modelDataService: ModelDataService[IO], predictionService: PredictionService[IO], metricStorageService: MetricStorageService[IO], profileStorageService: ProfileStorageService[IO]): Behavior[Message] = Behaviors.setup[Message](context => new SonarSupervisor(context))
}

class SonarSupervisor(context: ActorContext[SonarSupervisor.Message])(implicit config: Configuration, metricSpecService: MetricSpecService[IO], modelDataService: ModelDataService[IO], predictionService: PredictionService[IO], metricStorageService: MetricStorageService[IO], profileStorageService: ProfileStorageService[IO]) extends AbstractBehavior[SonarSupervisor.Message] {
  import Processable.implicits._
  import SonarSupervisor._
  import io.hydrosphere.sonar.utils.ProcessableMetricSpecs._

  context.log.info("SonarSupervisor actor was started")
  
  private def createRestartableActor[T](behavior: Behavior[T], name: String): ActorRef[T] =
    context.spawn[T](Behaviors.supervise(behavior).onFailure(SupervisorStrategy.restart), name)
  
  private lazy val metricWriter = createRestartableActor(MetricWriter(metricStorageService), "metric-writer")
  private lazy val profileWriter = createRestartableActor(ProfileWriter(profileStorageService), "profile-writer")
  
  private var metricChildren: Map[String, ActorRef[Processor.MetricMessage]] = Map.empty
  private var profileChildren: Map[(Long, DataProfileType), ActorRef[Processor.ProfileMessage]] = Map.empty
  private var subsamplingChildren: Map[Long, ActorRef[Processor.SubsamplingMessage]] = Map.empty

  private def processor[T: Processable](metricSpec: T): Behavior[Processor.MetricMessage] = metricSpec.processor
  
  private def getOrCreateMetricActor(behavior: Behavior[Processor.MetricMessage], name: String): ActorRef[Processor.MetricMessage] = {
    metricChildren.get(name) match {
      case Some(actor) => actor
      case None =>
        context.log.debug(s"Creating new metric actor ($name)")
        val actor = context.spawn(behavior, name)
        metricChildren += name -> actor
        context.watchWith(actor, MetricProcessorWasTerminated(actor))
        actor
    }
  }
  
  private def getOrCreateProfileActor(behavior: Behavior[Processor.ProfileMessage], modelVersionId: Long, dataType: DataProfileType): ActorRef[Processor.ProfileMessage] = {
    profileChildren.getOrElse((modelVersionId, dataType), {
      context.log.debug(s"Creating new profile actor (${dataType}_$modelVersionId)")
      val actor = context.spawn(behavior, s"${dataType}_$modelVersionId")
      profileChildren += (modelVersionId, dataType) -> actor
      context.watchWith(actor, ProfileProcessorWasTerminated(actor))
      actor
    })
  }
  
  private def getOrCreateSubsamplingActor(behavior: Behavior[Processor.SubsamplingMessage], modelVersionId: Long): ActorRef[Processor.SubsamplingMessage] = {
    subsamplingChildren.getOrElse(modelVersionId, {
      context.log.debug(s"Creating new subsampling actor $modelVersionId")
      val actor = context.spawn(behavior, s"subsamplin_$modelVersionId")
      subsamplingChildren += modelVersionId -> actor
      context.watchWith(actor, SubsamplingProcessorWasTerminated(actor))
      actor
    })
  }

  override def onMessage(msg: SonarSupervisor.Message): Behavior[SonarSupervisor.Message] = msg match {
    case SonarSupervisor.Request(payload) =>
      context.log.debug(s"Got Request: $payload".slice(0, 1024))
      payload.metadata match {
        case Some(metadata) =>
          context.log.info(s"$metadata")
          val modelVersionId = metadata.modelVersionId
          // Concept drift metrics
          // Each MetricSpec *must* have an appropriate processor
          metricSpecService.getMetricSpecsByModelVersion(modelVersionId).unsafeRunAsync {
            case Right(metricSpecs) => 
              metricSpecs
                .map(spec => getOrCreateMetricActor(processor(spec), spec.id))
                .foreach(_ ! Processor.MetricRequest(payload, metricWriter))
            case Left(exc) => context.log.error(exc, s"Error while getting MetricSpecs")
          }
          
          // Data profiles
          // Each DataProfileType *can* have a processor, otherwise it will be ignored
          modelDataService.getModelVersion(modelVersionId).unsafeRunAsync {
            case Right(modelVersion) =>
              val modelDataTypes = modelVersion.dataTypes.values.toList 
              if (modelDataTypes.contains(DataProfileType.NUMERICAL)) {
                val actor = getOrCreateProfileActor(Behaviors.setup(ctx => NumericalProfileProcessor.behavior(ctx, modelVersion, profileWriter, 1 minute, 10)), modelVersionId, DataProfileType.NUMERICAL)
                actor ! Processor.ProfileRequest(payload)
              }
            case Left(exc) => context.log.error(exc, s"Error while getting ModelVersion")
          }
          
          // Subsampling
          val behavior = config.subsampling match {
            case Reservoir(size) => Behaviors.setup[Processor.SubsamplingMessage](ctx => new ReservoirProcessor(ctx, size))
          }
          val actor = getOrCreateSubsamplingActor(behavior, metadata.modelVersionId)
          actor ! Processor.SubsamplingRequest(payload)
        case None => context.log.warning(s"Empty metadata: $payload")
      }
      this
      
    case AddProcessor(metricSpec) =>
      context.log.debug(s"Got AddProcessor: $metricSpec")
      getOrCreateMetricActor(processor(metricSpec), metricSpec.id)
      this
    case RemoveProcessor(metricSpecId) =>
      context.log.debug(s"Got RemoveProcessor: $metricSpecId")
      metricChildren -= metricSpecId
      this
      
    case MetricProcessorWasTerminated(actor) =>
      context.log.debug(s"Metric Processor actor was terminated")
      metricChildren.find({case (_, a) => a == actor}).foreach({case (id, _) => metricChildren -= id})
      this
    case ProfileProcessorWasTerminated(actor) =>
      context.log.debug(s"Profile Processor actor was terminated")
      profileChildren.find({ case (_, a) => a == actor }).foreach({ case (key, _) => profileChildren -= key })
      this
    case SubsamplingProcessorWasTerminated(actor) =>
      context.log.debug(s"Subsampling actor was terminated")
      subsamplingChildren.find({ case (_, a) => a == actor }).foreach({ case (key, _) => subsamplingChildren -= key })
      this
  }

  override def onSignal: PartialFunction[Signal, Behavior[SonarSupervisor.Message]] = {
    case PostStop =>
      context.log.info("SonarSupervisor actor was stopped")
      this
  }
}