package io.hydrosphere.sonar.legacy.actors

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior, PostStop, Signal, SupervisorStrategy}
import cats.effect.IO
import io.hydrosphere.serving.manager.data_profile_types.DataProfileType
import io.hydrosphere.serving.monitoring.api.ExecutionInformation
import io.hydrosphere.sonar.legacy.actors.processors.profiles.{NumericalProfileProcessor, TextProfileProcessor}
import io.hydrosphere.sonar.legacy.actors.writers.ProfileWriter
import io.hydrosphere.sonar.legacy.services._
import java.util.concurrent.ConcurrentHashMap

import io.hydrosphere.sonar.common.config.Configuration

import scala.concurrent.duration._
import scala.util.Try

object SonarSupervisor {
  
  implicit class ConcurrentHashMapImprovements[A, B](hashMap: ConcurrentHashMap[A, B]) {
    def find(value: B): Option[A] = {
      Try {
        hashMap
          .entrySet()
          .stream()
          .filter(_.getValue.equals(value))
          .map[A](_.getKey)
          .findFirst()
          .get() 
      }.toOption
    }
  }
  
  sealed trait Message
  final case class Request(payload: ExecutionInformation) extends Message
  private[actors] final case class ProfileProcessorWasTerminated(actor: ActorRef[Processor.ProfileMessage]) extends Message

  def apply(implicit config: Configuration, metricSpecService: MetricSpecService[IO], modelDataService: ModelDataService[IO], predictionService: PredictionService[IO], profileStorageService: ProfileStorageService[IO]/*, alertManagerService: AlertManagerService[IO]*/): Behavior[Message] = Behaviors.setup[Message](context => new SonarSupervisor(context))
}

class SonarSupervisor(context: ActorContext[SonarSupervisor.Message])(implicit config: Configuration, metricSpecService: MetricSpecService[IO], modelDataService: ModelDataService[IO], predictionService: PredictionService[IO], profileStorageService: ProfileStorageService[IO]/*, alertManagerService: AlertManagerService[IO]*/) extends AbstractBehavior[SonarSupervisor.Message] {
  import SonarSupervisor._
  context.log.info("SonarSupervisor actor was started")
  
  private def createRestartableActor[T](behavior: Behavior[T], name: String): ActorRef[T] =
    context.spawn[T](Behaviors.supervise(behavior).onFailure(SupervisorStrategy.restart), name)
  
  private lazy val profileWriter = createRestartableActor(ProfileWriter(profileStorageService), "profile-writer")
  
  private val profileChildren: ConcurrentHashMap[(Long, DataProfileType), ActorRef[Processor.ProfileMessage]] = new ConcurrentHashMap[(Long, DataProfileType), ActorRef[Processor.ProfileMessage]]
  
//  private val textProfileProcessor: TextProfileProcessor = new TextProfileProcessor(config)
  
  private def getOrCreateProfileActor(behavior: Behavior[Processor.ProfileMessage], modelVersionId: Long, dataType: DataProfileType): ActorRef[Processor.ProfileMessage] = {
    Option(profileChildren.get((modelVersionId, dataType))).getOrElse({
      context.log.debug(s"Creating new profile actor (${dataType}_$modelVersionId)")
      val actor = context.spawn(behavior, s"${dataType}_$modelVersionId")
      profileChildren.put((modelVersionId, dataType), actor)
      context.watchWith(actor, ProfileProcessorWasTerminated(actor))
      actor
    })
  }

  override def onMessage(msg: SonarSupervisor.Message): Behavior[SonarSupervisor.Message] = msg match {
    case SonarSupervisor.Request(payload) =>
      context.log.debug(s"Got Request: $payload".slice(0, 1024))
      payload.metadata match {
        case Some(metadata) =>
          context.log.info(s"ExecutionMetadata(modelVersionId=${metadata.modelVersionId}, modelName=${metadata.modelName}, appInfo=${metadata.appInfo}, latency=${metadata.latency})")
          val modelVersionId = metadata.modelVersionId
          // Data profiles
          // Each DataProfileType *can* have a processor, otherwise it will be ignored
          modelDataService.getModelVersion(modelVersionId).unsafeRunAsync {
            case Right(modelVersion) =>
              modelVersion.contract.flatMap(_.predict) match {
                case Some(signature) =>
                  val inputs = signature.inputs.map(_.profile)
                  val outputs = signature.outputs.map(_.profile)
                  val modelDataTypes = inputs ++ outputs
                  if (modelDataTypes.contains(DataProfileType.NUMERICAL)) {
                    val actor = getOrCreateProfileActor(Behaviors.setup(ctx => NumericalProfileProcessor.behavior(ctx, modelVersion, profileWriter, 1 minute, 10)), modelVersionId, DataProfileType.NUMERICAL)
                    actor ! Processor.ProfileRequest(payload)
                  }
                case None =>
                  context.log.error(s"Tried to access ModelVersion without predict signature")
              }
            case Left(exc) => context.log.error(exc, s"Error while getting ModelVersion")
          }
        case None => context.log.warning(s"Empty metadata: $payload")
      }
      this
    case ProfileProcessorWasTerminated(actor) =>
      context.log.debug(s"Profile Processor actor was terminated")
      profileChildren.find(actor) match {
        case Some(key) => profileChildren.remove(key) 
        case None =>
      }
      this
  }

  override def onSignal: PartialFunction[Signal, Behavior[SonarSupervisor.Message]] = {
    case PostStop =>
      context.log.info("SonarSupervisor actor was stopped")
      this
  }
}