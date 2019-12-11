package io.hydrosphere.sonar.actors

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior, PostStop, Signal, SupervisorStrategy}
import cats.effect.IO
import io.hydrosphere.serving.manager.data_profile_types.DataProfileType
import io.hydrosphere.serving.monitoring.api.ExecutionInformation
import io.hydrosphere.sonar.actors.processors.profiles.{NumericalProfileProcessor, TextProfileProcessor}
import io.hydrosphere.sonar.actors.writers.{MetricWriter, ProfileWriter}
import io.hydrosphere.sonar.config.Configuration
import io.hydrosphere.sonar.services._
import io.hydrosphere.sonar.terms.{MetricSpec, Processable}
import java.util.concurrent.ConcurrentHashMap

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
  final case class AddProcessor(metricSpec: MetricSpec) extends Message
  final case class RemoveProcessor(metricSpecId: String) extends Message
  private[actors] final case class MetricProcessorWasTerminated(actor: ActorRef[Processor.MetricMessage]) extends Message
  private[actors] final case class ProfileProcessorWasTerminated(actor: ActorRef[Processor.ProfileMessage]) extends Message

  def apply(implicit config: Configuration, metricSpecService: MetricSpecService[IO], modelDataService: ModelDataService[IO], predictionService: PredictionService[IO], metricStorageService: MetricStorageService[IO], profileStorageService: ProfileStorageService[IO], alertManagerService: AlertManagerService[IO]): Behavior[Message] = Behaviors.setup[Message](context => new SonarSupervisor(context))
}

class SonarSupervisor(context: ActorContext[SonarSupervisor.Message])(implicit config: Configuration, metricSpecService: MetricSpecService[IO], modelDataService: ModelDataService[IO], predictionService: PredictionService[IO], metricStorageService: MetricStorageService[IO], profileStorageService: ProfileStorageService[IO], alertManagerService: AlertManagerService[IO]) extends AbstractBehavior[SonarSupervisor.Message] {
  import Processable.implicits._
  import SonarSupervisor._
  import io.hydrosphere.sonar.utils.ProcessableMetricSpecs._

  context.log.info("SonarSupervisor actor was started")
  
  private def createRestartableActor[T](behavior: Behavior[T], name: String): ActorRef[T] =
    context.spawn[T](Behaviors.supervise(behavior).onFailure(SupervisorStrategy.restart), name)
  
  private lazy val metricWriter = createRestartableActor(MetricWriter(metricStorageService, alertManagerService), "metric-writer")
  private lazy val profileWriter = createRestartableActor(ProfileWriter(profileStorageService), "profile-writer")
  
  private val metricChildren: ConcurrentHashMap[String, ActorRef[Processor.MetricMessage]] = new ConcurrentHashMap[String, ActorRef[Processor.MetricMessage]]
  private val profileChildren: ConcurrentHashMap[(Long, DataProfileType), ActorRef[Processor.ProfileMessage]] = new ConcurrentHashMap[(Long, DataProfileType), ActorRef[Processor.ProfileMessage]]
  
  private val textProfileProcessor: TextProfileProcessor = new TextProfileProcessor(config)

  private def processor[T: Processable](metricSpec: T): Behavior[Processor.MetricMessage] = metricSpec.processor
  
  private def getOrCreateMetricActor(behavior: Behavior[Processor.MetricMessage], name: String): ActorRef[Processor.MetricMessage] = {
    Option(metricChildren.get(name)) match {
      case Some(actor) => actor
      case None =>
        context.log.debug(s"Creating new metric actor ($name)")
        val actor = context.spawn(behavior, name)
        metricChildren.put(name, actor)
        context.watchWith(actor, MetricProcessorWasTerminated(actor))
        actor
    }
  }
  
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
          // Concept drift metrics
          // Each MetricSpec *must* have an appropriate processor
//          metricSpecService.getMetricSpecsByModelVersion(modelVersionId).unsafeRunAsync {
//            case Right(metricSpecs) => 
//              metricSpecs
//                /*_*/
//                .map(spec => getOrCreateMetricActor(processor(spec), s"${spec.id}-${spec.modelVersionId}"))
//                /*_*/
//                .foreach(_ ! Processor.MetricRequest(payload, metricWriter))
//            case Left(exc) => context.log.error(exc, s"Error while getting MetricSpecs")
//          }
          
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
                  if (modelDataTypes.contains(DataProfileType.TEXT)) {
                    val actor = getOrCreateProfileActor(Behaviors.setup(ctx => textProfileProcessor.behavior(ctx, modelVersion, profileWriter, 1 minute, 10)), modelVersionId, DataProfileType.TEXT)
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
      
    case AddProcessor(metricSpec) =>
      context.log.debug(s"Got AddProcessor: $metricSpec")
      /*_*/
      getOrCreateMetricActor(processor(metricSpec), metricSpec.id)
      /*_*/
      this
    case RemoveProcessor(metricSpecId) =>
      context.log.debug(s"Got RemoveProcessor: $metricSpecId")
      metricChildren.remove(metricSpecId)
      this
      
    case MetricProcessorWasTerminated(actor) =>
      context.log.debug(s"Metric Processor actor was terminated")
      metricChildren.find(actor) match {
        case Some(key) => metricChildren.remove(key) 
        case None =>
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