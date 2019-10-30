package io.hydrosphere.sonar.actors

import java.util.UUID

import akka.actor.typed.scaladsl._
import akka.actor.typed.{ActorRef, Behavior}
import com.google.protobuf.empty.Empty
import io.grpc.stub.StreamObserver
import io.hydrosphere.serving.discovery.serving.MetricSpecDiscoveryEvent
import io.hydrosphere.serving.discovery.serving.ServingDiscoveryGrpc.ServingDiscovery
import io.hydrosphere.serving.manager.grpc.entities.ThresholdConfig.CmpOp
import io.hydrosphere.serving.manager.grpc.entities.{ThresholdConfig, MetricSpec => GMetricSpec}
import io.hydrosphere.sonar.actors.MetricSpecDiscoverer._
import io.hydrosphere.sonar.terms._

import scala.concurrent.duration.FiniteDuration
import scala.util.Try


class MetricSpecDiscoverer(
                            context: ActorContext[DiscoveryMsg],
                            reconnectTimeout: FiniteDuration,
                            stub: ServingDiscovery
                          )
  extends AbstractBehavior[DiscoveryMsg] {

  var metricSpecs = Map.empty[String, MetricSpec]

  override def onMessage(msg: DiscoveryMsg): Behavior[DiscoveryMsg] = disconnected

  def disconnected: Behavior[DiscoveryMsg] =  Behaviors.withTimers[DiscoveryMsg] { timer =>
    val id = UUID.randomUUID().toString
    val timerKey = "connect-" + id
    timer.startPeriodicTimer(timerKey, Connect, reconnectTimeout)
    Behaviors.receiveMessagePartial {
      case Connect =>
        Try(connect()) match {
          case util.Success(serv) =>
            timer.cancel(timerKey)
            listening(serv)
          case util.Failure(e) =>
            context.log.error(e, s"Can't setup discovery connection")
            Behaviors.same
        }

      case GetAll(replyTo) =>
        replyTo ! GetAllResponse(metricSpecs.values.toList)
        Behavior.same

      case FindBySpecId(specId, replyTo) =>
        replyTo ! FindBySpecResponse(metricSpecs.get(specId))
        Behavior.same

      case GetByModelVersion(versionId, replyTo) =>
        replyTo ! GetByModelVersionResponse(metricSpecs.values.filter(_.modelVersionId == versionId).toList)
        Behavior.same
    }
  }

  def listening(servableResponse: StreamObserver[Empty]): Behavior[DiscoveryMsg] = Behaviors.receiveMessagePartial {
    case GetAll(replyTo) =>
      replyTo ! GetAllResponse(metricSpecs.values.toList)
      Behavior.same

    case FindBySpecId(specId, replyTo) =>
      replyTo ! FindBySpecResponse(metricSpecs.get(specId))
      Behavior.same

    case GetByModelVersion(versionId, replyTo) =>
      replyTo ! GetByModelVersionResponse(metricSpecs.values.filter(_.modelVersionId == versionId).toList)
      Behavior.same

    case DiscoveredSpec(internal) =>
      handleDiscovery(internal)
      Behavior.same

    case ConnectionFailed(maybeE) => Behaviors.withTimers { timer =>
      maybeE match {
        case Some(e) => context.log.debug(s"Discovery stream was failed with error: $e")
        case None => context.log.warning("Discovery stream was closed")
      }
      timer.startSingleTimer("connect", Connect, reconnectTimeout)
      disconnected
    }
  }

  def handleDiscovery(ev: MetricSpecDiscoveryEvent): Unit = {
    context.log.debug(s"Discovery stream update: $ev")
    val added = ev.added.flatMap(specFromGrpc).map { m =>
      m.id -> m
    }.toMap
    metricSpecs = (metricSpecs ++ added).filterKeys(x => !ev.removedIdx.contains(x))
  }

  def connect(): StreamObserver[Empty] = {
    val sObserver = actorObserver(context.self)
    stub.watchMetricSpec(sObserver)
  }
}

object MetricSpecDiscoverer {

  sealed trait DiscoveryMsg

  sealed trait DiscoveryResponse

  case class ConnectionFailed(err: Option[Throwable]) extends DiscoveryMsg

  case object Connect extends DiscoveryMsg

  case class DiscoveredSpec(internal: MetricSpecDiscoveryEvent) extends DiscoveryMsg

  case class GetAll(replyTo: ActorRef[GetAllResponse]) extends DiscoveryMsg

  case class GetAllResponse(specs: List[MetricSpec]) extends DiscoveryResponse

  case class FindBySpecId(specId: String, replyTo: ActorRef[FindBySpecResponse]) extends DiscoveryMsg

  case class FindBySpecResponse(spec: Option[MetricSpec]) extends DiscoveryResponse

  case class GetByModelVersion(versionId: Long, replyTo: ActorRef[GetByModelVersionResponse]) extends DiscoveryMsg

  case class GetByModelVersionResponse(specs: List[MetricSpec]) extends DiscoveryResponse

  def actorObserver(actorRef: ActorRef[DiscoveryMsg]): StreamObserver[MetricSpecDiscoveryEvent] = {
    new StreamObserver[MetricSpecDiscoveryEvent] {
      override def onNext(value: MetricSpecDiscoveryEvent): Unit = {
        actorRef ! DiscoveredSpec(value)
      }

      override def onError(t: Throwable): Unit = {
        actorRef ! ConnectionFailed(Option(t))
      }

      override def onCompleted(): Unit = {
        actorRef ! ConnectionFailed(None)
      }
    }
  }

  def mapCmp(comparison: ThresholdConfig.CmpOp): Option[ThresholdCmpOperator] = {
    comparison match {
      case CmpOp.EQ => Some(Eq)
      case CmpOp.NOT_EQ => Some(NotEq)
      case CmpOp.GREATER => Some(Greater)
      case CmpOp.LESS => Some(Less)
      case CmpOp.GREATER_EQ => Some(GreaterEq)
      case CmpOp.LESS_EQ => Some(LessEq)
      case CmpOp.Unrecognized(_) => None
    }
  }

  def specFromGrpc(gms: GMetricSpec): Option[MetricSpec] = {
    for {
      rawConfig <- gms.customModelConfig
      thr <- rawConfig.threshold
      servable <- rawConfig.servable
      config = CustomModelMetricSpecConfiguration(
        modelVersionId = rawConfig.monitorModelId,
        servableName = servable.name,
        threshold = Some(thr.value),
        thresholdCmpOperator = mapCmp(thr.comparison)
      )
    } yield CustomModelMetricSpec(
      name = gms.name,
      modelVersionId = gms.modelVersionId,
      withHealth = true,
      config = config,
      id = gms.id
    )
  }

  def apply(reconnectTimeout: FiniteDuration, stub: ServingDiscovery): Behavior[DiscoveryMsg] = {
    Behaviors.setup[DiscoveryMsg](context => new MetricSpecDiscoverer(context, reconnectTimeout, stub))
  }
}