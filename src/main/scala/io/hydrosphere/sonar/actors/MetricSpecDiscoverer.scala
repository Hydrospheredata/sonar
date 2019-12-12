package io.hydrosphere.sonar.actors

import java.util.UUID

import akka.actor.typed.scaladsl._
import akka.actor.typed.{ActorRef, Behavior}
import io.grpc.stub.StreamObserver
import io.hydrosphere.serving.discovery.serving.MetricSpecDiscoveryEvent
import io.hydrosphere.serving.discovery.serving.ServingDiscoveryGrpc.ServingDiscovery
import io.hydrosphere.serving.manager.grpc.entities.ThresholdConfig.CmpOp
import io.hydrosphere.serving.manager.grpc.entities.{ThresholdConfig, MetricSpec => GMetricSpec}
import io.hydrosphere.sonar.terms._

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

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

  def apply(reconnectTimeout: FiniteDuration, stub: ServingDiscovery): Behavior[DiscoveryMsg] = {
    val metricSpecs = TrieMap.empty[String, MetricSpec]

    def metricSpecHandler = Behaviors.receivePartial[DiscoveryMsg] {
      case (_, GetAll(replyTo)) =>
        replyTo ! GetAllResponse(metricSpecs.values.toList)
        Behavior.same

      case (_, FindBySpecId(specId, replyTo)) =>
        replyTo ! FindBySpecResponse(metricSpecs.get(specId))
        Behavior.same

      case (_, GetByModelVersion(versionId, replyTo)) =>
        replyTo ! GetByModelVersionResponse(metricSpecs.values.filter(_.modelVersionId == versionId).toList)
        Behavior.same

      case (context, DiscoveredSpec(internal)) =>
        metricSpecs ++= internal.added.flatMap { x =>
          specFromGrpc(x) match {
            case Some(value) => Some(value)
            case None =>
              context.log.info(s"Invalid MetricSpec id=${x.id} name=${x.name}. Ignoring.")
              None
          }
        }.map(x => x.id -> x).toMap
        metricSpecs --= internal.removedIdx
        Behavior.same
    }

    def listening: Behavior[DiscoveryMsg] = Behaviors.receivePartial[DiscoveryMsg] {
      case (context, ConnectionFailed(maybeE)) => Behaviors.withTimers[DiscoveryMsg] { timer =>
        maybeE match {
          case Some(e) => context.log.info(s"Discovery stream was failed with error: $e")
          case None => context.log.warning("Discovery stream was closed")
        }
        disconnected
      }
    }.orElse(metricSpecHandler)

    def disconnected: Behavior[DiscoveryMsg] =  Behaviors.withTimers[DiscoveryMsg] { timer =>
      val timerKey = "connect-" + UUID.randomUUID().toString
      timer.startSingleTimer(timerKey, Connect, reconnectTimeout)
      Behaviors.receivePartial[DiscoveryMsg] {
        case (context, Connect) =>
          val connect = Try {
            val sObserver = actorObserver(context.self)
            stub.watchMetricSpec(sObserver)
          }
          connect match {
            case util.Success(serv) =>
              timer.cancel(timerKey)
              context.log.info("MetricSpec discoverer is in listening state")
              listening
            case util.Failure(e) =>
              context.log.error(e, s"Can't setup discovery connection")
              Behavior.same
          }
      }
    }.orElse(metricSpecHandler)

    disconnected
  }
}