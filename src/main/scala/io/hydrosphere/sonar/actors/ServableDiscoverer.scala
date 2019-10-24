package io.hydrosphere.sonar.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Timers}
import cats.effect.Effect
import com.google.protobuf.empty.Empty
import io.grpc.stub.StreamObserver
import io.hydrosphere.serving.discovery.serving.ServableDiscoveryEvent
import io.hydrosphere.serving.discovery.serving.ServingDiscoveryGrpc.ServingDiscovery
import io.hydrosphere.sonar.actors.ServableDiscoverer._

import scala.concurrent.duration.FiniteDuration
import scala.util.Try


class ServableDiscoverer[F[_]](
                              clientDeadline: FiniteDuration,
                              reconnectTimeout: FiniteDuration,
                              stub: ServingDiscovery
                            )(implicit F: Effect[F])
  extends Actor with Timers with ActorLogging {

  override def aroundPreStart(): Unit = self ! Connect

  override def receive: Receive = disconnected

  def disconnected: Receive = {
    case Connect =>
      Try(connect()) match {
        case util.Success(serv) => context become listening(serv)
        case util.Failure(e) =>
          log.error(e, s"Can't setup discovery connection")
          timers.startSingleTimer("connect", Connect, reconnectTimeout)
      }
  }

  def listening(servableResponse: StreamObserver[Empty]): Receive = {
    case ev: ServableDiscoveryEvent => handleServableEvent(ev)

    case ConnectionFailed(maybeE) =>
      maybeE match {
        case Some(e) => log.debug(s"Discovery stream was failed with error: $e")
        case None => log.warning("Discovery stream was closed")
      }
      timers.startSingleTimer("connect", Connect, reconnectTimeout)
      context become disconnected
    case x =>
      log.debug(s"Unknown message: $x")
  }

  def handleServableEvent(ev: ServableDiscoveryEvent): Unit = {
    log.debug(s"Servable stream update: $ev")
    log.error(???)
  }

  def connect(): StreamObserver[Empty] = {
    val sObserver = actorObserver(self)
    stub.watchServables(sObserver)
  }

  override def preRestart(reason: Throwable, message: Option[Any]) {
    log.error(reason, "Restarting due to [{}] when processing [{}]",
      reason.getMessage, message.getOrElse("<no message>"))
  }
}

object ServableDiscoverer {

  case class ConnectionFailed(err: Option[Throwable])

  case object Connect

  def actorObserver(actorRef: ActorRef): StreamObserver[ServableDiscoveryEvent] = {
    new StreamObserver[ServableDiscoveryEvent] {
      override def onNext(value: ServableDiscoveryEvent): Unit = {
        actorRef ! value
      }

      override def onError(t: Throwable): Unit = {
        actorRef ! ConnectionFailed(Option(t))
      }

      override def onCompleted(): Unit = {
        actorRef ! ConnectionFailed(None)
      }
    }
  }
}