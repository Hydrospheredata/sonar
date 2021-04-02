package io.hydrosphere.sonar.services

import akka.actor.Scheduler
import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.AskPattern._
import akka.util.Timeout
import cats.effect.Async
import cats.implicits._
import io.hydrosphere.serving.proto.manager.entities.MetricSpec
import io.hydrosphere.sonar.actors.MetricSpecDiscoverer._
import io.hydrosphere.sonar.terms._

trait MetricSpecService[F[_]] {
  
  def getMetricSpecById(id: String): F[Option[MetricSpec]]
  def getMetricSpecsByModelVersion(modelVersionId: Long): F[List[MetricSpec]]
  def getAllMetricSpecs: F[List[MetricSpec]]
}

class MetricSpecServiceInterpreter[F[_]](discovererActor: ActorRef[DiscoveryMsg])(implicit F: Async[F], timeout: Timeout, scheduler: Scheduler) extends MetricSpecService[F] {

  import io.hydrosphere.sonar.utils.FutureOps._

  override def getMetricSpecById(id: String): F[Option[MetricSpec]] = {
    discovererActor.?[FindBySpecResponse](r => FindBySpecId(id, r))
      .liftToAsync
      .map(_.spec)
  }

  override def getAllMetricSpecs: F[List[MetricSpec]] = {
    discovererActor.?[GetAllResponse](r => GetAll(r))
      .liftToAsync
      .map(_.specs)
  }

  override def getMetricSpecsByModelVersion(modelVersionId: Long): F[List[MetricSpec]] = {
    discovererActor.?[GetByModelVersionResponse](r => GetByModelVersion(modelVersionId, r))
      .liftToAsync
      .map(_.specs)
  }
}