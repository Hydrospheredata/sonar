package io.hydrosphere.sonar.endpoints

import akka.actor.typed.ActorRef
import com.google.protobuf.empty.Empty
import io.hydrosphere.serving.monitoring.monitoring.ExecutionInformation
import io.hydrosphere.serving.monitoring.monitoring.MonitoringServiceGrpc.MonitoringService
import io.hydrosphere.sonar.Logging
import io.hydrosphere.sonar.actors.SonarSupervisor

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global


class MonitoringServiceGrpcApi(recipient: ActorRef[SonarSupervisor.Message]) extends MonitoringService with Logging {
  override def analyze(request: ExecutionInformation): Future[Empty] = {
    Future {
      logger.info("Got request from GRPC")
      recipient ! SonarSupervisor.Request(request)
      Empty()
    }
  }
}
