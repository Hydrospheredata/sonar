package io.hydrosphere.sonar.services

import cats.effect.Async
import cats.implicits._
import io.hydrosphere.monitoring.proto.audo_od.api.LaunchAutoOdRequest
import io.hydrosphere.sonar.utils.AutoODServiceRpc

trait AutoODService[F[_]] {
  def launchAutoOD(modelVersionId: Long, trainingDataPath: String): F[Unit]
}


class GRPCAutoODService[F[_]: Async](client: AutoODServiceRpc[F]) extends AutoODService[F] {
  override def launchAutoOD(modelVersionId: Long, trainingDataPath: String): F[Unit] = {
    val request = LaunchAutoOdRequest(trainingDataPath = trainingDataPath, modelVersionId = modelVersionId)
    client.launch(request) *> Async[F].unit
  }
}