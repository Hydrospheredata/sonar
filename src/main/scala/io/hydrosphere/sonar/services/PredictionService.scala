package io.hydrosphere.sonar.services

import cats.effect.Async
import io.hydrosphere.serving.proto.gateway.api.GatewayPredictRequest
import io.hydrosphere.serving.proto.runtime.api.PredictResponse
import io.hydrosphere.serving.proto.contract.tensor.Tensor
import io.hydrosphere.sonar.utils.GatewayServiceRpc

trait PredictionService[F[_]] {
  def predict(metricId: String, inputs: Map[String, Tensor]): F[PredictResponse]
}

object PredictionService {

  def apply[F[_] : Async](client: GatewayServiceRpc[F]): PredictionService[F] = {
    (servableName: String, inputs: Map[String, Tensor]) => {
      val request = GatewayPredictRequest(name = servableName, data = inputs)
      client.shadowlessPredictServable(request)
    }
  }
}