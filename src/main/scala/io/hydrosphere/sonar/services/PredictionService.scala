package io.hydrosphere.sonar.services

import cats.effect.Async
import io.hydrosphere.serving.gateway.api.ServablePredictRequest
import io.hydrosphere.serving.tensorflow.api.predict.PredictResponse
import io.hydrosphere.serving.tensorflow.tensor.TensorProto
import io.hydrosphere.sonar.utils.grpc.GatewayServiceWrapper

trait PredictionService[F[_]] {
  def predict(metricId: String, inputs: Map[String, TensorProto]): F[PredictResponse]
}

object PredictionService {

  def apply[F[_] : Async](client: GatewayServiceWrapper[F]): PredictionService[F] = {
    new PredictionService[F] {
      override def predict(servableName: String, inputs: Map[String, TensorProto]): F[PredictResponse] = {
        val request = ServablePredictRequest(servableName = servableName, data = inputs)
        client.shadowlessPredictServable(request)
      }
    }
  }
}