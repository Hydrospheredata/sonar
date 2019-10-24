package io.hydrosphere.sonar.services

import cats.data.OptionT
import cats.effect.Async
import cats.effect.concurrent.Ref
import cats.implicits._
import io.hydrosphere.serving.gateway.api.ServablePredictRequest
import io.hydrosphere.serving.tensorflow.api.predict.PredictResponse
import io.hydrosphere.serving.tensorflow.tensor.TensorProto
import io.hydrosphere.sonar.utils.grpc.GatewayServiceWrapper

trait PredictionService[F[_]] {
  def predict(metricId: String, inputs: Map[String, TensorProto]): F[PredictResponse]
}

trait MetricServableMapper[F[_]] {
  def getServable(metricId: String): F[Option[String]]
}

object PredictionService {

  case class MissingServableError(metricId: String) extends Throwable

  def apply[F[_] : Async](client: GatewayServiceWrapper[F], mapper: MetricServableMapper[F]): PredictionService[F] = {
    new PredictionService[F] {
      override def predict(metricId: String, inputs: Map[String, TensorProto]): F[PredictResponse] = {
        for {
          servableName <- OptionT(mapper.getServable(metricId)).getOrElseF(MissingServableError(metricId).raiseError[F, String])
          request = ServablePredictRequest(servableName = servableName, data = inputs)
          result <- client.shadowlessPredictServable(request)
        } yield result
      }
    }
  }
}