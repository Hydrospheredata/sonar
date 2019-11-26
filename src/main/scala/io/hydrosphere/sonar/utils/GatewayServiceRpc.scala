package io.hydrosphere.sonar.utils

import cats.effect.Async
import io.grpc.Channel
import io.hydrosphere.serving.gateway.api.{GatewayServiceGrpc, ServablePredictRequest}
import io.hydrosphere.serving.grpc.AuthorityReplacerInterceptor
import io.hydrosphere.serving.tensorflow.api.predict.PredictResponse

trait GatewayServiceRpc[F[_]] {
  def shadowlessPredictServable(request: ServablePredictRequest): F[PredictResponse]
}

object GatewayServiceRpc {
  import FutureOps._

  def make[F[_]](channel: Channel)(implicit F: Async[F]): GatewayServiceRpc[F] = {
    val grpcStub = GatewayServiceGrpc.stub(channel)
    new GatewayServiceRpc[F] {
      override def shadowlessPredictServable(request: ServablePredictRequest): F[PredictResponse] = {
        grpcStub
          .withOption(AuthorityReplacerInterceptor.DESTINATION_KEY, "gateway")
          .shadowlessPredictServable(request)
          .liftToAsync[F]
      }
    }
  }
}