package io.hydrosphere.sonar.utils

import cats.effect.Async
import io.grpc.Channel
import io.hydrosphere.serving.proto.gateway.api.{GatewayPredictRequest, GatewayServiceGrpc}
import io.hydrosphere.serving.grpc.AuthorityReplacerInterceptor
import io.hydrosphere.serving.proto.runtime.api.PredictResponse

trait GatewayServiceRpc[F[_]] {
  def shadowlessPredictServable(request: GatewayPredictRequest): F[PredictResponse]
}

object GatewayServiceRpc {
  import FutureOps._

  def make[F[_]](channel: Channel)(implicit F: Async[F]): GatewayServiceRpc[F] = {
    val grpcStub = GatewayServiceGrpc.stub(channel)
    new GatewayServiceRpc[F] {
      override def shadowlessPredictServable(request: GatewayPredictRequest): F[PredictResponse] = {
        grpcStub
          .withOption(AuthorityReplacerInterceptor.DESTINATION_KEY, "gateway")
          .shadowlessPredictServable(request)
          .liftToAsync[F]
      }
    }
  }
}