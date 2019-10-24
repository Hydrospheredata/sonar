package io.hydrosphere.sonar.utils.grpc

import cats.effect.Async
import io.grpc.ManagedChannel
import io.hydrosphere.serving.gateway.api.{GatewayServiceGrpc, ServablePredictRequest}
import io.hydrosphere.serving.grpc.AuthorityReplacerInterceptor
import io.hydrosphere.serving.tensorflow.api.predict.PredictResponse
import io.hydrosphere.sonar.utils.FutureOps

trait GatewayServiceWrapper[F[_]] {
  def shadowlessPredictServable(request: ServablePredictRequest): F[PredictResponse]
}

object GatewayServiceWrapper {
  import FutureOps._

  def make[F[_]](channel: ManagedChannel)(implicit F: Async[F]): GatewayServiceWrapper[F] = {
    val grpcStub = GatewayServiceGrpc.stub(channel)
    new GatewayServiceWrapper[F] {
      override def shadowlessPredictServable(request: ServablePredictRequest): F[PredictResponse] = {
        grpcStub
          .withOption(AuthorityReplacerInterceptor.DESTINATION_KEY, "gateway")
          .shadowlessPredictServable(request)
          .liftToAsync[F]
      }
    }
  }
}