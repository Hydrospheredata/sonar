package io.hydrosphere.sonar.utils

import cats.effect.Async
import io.grpc.Channel
import io.hydrosphere.monitoring.proto.audo_od.api.{LaunchAutoOdRequest, LaunchAutoOdResponse, AutoOdServiceGrpc}
import io.hydrosphere.serving.grpc.AuthorityReplacerInterceptor

trait AutoODServiceRpc[F[_]] {
  def launch(request: LaunchAutoOdRequest): F[LaunchAutoOdResponse]
}

object AutoODServiceRpc {

  import FutureOps._

  def make[F[_]](channel: Channel)(implicit F: Async[F]): AutoODServiceRpc[F] = {
    val grpcStub = AutoOdServiceGrpc.stub(channel)
    new AutoODServiceRpc[F] {
      override def launch(request: LaunchAutoOdRequest): F[LaunchAutoOdResponse] = {
        grpcStub
          .withOption(AuthorityReplacerInterceptor.DESTINATION_KEY, "auto_od")
          .launchAutoOd(request)
          .liftToAsync[F]
      }
    }
  }
}