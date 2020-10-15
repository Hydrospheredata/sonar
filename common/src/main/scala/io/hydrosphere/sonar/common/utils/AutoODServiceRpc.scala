package io.hydrosphere.sonar.common.utils

import cats.effect.Async
import io.grpc.Channel
import io.hydrosphere.serving.auto_od.api.{LauchAutoOdRequest, LauchAutoOdResponse, AutoOdServiceGrpc}
import io.hydrosphere.serving.grpc.AuthorityReplacerInterceptor

trait AutoODServiceRpc[F[_]] {
  def launch(request: LauchAutoOdRequest): F[LauchAutoOdResponse]
}

object AutoODServiceRpc {

  import FutureOps._

  def make[F[_]](channel: Channel)(implicit F: Async[F]): AutoODServiceRpc[F] = {
    val grpcStub = AutoOdServiceGrpc.stub(channel)
    new AutoODServiceRpc[F] {
      override def launch(request: LauchAutoOdRequest): F[LauchAutoOdResponse] = {
        grpcStub
          .withOption(AuthorityReplacerInterceptor.DESTINATION_KEY, "auto_od")
          .launchAutoOd(request)
          .liftToAsync[F]
      }
    }
  }
}