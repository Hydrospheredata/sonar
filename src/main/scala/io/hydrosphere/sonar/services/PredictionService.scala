package io.hydrosphere.sonar.services

import cats.effect.Async
import io.grpc.{ClientInterceptors, ManagedChannelBuilder}
import io.hydrosphere.serving.grpc.{AuthorityReplacerInterceptor, Headers}
import io.hydrosphere.serving.tensorflow.api.model.ModelSpec
import io.hydrosphere.serving.tensorflow.api.predict.{PredictRequest, PredictResponse}
import io.hydrosphere.serving.tensorflow.api.prediction_service.PredictionServiceGrpc
import io.hydrosphere.serving.tensorflow.tensor.TensorProto
import io.hydrosphere.sonar.config.Configuration
import io.hydrosphere.sonar.utils.FutureOps
import io.netty.handler.ssl.util.InsecureTrustManagerFactory

import scala.concurrent.duration._

trait PredictionService[F[_]] {
  def callApplication(request: PredictRequest): F[PredictResponse]
  def callApplication(applicationName: String, inputs: Map[String, TensorProto]): F[PredictResponse]
}

class PredictionServiceGrpcInterpreter[F[_] : Async](config: Configuration) extends PredictionService[F] {

  import FutureOps._

  // TODO: remove copypaste
  private lazy val grpcChannel = {
    val deadline = 2 minutes
    val builder = ManagedChannelBuilder.forAddress(config.sidecar.host, config.sidecar.grpcPort)
    builder.enableRetry()
    builder.usePlaintext()
    builder.keepAliveTimeout(deadline.length, deadline.unit)
    ClientInterceptors.intercept(builder.build(), new AuthorityReplacerInterceptor +: Headers.interceptors: _*)
  }

  private lazy val predictionClient = PredictionServiceGrpc.stub(grpcChannel)

//  private lazy val managerClient = ManagerServiceGrpc.stub(grpcChannel)

  override def callApplication(request: PredictRequest): F[PredictResponse] = predictionClient
    .withOption(AuthorityReplacerInterceptor.DESTINATION_KEY, "gateway")
    .predict(request)
    .liftToAsync[F]

  override def callApplication(applicationName: String, inputs: Map[String, TensorProto]): F[PredictResponse] = {
    callApplication(PredictRequest(
      modelSpec = Some(ModelSpec(applicationName, None, "")),
      inputs
    ))
  }
}

