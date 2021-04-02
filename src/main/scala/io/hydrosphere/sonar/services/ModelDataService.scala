package io.hydrosphere.sonar.services

import cats._
import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._
import io.grpc.{ClientInterceptors, ManagedChannelBuilder}
import io.hydrosphere.serving.proto.contract.signature.ModelSignature
import io.hydrosphere.serving.proto.contract.field.ModelField
import io.hydrosphere.serving.grpc.{AuthorityReplacerInterceptor, Headers}
import io.hydrosphere.serving.proto.manager.api.{GetVersionRequest, ManagerServiceGrpc}
import io.hydrosphere.serving.proto.contract.types.DataProfileType
import io.hydrosphere.serving.proto.manager.entities.{ModelVersion, ModelVersionStatus}
import io.hydrosphere.sonar.config.Configuration
import io.hydrosphere.sonar.utils.FutureOps

import scala.concurrent.duration._

trait ModelDataService[F[_]] {
  def getModelVersion(modelVersionId: Long): F[ModelVersion]
}

class ModelDataServiceIdInterpreter[F[_] : Monad] extends ModelDataService[F] {
  override def getModelVersion(modelVersionId: Long): F[ModelVersion] = {
    val mv = ModelVersion(
      id = 1,
      name = "ModelType",
      version = 1,
      status = ModelVersionStatus.Success,
      signature = Some(ModelSignature(
        signatureName = "infer",
        inputs = Seq(ModelField(name = "input", profile = DataProfileType.NUMERICAL)),
        outputs = Seq(ModelField(name = "output", profile = DataProfileType.NUMERICAL))
      )),
      imageSha = "",
      depconfigName = "",
      metadata = Map.empty[String, String],
    )
    Monad[F].pure(mv)
  }
}

class ModelDataServiceGrpcInterpreter[F[_] : Async](config: Configuration, state: Ref[F, Map[Long, ModelVersion]]) extends ModelDataService[F] {

  import FutureOps._

  private lazy val grpcChannel = {
    val deadline = 2 minutes
    val builder = ManagedChannelBuilder.forAddress(config.sidecar.host, config.sidecar.grpcPort)
    builder.enableRetry()
    builder.usePlaintext()
    builder.keepAliveTimeout(deadline.length, deadline.unit)
    ClientInterceptors.intercept(builder.build(), new AuthorityReplacerInterceptor +: Headers.interceptors: _*)
  }

//  private lazy val predictionClient = PredictionServiceGrpc.stub(grpcChannel)

  private lazy val managerClient = ManagerServiceGrpc.stub(grpcChannel)

  private def requestModelVersion(modelVersionId: Long): F[ModelVersion] = managerClient
    .withOption(AuthorityReplacerInterceptor.DESTINATION_KEY, "manager")
    .getVersion(GetVersionRequest(modelVersionId))
    .liftToAsync[F]

  override def getModelVersion(modelVersionId: Long): F[ModelVersion] = for {
    stored <- state.get
    maybeStored = stored.get(modelVersionId)
    modelVersion <- maybeStored match {
      case Some(modelVersion) => Monad[F].pure(modelVersion)
      case None => for {
        mv <- requestModelVersion(modelVersionId)
        _ <- state.modify(st => (st.updated(modelVersionId, mv), mv))
      } yield mv
    }
  } yield modelVersion
}